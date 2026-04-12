#!/usr/bin/env python3
"""
Meshgate 過濾網關：從本地 bridge 訂閱，過濾後轉發至本地 EMQX Broker。
設定由 config.yaml 讀取，使用 aiomqtt 非同步連線。
支援依封包 from 比對 ignoreIdDropOnly；同一 (from_id, packet_id, 正規化 topic) 在 TTL 內僅處理一次。
Fan-out 模式：需 local.api_url / api_key / api_secret，轉發至 msh/TW{path} 與 msh/TW/node/!{node_id}{path}。
"""
import asyncio
import base64
import json
import logging
import os
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import aiomqtt
import yaml

logger = logging.getLogger("meshgate_filter")

try:
    from aiomqtt.exceptions import MqttError
except ImportError:
    MqttError = type("MqttError", (Exception,), {})  # noqa: no cover

try:
    from meshtastic import mesh_pb2, mqtt_pb2
except ImportError:
    from meshtastic.protobuf import mesh_pb2, mqtt_pb2

from google.protobuf.text_format import MessageToString

# ---------------------------------------------------------------------------
# 設定與常數
# ---------------------------------------------------------------------------

CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"

RX_TIME_MAX_AGE_SECONDS = 300  # rx_time 超過此秒數視為過期，不轉發
DEDUP_TTL_SECONDS = (
    30  # 同一 (from_id, packet_id, 正規化 topic) 僅處理一次；TTL 內重複略過
)
ONLINE_NODES_REFRESH_SECONDS = 10  # 在線節點 API 更新間隔（秒）

# 轉發 path：main 用三種 path；僅 /2/e/ 會 fan-out 到節點
MAIN_PATH_PREFIXES: tuple[str, ...] = ("/2/e/", "/2/map", "/2/json/")


_dedup_seen: set[tuple[int, int, str]] = set()
_dedup_expire_at: list[tuple[float, tuple[int, int, str]]] = []
_online_nodes: list[str] = []
_online_nodes_updated: float = 0.0


def load_config(path: Path) -> dict:
    """從 YAML 載入設定。"""
    if not path.exists():
        logger.error("找不到設定檔 %s", path)
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# 去重 (from_id, packet_id, topic_key)；topic_key 為 _dedup_topic_key(進線 topic)，已去除 bridge 前綴等
# ---------------------------------------------------------------------------


def _dedup_expire() -> None:
    now = time.monotonic()
    still_valid = [(t, k) for t, k in _dedup_expire_at if t >= now]
    expired_keys = {k for t, k in _dedup_expire_at if t < now}
    for k in expired_keys:
        _dedup_seen.discard(k)
    _dedup_expire_at.clear()
    _dedup_expire_at.extend(still_valid)


def _dedup_is_duplicate(from_id: int, packet_id: int, topic_key: str) -> bool:
    _dedup_expire()
    return (from_id, packet_id, topic_key) in _dedup_seen


def _dedup_mark_seen_if_present(
    from_id: int | None, packet_id: int | None, topic_key: str
) -> None:
    """有 from_id 與 packet_id 時依正規化 topic_key 標記已處理。"""
    if from_id is None or packet_id is None:
        return
    _dedup_expire()
    key = (from_id, packet_id, topic_key)
    if key in _dedup_seen:
        return
    _dedup_seen.add(key)
    _dedup_expire_at.append((time.monotonic() + DEDUP_TTL_SECONDS, key))


def _log_expired_packet(
    topic: str,
    rx_time: int | None,
    age_seconds: int | None,
) -> None:
    rx_local = (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(rx_time))
        if rx_time is not None
        else "unknown"
    )
    now_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    logger.warning(
        "過濾(過期): topic=%s packet_rx_time_local=%s packet_rx_time_epoch=%s now_local=%s packet_age=%ss max_age=%ss reason=packet_too_old",
        topic,
        rx_local,
        rx_time if rx_time is not None else "unknown",
        now_str,
        age_seconds if age_seconds is not None else "unknown",
        RX_TIME_MAX_AGE_SECONDS,
    )


# ---------------------------------------------------------------------------
# 過濾：topic 規則、封包 from / rx_time / ignore 名單
# ---------------------------------------------------------------------------


def _filter_ignore_id_strings(filt: dict, key: str) -> list[str]:
    return [s.strip() for s in filt.get(key, []) if s]


def _ignore_entry_matches_node_uint32(node_id: int, entry: str) -> bool:
    """名單可為十進位字串或 8 位 hex（與 topic !xxxxxxxx 相同），與節點 id 比對。"""
    s = entry.strip()
    if not s:
        return False
    return str(node_id) == s or format(node_id, "08x").lower() == s.lower()


def packet_layer_should_drop(config: dict, from_id: int | None) -> bool:
    """依封包 from 與設定判斷封包層是否略過本地轉發。"""
    if from_id is None:
        return False
    filt = config.get("filter") or {}
    drop_only_ids = _filter_ignore_id_strings(filt, "ignoreIdDropOnly")
    return any(_ignore_entry_matches_node_uint32(from_id, s) for s in drop_only_ids)


def _mesh_packet_from_and_id(
    mp: mesh_pb2.MeshPacket | None,
) -> tuple[int | None, int | None]:
    if mp is None:
        return None, None
    fid = getattr(mp, "from_", None)
    if fid is None:
        fid = getattr(mp, "from", None)
    return fid, getattr(mp, "id", None)


def topic_layer_should_drop(config: dict, topic: str) -> tuple[bool, str | None]:
    """
    依 topic 判斷是否略過本地轉發。
    回傳 (drop_topic, reason)。
    """
    parts = topic.rstrip("/").split("/")
    last = parts[-1] if parts else ""

    filt = config.get("filter", {}) or {}

    drop_only_ids = _filter_ignore_id_strings(filt, "ignoreIdDropOnly")
    if last.startswith("!"):
        nh = last[1:].lower()
        if re.match(r"^[a-f0-9]{8}$", nh):
            try:
                node_uint = int(nh, 16)
            except ValueError:
                node_uint = None
            if node_uint is not None and any(
                _ignore_entry_matches_node_uint32(node_uint, s) for s in drop_only_ids
            ):
                return True, "topic_ignoreIdDropOnly"

    # 最後一段須為合法 8 位 !node 或 map
    if last.startswith("!"):
        node_id = last[1:]
        if len(node_id) != 8 or not re.match(r"^[a-zA-Z0-9]{8}$", node_id):
            return True, "topic_invalid_node_or_map"
    elif last != "map":
        return True, "topic_invalid_node_or_map"
    return False, None


# topic_layer_should_drop 回傳原因 → 日誌用說明
_TOPIC_DROP_REASON_LABELS: dict[str, str] = {
    "topic_ignoreIdDropOnly": "最後一段 !node 命中 ignoreIdDropOnly",
    "topic_invalid_node_or_map": "最後一段非合法 8 位 !node 或 map",
}


def _mesh_packet_from_payload(payload: bytes) -> mesh_pb2.MeshPacket | None:
    """由 MQTT payload 解出 ServiceEnvelope，回傳內嵌 MeshPacket。"""
    try:
        se = mqtt_pb2.ServiceEnvelope()
        se.ParseFromString(payload)
        return se.packet
    except Exception:
        return None


def _mesh_packet_one_line(mp: mesh_pb2.MeshPacket | None) -> str:
    """protobuf TextFormat 單行（日誌不換行）。"""
    if mp is None:
        return "None"
    return MessageToString(mp, as_one_line=True)


def packet_rx_time_expired(
    mp: mesh_pb2.MeshPacket | None,
) -> tuple[bool, int | None, int | None]:
    """
    依封包 rx_time 與 RX_TIME_MAX_AGE_SECONDS 判斷是否過期。
    回傳 (是否過期, rx_time 或 None, age_seconds 或 None)。mp 為 None 時視為未過期。
    """
    if mp is None:
        return False, None, None
    rx_time = getattr(mp, "rx_time", None)
    age_seconds = None
    if rx_time is not None and rx_time > 0:
        age_seconds = int(time.time() - rx_time)
    if (
        rx_time is not None
        and rx_time > 0
        and age_seconds is not None
        and age_seconds > RX_TIME_MAX_AGE_SECONDS
    ):
        return True, int(rx_time), age_seconds
    return False, int(rx_time) if rx_time else None, age_seconds


def _from_id_packet_id_from_json_topic(
    topic: str, payload: bytes
) -> tuple[int | None, int | None]:
    """僅當 topic 含 /2/json/ 時從 JSON 解析 from、id（packet id），供去重與日誌。"""
    if "/2/json/" not in topic:
        return None, None
    try:
        data = json.loads(payload.decode("utf-8"))
        if not isinstance(data, dict):
            return None, None
        raw_from = data.get("from")
        if raw_from is None:
            return None, None
        from_id = int(raw_from)
        raw_id = data.get("id")
        packet_id = int(raw_id) if raw_id is not None else None
        return from_id, packet_id
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
        return None, None


def _dedup_topic_key(topic: str) -> str:
    """
    去重用 topic 字串：整理 msh/TW/… 路徑，避免重複前綴與多餘斜線。
    - 若以 msh/TW/bridge/<通道名>/ 開頭，去掉 bridge 與該通道名一層
    - 合併錯誤重複的 msh/TW、…/msh/TW/msh/TW/…
    - 合併 //
    """
    t = topic
    tw = "msh/TW/"
    if not t.startswith(tw):
        return t
    parts = [p for p in t.split("/") if p != ""]
    if (
        len(parts) >= 5
        and parts[0] == "msh"
        and parts[1] == "TW"
        and parts[2] == "bridge"
    ):
        rest = "/".join(parts[4:])
        t = f"{tw}{rest}" if rest else tw
    while "msh/TW/msh/TW" in t:
        t = t.replace("msh/TW/msh/TW", "msh/TW", 1)
    while "/msh/TW/msh/TW" in t:
        t = t.replace("/msh/TW/msh/TW", "/msh/TW", 1)
    while "//" in t:
        t = t.replace("//", "/")
    return t


# ---------------------------------------------------------------------------
# 連線與 topic 輔助
# ---------------------------------------------------------------------------


def _topic_str(message) -> str:
    """取得訊息的 topic 字串（相容 aiomqtt 的 Topic 物件）。"""
    t = message.topic
    return getattr(t, "value", t) if t is not None else ""


def is_supported_meshtastic_topic_shape(topic: str) -> bool:
    """
    是否為支援的 Meshtastic TW topic 形狀（依路徑分段，非單純子字串搜尋）。
    對應常見結構（+ 表示實際發佈時的一層 topic）：
      msh/TW/2/e/+/+、msh/TW/2/json/+/+、msh/TW/2/map/+
      msh/TW/+/2/e/+/+、msh/TW/+/2/json/+/+、msh/TW/+/2/map/+
      msh/TW/+/+/2/e/+/+、msh/TW/+/+/2/json/+/+、msh/TW/+/+/2/map/+
      msh/TW/+/+/+/2/e/+/+、msh/TW/+/+/+/2/json/+/+、msh/TW/+/+/+/2/map/+
    約定：2/e、2/json 在種類後至少還有兩層。2/map 僅接受 .../2/map/（整段 topic 必須以 / 結尾），不會出現 .../2/map 無尾端斜線。
    """
    parts = [p for p in topic.split("/") if p != ""]
    if len(parts) < 4:
        return False
    if parts[0] != "msh" or parts[1] != "TW":
        return False

    def branch_from_2_at(i: int) -> bool:
        """parts[i] == \"2\"，且 parts[i+1] 為 e | json | map，後綴段數符合。"""
        if i + 1 >= len(parts):
            return False
        kind = parts[i + 1]
        if kind == "e" or kind == "json":
            return len(parts) >= i + 4  # 2, kind 之後至少兩層
        if kind == "map":
            if not topic.endswith("/"):
                return False
            return (
                len(parts) >= i + 2
            )  # .../2/map/（尾端 / 表示最後一層空，split 無額外段）
        return False

    if parts[2] == "2":
        return branch_from_2_at(2)
    if len(parts) >= 5 and parts[3] == "2":
        return branch_from_2_at(3)
    if len(parts) >= 6 and parts[4] == "2":
        return branch_from_2_at(4)
    if len(parts) >= 7 and parts[5] == "2":
        return branch_from_2_at(5)
    return False


def _forward_topic(topic: str, prefixes: tuple[str, ...]) -> str | None:
    """
    若 topic 含 prefixes 任一路徑，回傳從「第一個允許前綴」起至結尾的 path。
    通常傳入 MAIN_PATH_PREFIXES 取得轉發用 path。
    """
    best_start: int | None = None
    for p in prefixes:
        for q in (p, p.rstrip("/")):
            idx = topic.find(q)
            if idx != -1:
                if best_start is None or idx < best_start:
                    best_start = idx
    if best_start is None:
        return None
    segment = topic[best_start:]
    best_i: int | None = None
    for p in prefixes:
        for q in (p, p.rstrip("/")):
            i = segment.find(q)
            if i != -1 and (best_i is None or i < best_i):
                best_i = i
    if best_i is not None:
        path = segment[best_i:]
    else:
        path = segment
    # 絕不回傳以 msh 開頭的 path；若有 /2 則從該處切
    if path.startswith("msh/") or (path and not path.startswith("/2")):
        i = path.find("/2")
        if i != -1:
            path = path[i:]
        else:
            path = "/2/e/"  # fallback 避免組出錯誤 topic
    return path


# ---------------------------------------------------------------------------
# EMQX API：在線節點列表
# ---------------------------------------------------------------------------


def _fetch_online_nodes_sync(api_url: str, api_key: str, api_secret: str) -> list[str]:
    """
    呼叫 EMQX GET /api/v5/clients?conn_state=connected，回傳在線節點 ID 列表（帶 ! 前綴）。
    username 須為 8 位英數字；且 clientid 須包含 !username（即 ! 加該 8 位）。
    另：clientid 含 MeshtasticAndroidMqttProxy 時亦視為在線（相容 Android MQTT Proxy）。
    """
    url = f"{api_url.rstrip('/')}/api/v5/clients?conn_state=connected&limit=1000"
    auth = base64.b64encode(f"{api_key}:{api_secret}".encode()).decode()
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "Authorization": f"Basic {auth}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode() if e.fp else ""
        logger.error(
            "EMQX API HTTP 錯誤: status=%s reason=%s body=%r", e.code, e.reason, body
        )
        raise
    out: list[str] = []
    seen: set[str] = set()
    for client in data.get("data", []):
        clientid = client.get("clientid") or ""
        username = (client.get("username") or "").strip()
        if not re.match(r"^[a-zA-Z0-9]{8}$", username):
            continue
        node_id = "!" + username.lower()
        if node_id not in clientid:
            if "MeshtasticAndroidMqttProxy" not in clientid:
                continue
        if node_id not in seen:
            seen.add(node_id)
            out.append(node_id)
    return out


def _validate_local_config(config: dict) -> None:
    """檢查 local 區塊必填欄位與環境變數，不符則印出錯誤並 sys.exit(1)。"""
    local_cfg = config.get("local", {})
    username = (local_cfg.get("username") or "").strip()
    if not username:
        logger.error("請在 config.yaml 的 local 區塊填寫 username 與 password")
        sys.exit(1)
    api_url = (local_cfg.get("api_url") or "").strip()
    api_key = (os.environ.get("MESHGATE_BROKER_API_KEY") or "").strip()
    if not api_url or not api_key:
        logger.error(
            "Fan-out 模式需填寫 local.api_url，並設定環境變數 MESHGATE_BROKER_API_KEY、MESHGATE_BROKER_API_SECRET"
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# 主程式：連線、訂閱、過濾、轉發
# ---------------------------------------------------------------------------


async def run_async(config: dict) -> None:
    global _online_nodes, _online_nodes_updated
    _validate_local_config(config)

    source_cfg = config.get("source", {})
    local_cfg = config.get("local", {})
    subscribe_topic = source_cfg.get("topic", "msh/TW/#")
    api_url = (local_cfg.get("api_url") or "").strip()
    api_key = (os.environ.get("MESHGATE_BROKER_API_KEY") or "").strip()
    api_secret = (os.environ.get("MESHGATE_BROKER_API_SECRET") or "").strip()

    local_host = local_cfg.get("host", "meshgate-broker")
    local_port = int(local_cfg.get("port", 1883))
    source_host = source_cfg.get("host", "meshgate-bridge")
    source_port = int(source_cfg.get("port", 1883))

    logger.info("啟動智慧過濾網關（Fan-out 模式），訂閱 %s ...", subscribe_topic)
    try:
        _online_nodes = await asyncio.to_thread(
            _fetch_online_nodes_sync, api_url, api_key, api_secret
        )
        _online_nodes_updated = time.monotonic()
        logger.info("在線節點數: %d", len(_online_nodes))
    except Exception as e:
        logger.error(
            "取得在線節點失敗，請檢查 api_url 與環境變數 MESHGATE_BROKER_API_KEY、MESHGATE_BROKER_API_SECRET: %s",
            e,
        )
        sys.exit(1)

    source_kw: dict = {
        "hostname": source_host,
        "port": source_port,
        "identifier": "meshgate_filter_listener",
        "username": source_cfg.get("username") or "",
        "password": source_cfg.get("password") or "",
    }

    for attempt in range(1, 31):
        try:
            async with aiomqtt.Client(
                hostname=local_host,
                port=local_port,
                identifier="meshgate_smart_filter",
                username=local_cfg["username"].strip(),
                password=local_cfg.get("password") or "",
            ) as local_client:
                async with aiomqtt.Client(**source_kw) as source_client:
                    await source_client.subscribe(subscribe_topic)
                    logger.info(
                        "已連線至遠端 %s，訂閱 %s", source_host, subscribe_topic
                    )

                    async for message in source_client.messages:
                        topic = _topic_str(message)
                        payload = message.payload
                        if not payload:
                            continue
                        # 1. 不支援的路徑（非預期 msh/TW/... 分段形狀或 path 萃取失敗）→ 略過
                        if not is_supported_meshtastic_topic_shape(topic):
                            logger.warning("過濾(unsupported_path): %s", topic)
                            continue
                        main_path = _forward_topic(topic, MAIN_PATH_PREFIXES)
                        if main_path is None:
                            logger.warning(
                                "過濾(unsupported_path): %s (path 萃取失敗)", topic
                            )
                            continue
                        # 2. topic_key、解包、rx_time、from／packet_id、封包層略過判斷（僅計算，尚不略過）
                        topic_key = _dedup_topic_key(topic)
                        mesh_packet = _mesh_packet_from_payload(payload)
                        expired, rx_time, age_seconds = packet_rx_time_expired(
                            mesh_packet
                        )
                        from_id, packet_id = _mesh_packet_from_and_id(mesh_packet)
                        j_from, j_pid = _from_id_packet_id_from_json_topic(
                            topic, payload
                        )
                        if from_id is None:
                            from_id = j_from
                        if packet_id is None:
                            packet_id = j_pid
                        drop_by_from = packet_layer_should_drop(config, from_id)
                        # 3. 去重（topic_key = 正規化後 topic）
                        if (
                            from_id is not None
                            and packet_id is not None
                            and _dedup_is_duplicate(from_id, packet_id, topic_key)
                        ):
                            continue
                        # 4. 封包層過濾 → 不轉本地
                        if drop_by_from:
                            if from_id is not None:
                                from_hex = format(from_id, "08x").lower()
                                logger.warning(
                                    "過濾(ignoreId/from): topic=%s qos=%s retain=%s from=%s (!%s) mesh_packet=%s",
                                    topic,
                                    message.qos,
                                    message.retain or False,
                                    from_id,
                                    from_hex,
                                    _mesh_packet_one_line(mesh_packet),
                                )
                            else:
                                logger.warning(
                                    "過濾(ignoreId/from): topic=%s qos=%s retain=%s from_id=None mesh_packet=%s",
                                    topic,
                                    message.qos,
                                    message.retain or False,
                                    _mesh_packet_one_line(mesh_packet),
                                )
                            _dedup_mark_seen_if_present(from_id, packet_id, topic_key)
                            continue
                        # 5. topic 層過濾 → 略過
                        drop_topic, drop_reason = topic_layer_should_drop(config, topic)
                        if drop_topic and drop_reason:
                            _lbl = _TOPIC_DROP_REASON_LABELS.get(
                                drop_reason, drop_reason
                            )
                            logger.warning(
                                "過濾(topic層) %s [%s]: topic=%s qos=%s retain=%s mesh_packet=%s",
                                _lbl,
                                drop_reason,
                                topic,
                                message.qos,
                                message.retain or False,
                                _mesh_packet_one_line(mesh_packet),
                            )
                            _dedup_mark_seen_if_present(from_id, packet_id, topic_key)
                            continue
                        # 6. 丟棄過期封包（僅 rx_time）
                        if expired:
                            _log_expired_packet(topic, rx_time, age_seconds)
                            continue
                        # 7. 轉發至本地 EMQX；定期刷新在線節點後 fan-out（僅 /2/e/）
                        if (
                            time.monotonic() - _online_nodes_updated
                        ) > ONLINE_NODES_REFRESH_SECONDS:
                            try:
                                nodes = await asyncio.to_thread(
                                    _fetch_online_nodes_sync,
                                    api_url,
                                    api_key,
                                    api_secret,
                                )
                                _online_nodes.clear()
                                _online_nodes.extend(nodes)
                                _online_nodes_updated = time.monotonic()
                            except Exception as e:
                                logger.warning("取得在線節點失敗: %s，使用既有清單", e)
                        nodes_to_publish = list(dict.fromkeys(_online_nodes))
                        # 一律轉發至 msh/TW（/2/e/、/2/map、/2/json/）
                        main_topic = f"msh/TW{main_path}"
                        _fwd_from = (
                            f"{from_id} (!{format(from_id, '08x').lower()})"
                            if from_id is not None
                            else "None"
                        )
                        _fwd_packet_id = (
                            str(packet_id) if packet_id is not None else "None"
                        )
                        await local_client.publish(
                            main_topic,
                            payload=payload,
                            qos=message.qos,
                            retain=message.retain or False,
                        )
                        _dedup_mark_seen_if_present(from_id, packet_id, topic_key)
                        # 僅 path 以 /2/e/ 開頭時才 fan-out 給節點（/2/json/、/2/map 只發 main）
                        sender_node_id = (
                            "!" + format(from_id, "08x").lower()
                            if from_id is not None
                            else None
                        )
                        if main_path.startswith("/2/e/") and nodes_to_publish:
                            sent_count = 0
                            for node_id in nodes_to_publish:
                                if (
                                    sender_node_id is not None
                                    and node_id == sender_node_id
                                ):
                                    continue
                                target = f"msh/TW/node/{node_id}{main_path}"
                                await local_client.publish(
                                    target,
                                    payload=payload,
                                    qos=message.qos,
                                    retain=message.retain or False,
                                )
                                sent_count += 1
                            logger.debug(
                                "轉發至本地 EMQX: topic=%s main=%s from=%s packet_id=%s fan_out=%d",
                                topic,
                                main_topic,
                                _fwd_from,
                                _fwd_packet_id,
                                sent_count,
                            )
                        else:
                            reason = (
                                "（僅 main，不 fan-out）"
                                if not main_path.startswith("/2/e/")
                                else "（在線節點為空）"
                            )
                            logger.debug(
                                "轉發至本地 EMQX: topic=%s main=%s from=%s packet_id=%s %s",
                                topic,
                                main_topic,
                                _fwd_from,
                                _fwd_packet_id,
                                reason,
                            )
        except (OSError, ConnectionError, MqttError) as e:
            if attempt < 30:
                logger.warning("連線失敗 (嘗試 %d/30)，2 秒後重試: %s", attempt, e)
                await asyncio.sleep(2)
                continue
            raise


def main() -> None:
    """程式進入點。日誌等級由 config.yaml 的 logging.level 或環境變數 LOG_LEVEL 控制。"""
    config = load_config(CONFIG_PATH)
    log_cfg = config.get("logging", {})
    level_name = (log_cfg.get("level") or os.environ.get("LOG_LEVEL") or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(levelname)s [%(name)s] %(message)s",
        stream=sys.stdout,
    )
    asyncio.run(run_async(config))


if __name__ == "__main__":
    main()
