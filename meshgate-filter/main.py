#!/usr/bin/env python3
"""
Meshgate 過濾網關：從本地 bridge 訂閱，過濾後轉發至本地 EMQX Broker。
設定由 config.yaml 讀取，使用 aiomqtt 非同步連線。
支援解析 ServiceEnvelope 依封包 from 比對 ignoreId、封包 ID 去重。
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

logger = logging.getLogger("meshgate_filter")

import aiomqtt
import yaml

try:
    from aiomqtt.exceptions import MqttError
except ImportError:
    MqttError = type("MqttError", (Exception,), {})  # noqa: no cover

try:
    from meshtastic import mqtt_pb2
except ImportError:
    from meshtastic.protobuf import mqtt_pb2

# ---------------------------------------------------------------------------
# 設定與常數
# ---------------------------------------------------------------------------

CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"

# Topic / node 過濾：固定排除關鍵字、node id 片段；僅允許 8 位英數字 node 或 /map
EXCLUDE_TOPIC_CONTAINS: tuple[str, ...] = ()
EXCLUDE_NODE_ID_CONTAINS = ("!abcd",)
REQUIRE_VALID_NODE_OR_MAP = True

RX_TIME_MAX_AGE_SECONDS = 300  # rx_time 超過此秒數視為過期，不轉發
DEDUP_TTL_SECONDS = 30  # 去重 (from_id, packet_id) TTL
ONLINE_NODES_REFRESH_SECONDS = 10  # 在線節點 API 更新間隔（秒）

# 轉發 path：main 用三種 path；僅 /2/e/ 會 fan-out 到節點
MAIN_PATH_PREFIXES: tuple[str, ...] = ("/2/e/", "/2/map", "/2/json/")
ALLOWED_PATH_PREFIXES: tuple[str, ...] = ("/2/e/",)

_dedup_seen: set[tuple[int, int]] = set()
_dedup_expire_at: list[tuple[float, tuple[int, int]]] = []
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
# 去重 (from_id, packet_id)
# ---------------------------------------------------------------------------


def _dedup_expire() -> None:
    now = time.monotonic()
    still_valid = [(t, k) for t, k in _dedup_expire_at if t >= now]
    expired_keys = {k for t, k in _dedup_expire_at if t < now}
    for k in expired_keys:
        _dedup_seen.discard(k)
    _dedup_expire_at.clear()
    _dedup_expire_at.extend(still_valid)


def _dedup_is_duplicate(from_id: int, packet_id: int) -> bool:
    _dedup_expire()
    return (from_id, packet_id) in _dedup_seen


def _dedup_mark_seen(from_id: int, packet_id: int) -> None:
    _dedup_expire()
    key = (from_id, packet_id)
    if key in _dedup_seen:
        return
    _dedup_seen.add(key)
    _dedup_expire_at.append((time.monotonic() + DEDUP_TTL_SECONDS, key))


# ---------------------------------------------------------------------------
# 過濾：topic 規則、封包 from / rx_time / ignoreId
# ---------------------------------------------------------------------------


def should_drop(config: dict, topic: str) -> tuple[bool, str | None]:
    """
    依 topic 規則判斷是否丟棄。回傳 (是否丟棄, 原因或 None)。
    原因: topic_contains_json / topic_ignoreId / topic_invalid_node_or_map
    """
    parts = topic.rstrip("/").split("/")
    last = parts[-1] if parts else ""

    filt = config.get("filter", {})
    for kw in EXCLUDE_TOPIC_CONTAINS:
        if kw in topic:
            return True, "topic_contains_json"
    ignore_ids = [s.strip() for s in filt.get("ignoreId", []) if s]
    if ignore_ids and last.startswith("!"):
        node_id = last[1:]
        if node_id in ignore_ids:
            return True, "topic_ignoreId"
    if REQUIRE_VALID_NODE_OR_MAP:
        if last.startswith("!"):
            node_id = last[1:]
            if len(node_id) != 8 or not re.match(r"^[a-zA-Z0-9]{8}$", node_id):
                return True, "topic_invalid_node_or_map"
        elif last != "map":
            return True, "topic_invalid_node_or_map"
    return False, None


def should_drop_by_packet_from(
    config: dict, payload: bytes
) -> tuple[bool, int | None, int | None, bool, int | None, int | None]:
    """
    解包 ServiceEnvelope，依 rx_time 過期、from 規則判斷是否丟棄，並回傳 from_id / packet_id 供去重用。
    回傳 (是否丟棄, from_id 或 None, packet_id 或 None, 是否因過期丟棄, rx_time 或 None, age_seconds 或 None)。
    """
    try:
        se = mqtt_pb2.ServiceEnvelope()
        se.ParseFromString(payload)
        mp = se.packet
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
            return (
                True,
                getattr(mp, "from", None),
                getattr(mp, "id", None),
                True,
                int(rx_time),
                age_seconds,
            )
        from_id = getattr(mp, "from", None)
        packet_id = getattr(mp, "id", None)
        if from_id is None:
            return (
                False,
                None,
                None,
                False,
                int(rx_time) if rx_time else None,
                age_seconds,
            )
        from_id_str = str(from_id)
        from_hex = format(from_id, "08x").lower()
        for kw in EXCLUDE_NODE_ID_CONTAINS:
            fragment = kw.lstrip("!").lower()
            if fragment and from_hex.startswith(fragment):
                return (
                    True,
                    from_id,
                    None,
                    False,
                    int(rx_time) if rx_time else None,
                    age_seconds,
                )
        ignore_ids_raw = [
            s.strip() for s in config.get("filter", {}).get("ignoreId", []) if s
        ]
        for s in ignore_ids_raw:
            s_lower = s.lower()
            if from_id_str == s or from_hex == s_lower:
                return (
                    True,
                    from_id,
                    None,
                    False,
                    int(rx_time) if rx_time else None,
                    age_seconds,
                )
        return (
            False,
            from_id,
            packet_id,
            False,
            int(rx_time) if rx_time else None,
            age_seconds,
        )
    except Exception:
        return False, None, None, False, None, None


# ---------------------------------------------------------------------------
# 連線與 topic 輔助
# ---------------------------------------------------------------------------


def _topic_str(message) -> str:
    """取得訊息的 topic 字串（相容 aiomqtt 的 Topic 物件）。"""
    t = message.topic
    return getattr(t, "value", t) if t is not None else ""


def _forward_topic(topic: str, prefixes: tuple[str, ...] | None = None) -> str | None:
    """
    若 topic 含 prefixes 任一路徑，回傳從「第一個允許前綴」起至結尾的 path。
    prefixes 預設為 ALLOWED_PATH_PREFIXES；傳 MAIN_PATH_PREFIXES 可取得 main 用 path。
    """
    if prefixes is None:
        prefixes = ALLOWED_PATH_PREFIXES
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


async def run_async():
    global _online_nodes, _online_nodes_updated
    config = load_config(CONFIG_PATH)
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
                        (
                            dropped_by_from,
                            from_id,
                            packet_id,
                            expired,
                            rx_time,
                            age_seconds,
                        ) = should_drop_by_packet_from(config, payload)
                        if dropped_by_from:
                            if expired:
                                rx_time_str = (
                                    time.strftime(
                                        "%Y-%m-%d %H:%M:%S", time.localtime(rx_time)
                                    )
                                    if rx_time is not None
                                    else "unknown"
                                )
                                now_local_str = time.strftime(
                                    "%Y-%m-%d %H:%M:%S", time.localtime()
                                )
                                logger.warning(
                                    "過濾(過期): topic=%s packet_rx_time_local=%s packet_rx_time_epoch=%s now_local=%s packet_age=%ss max_age=%ss reason=packet_too_old",
                                    topic,
                                    rx_time_str,
                                    rx_time if rx_time is not None else "unknown",
                                    now_local_str,
                                    (
                                        age_seconds
                                        if age_seconds is not None
                                        else "unknown"
                                    ),
                                    RX_TIME_MAX_AGE_SECONDS,
                                )
                            elif from_id is not None:
                                from_hex = format(from_id, "08x").lower()
                                logger.warning(
                                    "過濾(ignoreId/from): topic=%s from=%s (!%s)",
                                    topic,
                                    from_id,
                                    from_hex,
                                )
                            continue
                        drop_topic, drop_reason = should_drop(config, topic)
                        if drop_topic and drop_reason:
                            logger.warning("過濾(%s): %s", drop_reason, topic)
                            continue
                        main_path = _forward_topic(topic, MAIN_PATH_PREFIXES)
                        if main_path is None:
                            logger.warning("過濾(topic_not_forward): %s", topic)
                            continue
                        if (
                            from_id is not None
                            and packet_id is not None
                            and _dedup_is_duplicate(from_id, packet_id)
                        ):
                            logger.warning(
                                "去重: topic=%s from=%s id=%s",
                                topic,
                                from_id,
                                packet_id,
                            )
                            continue
                        # Fan-out：定期更新在線節點
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
                        await local_client.publish(
                            main_topic,
                            payload=payload,
                            qos=message.qos or 0,
                            retain=message.retain or False,
                        )
                        if from_id is not None and packet_id is not None:
                            _dedup_mark_seen(from_id, packet_id)
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
                                    qos=message.qos or 0,
                                    retain=message.retain or False,
                                )
                                sent_count += 1
                            logger.info(
                                "轉發: %s -> main + fan-out %d 節點",
                                topic,
                                sent_count,
                            )
                        else:
                            reason = (
                                "（僅 main，不 fan-out）"
                                if not main_path.startswith("/2/e/")
                                else "（在線節點為空）"
                            )
                            logger.info("轉發: %s -> main %s", topic, reason)
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
    asyncio.run(run_async())


if __name__ == "__main__":
    main()
