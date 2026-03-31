#!/bin/sh
set -eu

# 啟動時強制覆蓋 volume 內設定，確保使用 repo 版本。
install -d /opt/emqx/data/configs /opt/emqx/data/authz
cp -f /opt/emqx/bootstrap/cluster.hocon /opt/emqx/data/configs/cluster.hocon
cp -f /opt/emqx/bootstrap/acl.conf /opt/emqx/data/authz/acl.conf

# 交回官方 entrypoint（不同 base image 版本路徑可能不同）。
if [ -x /usr/bin/docker-entrypoint.sh ]; then
  exec /usr/bin/docker-entrypoint.sh "$@"
elif [ -x /docker-entrypoint.sh ]; then
  exec /docker-entrypoint.sh "$@"
fi

echo "找不到 EMQX 官方 entrypoint，改直接啟動 emqx" >&2
exec emqx "$@"
