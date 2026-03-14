#!/bin/sh
set -e
# 未設定時預設為 official_bridge
: "${MESHGATE_BRIDGE_CONNECTION_NAME:=official_bridge}"
# 用 sed 替換模板中的變數，寫入正式設定檔（不依賴 gettext/envsubst）
sed "s/\${MESHGATE_BRIDGE_CONNECTION_NAME}/$MESHGATE_BRIDGE_CONNECTION_NAME/g" \
  /templates/mosquitto.conf > /mosquitto/config/mosquitto.conf
exec /docker-entrypoint.sh mosquitto -c /mosquitto/config/mosquitto.conf
