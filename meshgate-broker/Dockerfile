FROM emqx/emqx:5.8

# 將 broker 固定設定打包進 image，避免部署時額外掛載
COPY acl.conf /opt/emqx/data/authz/acl.conf
COPY cluster.hocon /opt/emqx/data/configs/cluster.hocon
