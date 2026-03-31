FROM emqx/emqx:5.8

# 將 broker 固定設定打包進 image（放在不受 volume 覆蓋的位置）
COPY acl.conf /opt/emqx/bootstrap/acl.conf
COPY cluster.hocon /opt/emqx/bootstrap/cluster.hocon
COPY --chmod=755 entrypoint.sh /opt/emqx/bootstrap/entrypoint.sh

ENTRYPOINT ["/opt/emqx/bootstrap/entrypoint.sh"]
CMD ["emqx", "foreground"]
