FROM eclipse-mosquitto:2
ENV MESHGATE_BRIDGE_CONNECTION_NAME=official_bridge

COPY mosquitto.conf /templates/mosquitto.conf
COPY entrypoint.sh /custom-entrypoint.sh
RUN chmod +x /custom-entrypoint.sh
ENTRYPOINT ["/custom-entrypoint.sh"]
