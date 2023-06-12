FROM c2id/c2id-server-demo:12.18 AS c2id
FROM openjdk:11.0.16-jre

# TODO this is obviously horrible
RUN apt-get update
RUN apt-get install -y python3
COPY --from=c2id /c2id-server /c2id-server
COPY --from=c2id /etc/c2id /etc/c2id
ADD ./oidc/setup.sh /

EXPOSE 8080
# We will launch the c2id server as part via a docker-compose command; we need to do this so that the c2id config with
# the injected ephemeral port is available
CMD sleep infinity
