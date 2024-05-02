FROM c2id/c2id-server-demo:12.18 AS c2id
FROM openjdk:11.0.16-jre

# Using this to launch a fake server on container start; see `setup.sh`
RUN apt-get update -qqy && apt-get install -qqy python3

COPY --from=c2id /c2id-server /c2id-server
COPY --from=c2id /etc/c2id /etc/c2id
COPY ./oidc/setup.sh /fixture/

ENV CATALINA_OPTS="-DsystemPropertiesURL=file:///config/c2id/override.properties"
EXPOSE 8080
CMD ["/bin/bash", "/fixture/setup.sh"]
