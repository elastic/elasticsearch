FROM c2id/c2id-server-demo:12.18 AS c2id
FROM openjdk:11.0.16-jre

COPY --from=c2id /c2id-server /c2id-server
COPY --from=c2id /etc/c2id /etc/c2id

ENV CATALINA_OPTS="-DsystemPropertiesURL=file:///etc/c2id/override.properties -Dlog4j.level=debug"
EXPOSE 8080

#CMD sleep infinity

CMD ["/bin/bash", "/c2id-server/tomcat/bin/catalina.sh", "run"]
