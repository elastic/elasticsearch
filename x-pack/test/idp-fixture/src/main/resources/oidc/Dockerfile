FROM c2id/c2id-server-demo:16.1.1 AS c2id
FROM openjdk:21-jdk-buster

# Using this to launch a fake server on container start; see `setup.sh`
RUN apt-get update -qqy && apt-get install -qqy python3

COPY --from=c2id /c2id-server /c2id-server
COPY --from=c2id /etc/c2id /etc/c2id
COPY ./oidc/setup.sh /fixture/
COPY ./oidc/testnode.jks /c2id-server/tomcat/conf/keystore.jks

RUN sed -i '/<!-- A "Connector" using the shared thread pool-->/ i\
    <Connector port="8443" \
               protocol="org.apache.coyote.http11.Http11NioProtocol" \
               SSLEnabled="true" \
               maxThreads="150" \
               scheme="https" \
               secure="true" \
               clientAuth="false" \
               sslProtocol="TLS" \
               sslEnabledProtocols="TLSv1.3"> \
        <SSLHostConfig> \
            <Certificate \
                certificateKeystoreFile="/c2id-server/tomcat/conf/keystore.jks" \
                certificateKeystorePassword="testnode" \
                type="RSA" /> \
        </SSLHostConfig> \
    </Connector>' \
    /c2id-server/tomcat/conf/server.xml

ENV CATALINA_OPTS="-DsystemPropertiesURL=file:///config/c2id/override.properties"
EXPOSE 8080
EXPOSE 8443
CMD ["/bin/bash", "/fixture/setup.sh"]
