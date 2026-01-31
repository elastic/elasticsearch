# OPTIMIZED: Using smaller JRE base to reduce image size from ~1.58GB to ~600MB
# The c2id server just needs a JRE + Tomcat, no need for full Ubuntu
#
# NOTE: c2id/c2id-server-demo only provides amd64 images, so we force the platform
# for the COPY stage. The runtime stage uses the native platform.
FROM --platform=linux/amd64 c2id/c2id-server-demo:16.1.1 AS c2id

# Using eclipse-temurin JRE (not Alpine) for ARM64 compatibility
# Alpine variants don't support ARM64, but the standard JRE is still much smaller
# than the original eclipse-temurin:17-noble (~400MB vs ~1.3GB)
FROM eclipse-temurin:17-jre

COPY --from=c2id /c2id-server /c2id-server
COPY --from=c2id /etc/c2id /etc/c2id
COPY ./build.sh /fixture/
COPY ./entrypoint.sh /fixture/
COPY ./testnode.jks /c2id-server/tomcat/conf/keystore.jks

# Build logic: prepare image (config dir, pre-deploy WAR, etc.)
RUN chmod +x /fixture/build.sh /fixture/entrypoint.sh && /fixture/build.sh

# Pre-deploy c2id WAR at build time so container startup skips ~30s Tomcat deployment
RUN apt-get update -qqy && apt-get install -qqy --no-install-recommends unzip \
    && cd /c2id-server/tomcat/webapps && unzip -q -o c2id.war -d c2id && rm -f c2id.war \
    && apt-get remove -qqy unzip && rm -rf /var/lib/apt/lists/*

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

# Limit JVM heap for test fixture to reduce memory footprint
ENV CATALINA_OPTS="-DsystemPropertiesURL=file:///config/c2id/override.properties -Xms128m -Xmx256m"
EXPOSE 8080
EXPOSE 8443
# Run logic: wait for override.properties (from test), then start Tomcat
CMD ["/bin/bash", "/fixture/entrypoint.sh"]
