# The c2id server just needs a JRE + Tomcat, no need for full Ubuntu
#
# Multi-arch: final image runs on both linux/amd64 and linux/arm64. Build for both with:
#   docker buildx build --platform linux/amd64,linux/arm64 -t <tag> .
#
# NOTE: c2id/c2id-server-demo only provides amd64 images, so we force the platform
# for the COPY stage via build-arg (avoids BuildKit FromPlatformFlagConstDisallowed).
# The runtime stage uses the native platform.
ARG C2ID_PLATFORM=linux/amd64
FROM --platform=${C2ID_PLATFORM} c2id/c2id-server-demo:16.1.1 AS c2id

# Extract WAR using JDK's jar (avoids apt-get + unzip in final image).
# No platform pin: uses build target so we get native arm64 or amd64 image.
FROM eclipse-temurin:17-jdk AS extract
COPY --from=c2id /c2id-server /c2id-server

# Download Tomcat Jakarta EE migration tool to pre-migrate legacy Java EE WARs.
# This avoids runtime migration overhead that can cause container startup timeouts.
ARG MIGRATION_TOOL_VERSION=1.0.10
ADD https://archive.apache.org/dist/tomcat/jakartaee-migration/v${MIGRATION_TOOL_VERSION}/binaries/jakartaee-migration-${MIGRATION_TOOL_VERSION}-shaded.jar /tmp/migrate.jar

# Extract c2id.war to webapps directory
RUN cd /c2id-server/tomcat/webapps && mkdir -p c2id && cd c2id && jar xf ../c2id.war && rm -f ../c2id.war

# Pre-migrate legacy Java EE WARs from webapps-javaee to webapps at build time.
# Tomcat 10+ normally does this at runtime which adds significant startup latency.
# By doing it at build time, container startup is faster and more deterministic.
RUN if [ -d /c2id-server/tomcat/webapps-javaee ]; then \
      for war in /c2id-server/tomcat/webapps-javaee/*.war; do \
        [ -f "$war" ] || continue; \
        base=$(basename "$war"); \
        echo "Pre-migrating $base from Java EE to Jakarta EE..."; \
        java -jar /tmp/migrate.jar "$war" "/c2id-server/tomcat/webapps/$base"; \
      done && \
      rm -rf /c2id-server/tomcat/webapps-javaee; \
    fi

# JRE base for final image (multi-arch: arm64 and amd64).
# Alpine variants don't support ARM64, so we use the standard JRE.
FROM eclipse-temurin:17-jre

COPY --from=extract /c2id-server /c2id-server
COPY --from=c2id /etc/c2id /etc/c2id
COPY ./build.sh /fixture/
COPY ./entrypoint.sh /fixture/
COPY ./testnode.jks /c2id-server/tomcat/conf/keystore.jks

# Build logic: prepare image (config dir) and Tomcat server.xml in a single layer
RUN chmod +x /fixture/build.sh /fixture/entrypoint.sh && /fixture/build.sh && \
    sed -i '/<!-- A "Connector" using the shared thread pool-->/ i\
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
