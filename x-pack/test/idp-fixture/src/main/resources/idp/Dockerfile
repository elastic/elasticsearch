# OPTIMIZED: Using smaller JRE base + combined RUN commands + cleanup
# Reduces image size from ~1.19GB to ~500MB
#
# Based on https://github.com/Unicon/shibboleth-idp-dockerized/blob/master/Dockerfile
# Building entire image to allow for easier upgrades since published image has not been updated for years
#
# NOTE: Using eclipse-temurin JRE (not Alpine) for ARM64 compatibility
# Alpine variants don't support ARM64, but the standard JRE is still much smaller
# than the original openjdk:11.0.16-jre (~394MB vs ~886MB base)

FROM eclipse-temurin:11-jre

ENV JAVA_HOME=/opt/java/openjdk
# JAVA variable is required by jetty.sh script (it looks for JAVA, not JAVA_HOME)
ENV JAVA=/opt/java/openjdk/bin/java

# Version configuration
ENV jetty_version=9.3.27.v20190418 \
    jetty_hash=7c7c80dd1c9f921771e2b1a05deeeec652d5fcaa \
    idp_version=3.4.3 \
    idp_hash=eb86bc7b6366ce2a44f97cae1b014d307b84257e3149469b22b2d091007309db \
    dta_hash=2f547074b06952b94c35631398f36746820a7697 \
    slf4j_version=1.7.25 \
    slf4j_hash=da76ca59f6a57ee3102f8f9bd9cee742973efa8a \
    logback_version=1.2.3 \
    logback_classic_hash=7c4f3c474fb2c041d8028740440937705ebb473a \
    logback_core_hash=864344400c3d4d92dfeb0a305dc87d953677c03c \
    logback_access_hash=e8a841cb796f6423c7afd8738df6e0e4052bf24a

ENV JETTY_HOME=/opt/jetty-home \
    JETTY_BASE=/opt/shib-jetty-base \
    PATH=$PATH:$JAVA_HOME/bin \
    JETTY_BROWSER_SSL_KEYSTORE_PASSWORD=secret \
    JETTY_BACKCHANNEL_SSL_KEYSTORE_PASSWORD=secret \
    JETTY_MAX_HEAP=64m

# Create directories for custom jetty config
RUN mkdir -p /opt/shib-jetty-base/modules /opt/shib-jetty-base/etc

# Manually override the jetty keystore otherwise it will attempt to download and fail
COPY ./jetty-custom/ssl.mod /opt/shib-jetty-base/modules/ssl.mod
COPY ./jetty-custom/keystore /opt/shib-jetty-base/etc/keystore

# Install dependencies, download all components, and clean up in a single layer
# This significantly reduces image size by not persisting intermediate files
RUN apt-get update && apt-get install -y --no-install-recommends wget gawk netcat-openbsd \
    # Create directories
    && mkdir -p /opt/shib-jetty-base/lib/ext /opt/shib-jetty-base/lib/logging /opt/shib-jetty-base/resources \
    # Download and install Jetty
    && wget -q https://repo.maven.apache.org/maven2/org/eclipse/jetty/jetty-distribution/$jetty_version/jetty-distribution-$jetty_version.tar.gz \
    && echo "$jetty_hash  jetty-distribution-$jetty_version.tar.gz" | sha1sum -c - \
    && tar -xzf jetty-distribution-$jetty_version.tar.gz -C /opt \
    && ln -s /opt/jetty-distribution-$jetty_version/ /opt/jetty-home \
    && rm jetty-distribution-$jetty_version.tar.gz \
    # Configure Jetty
    && cd /opt/shib-jetty-base \
    && touch start.ini \
    && java -jar ../jetty-home/start.jar --add-to-startd=http,https,deploy,ext,annotations,jstl,rewrite \
    # Download and install Shibboleth IdP
    && wget -q https://shibboleth.net/downloads/identity-provider/archive/$idp_version/shibboleth-identity-provider-$idp_version.tar.gz \
    && echo "$idp_hash  shibboleth-identity-provider-$idp_version.tar.gz" | sha256sum -c - \
    && tar -xzf shibboleth-identity-provider-$idp_version.tar.gz -C /opt \
    && ln -s /opt/shibboleth-identity-provider-$idp_version/ /opt/shibboleth-idp \
    && rm shibboleth-identity-provider-$idp_version.tar.gz \
    # Download SOAP Endpoints library
    && wget -q https://build.shibboleth.net/nexus/content/repositories/releases/net/shibboleth/utilities/jetty9/jetty9-dta-ssl/1.0.0/jetty9-dta-ssl-1.0.0.jar \
    && echo "$dta_hash  jetty9-dta-ssl-1.0.0.jar" | sha1sum -c - \
    && mv jetty9-dta-ssl-1.0.0.jar /opt/shib-jetty-base/lib/ext/ \
    # Download logging libraries
    && wget -q https://repo.maven.apache.org/maven2/org/slf4j/slf4j-api/$slf4j_version/slf4j-api-$slf4j_version.jar \
    && echo "$slf4j_hash  slf4j-api-$slf4j_version.jar" | sha1sum -c - \
    && mv slf4j-api-$slf4j_version.jar /opt/shib-jetty-base/lib/logging/ \
    && wget -q https://repo.maven.apache.org/maven2/ch/qos/logback/logback-classic/$logback_version/logback-classic-$logback_version.jar \
    && echo "$logback_classic_hash  logback-classic-$logback_version.jar" | sha1sum -c - \
    && mv logback-classic-$logback_version.jar /opt/shib-jetty-base/lib/logging/ \
    && wget -q https://repo.maven.apache.org/maven2/ch/qos/logback/logback-core/$logback_version/logback-core-$logback_version.jar \
    && echo "$logback_core_hash logback-core-$logback_version.jar" | sha1sum -c - \
    && mv logback-core-$logback_version.jar /opt/shib-jetty-base/lib/logging/ \
    && wget -q https://repo.maven.apache.org/maven2/ch/qos/logback/logback-access/$logback_version/logback-access-$logback_version.jar \
    && echo "$logback_access_hash logback-access-$logback_version.jar" | sha1sum -c - \
    && mv logback-access-$logback_version.jar /opt/shib-jetty-base/lib/logging/ \
    # Clean up apt cache
    && rm -rf /var/lib/apt/lists/*

## Copy local files
COPY shib-jetty-base/ /opt/shib-jetty-base/
COPY shibboleth-idp/ /opt/shibboleth-idp/
COPY bin/ /usr/local/bin/

# Setting owner ownership and permissions
# Note: We only modify permissions for jetty/shib directories, not /opt/java
RUN useradd jetty -U -s /bin/false \
    && chown -R root:jetty /opt/jetty-home /opt/jetty-distribution-* /opt/shib-jetty-base /opt/shibboleth-idp /opt/shibboleth-identity-provider-* \
    && chmod -R 640 /opt/jetty-home /opt/jetty-distribution-* /opt/shib-jetty-base /opt/shibboleth-idp /opt/shibboleth-identity-provider-* \
    && chmod -R 750 /opt/shibboleth-idp/bin \
    && chmod 750 /usr/local/bin/run-jetty.sh /usr/local/bin/init-idp.sh \
    && chmod +x /opt/jetty-home/bin/jetty.sh

# Opening 4443 (browser TLS), 8443 (mutual auth TLS)
EXPOSE 4443 8443

CMD ["run-jetty.sh"]
