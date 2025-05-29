################################################################################
# This Dockerfile was generated from the template at distribution/src/docker/Dockerfile
#
# Beginning of multi stage Dockerfile
################################################################################

<% /*
  This file is passed through Groovy's SimpleTemplateEngine, so dollars and backslashes
  have to be escaped in order for them to appear in the final Dockerfile. You
  can also comment out blocks, like this one. See:

  https://docs.groovy-lang.org/latest/html/api/groovy/text/SimpleTemplateEngine.html

  We use control-flow tags in this file to conditionally render the content. The
  layout/presentation here has been adjusted so that it looks reasonable when rendered,
  at the slight expense of how it looks here.

  Note that this file is also filtered to squash together newlines, so we can
  add as many newlines here as necessary to improve legibility.
*/ %>

################################################################################
# Build stage 1 `builder`:
# Extract Elasticsearch artifact
################################################################################

FROM ${base_image} AS builder

# Install required packages to extract the Elasticsearch distribution
RUN <%= retry.loop(package_manager, "export DEBIAN_FRONTEND=noninteractive && ${package_manager} update && ${package_manager} update && ${package_manager} add --no-cache curl") %>

RUN mkdir /usr/share/elasticsearch
WORKDIR /usr/share/elasticsearch

# Fetch the appropriate Elasticsearch distribution for this architecture.
# Keep this command on one line - it is replaced with a `COPY` during local builds.
# It uses the `arch` shell command to fetch the correct distro for the build machine,
# which is needed for Docker Hub builds.
RUN curl --retry 10 -S -L --output /tmp/elasticsearch.tar.gz https://artifacts-no-kpi.elastic.co/downloads/elasticsearch/elasticsearch-${version}-linux-\$(arch).tar.gz

RUN tar -zxf /tmp/elasticsearch.tar.gz --strip-components=1

# The distribution includes a `config` directory, no need to create it
COPY ${config_dir}/elasticsearch.yml config/
COPY ${config_dir}/log4j2.properties config/log4j2.docker.properties

#  1. Configure the distribution for Docker
#  2. Create required directory
#  3. Move the distribution's default logging config aside
#  4. Move the generated docker logging config so that it is the default
#  5. Reset permissions on all directories
#  6. Reset permissions on all files
#  7. Make CLI tools executable
#  8. Make some directories writable. `bin` must be writable because
#     plugins can install their own CLI utilities.
#  9. Make some files writable
RUN sed -i -e 's/ES_DISTRIBUTION_TYPE=tar/ES_DISTRIBUTION_TYPE=docker/' bin/elasticsearch-env && \\
    mkdir data && \\
    mv config/log4j2.properties config/log4j2.file.properties && \\
    mv config/log4j2.docker.properties config/log4j2.properties && \\
    find . -type d -exec chmod 0555 {} + && \\
    find . -type f -exec chmod 0444 {} + && \\
    chmod 0555 bin/* jdk/bin/* jdk/lib/jspawnhelper modules/x-pack-ml/platform/linux-*/bin/* && \\
    chmod 0775 bin config config/jvm.options.d data logs plugins && \\
    find config -type f -exec chmod 0664 {} +

# Add plugins infrastructure
RUN mkdir -p /opt/plugins/archive
RUN chmod -R 0555 /opt/plugins

RUN mkdir -p /fips/libs
COPY fips/libs/*.jar /fips/libs/

COPY filebeat-${version}.tar.gz metricbeat-${version}.tar.gz /tmp/
RUN set -eux ; \\
    for beat in filebeat metricbeat ; do \\
      if [ ! -s /tmp/\$beat-${version}.tar.gz ]; then \\
        echo "/tmp/\$beat-${version}.tar.gz is empty - cannot uncompress" 2>&1 ; \\
        exit 1 ; \\
      fi ; \\
      if ! tar tf /tmp/\$beat-${version}.tar.gz >/dev/null; then \\
        echo "/tmp/\$beat-${version}.tar.gz is corrupt - cannot uncompress" 2>&1 ; \\
        exit 1 ; \\
      fi ; \\
      mkdir -p /opt/\$beat ; \\
      tar xf /tmp/\$beat-${version}.tar.gz -C /opt/\$beat --strip-components=1 ; \\
    done

COPY plugins/*.zip /opt/plugins/archive/

RUN chown 1000:1000 /opt/plugins/archive/*
RUN chmod 0444 /opt/plugins/archive/*

COPY fips/resources/fips_java.security /usr/share/elasticsearch/config/fips_java.security
COPY fips/resources/fips_java.policy /usr/share/elasticsearch/config/fips_java.policy

WORKDIR /usr/share/elasticsearch/config

################################################################################
# Build stage 2 (the actual Elasticsearch image):
#
# Copy elasticsearch from stage 1
# Add entrypoint
################################################################################

FROM ${base_image}

RUN <%= retry.loop(package_manager,
          "export DEBIAN_FRONTEND=noninteractive && \n" +
          "      ${package_manager} update && \n" +
          "      ${package_manager} upgrade && \n" +
          "      ${package_manager} add --no-cache \n" +
          "        bash java-cacerts curl libstdc++ libsystemd netcat-openbsd p11-kit p11-kit-trust posix-libc-utils shadow tini unzip zip zstd && \n" +
          "      rm -rf /var/cache/apk/* "
     ) %>

# Set Bash as the default shell for future commands
SHELL ["/bin/bash", "-c"]

RUN groupadd -g 1000 elasticsearch && \
    adduser -G elasticsearch -u 1000 elasticsearch -D --home /usr/share/elasticsearch elasticsearch && \
    adduser elasticsearch root && \
    chown -R 0:0 /usr/share/elasticsearch

ENV ELASTIC_CONTAINER=true

WORKDIR /usr/share/elasticsearch

COPY --from=builder --chown=0:0 /usr/share/elasticsearch /usr/share/elasticsearch
COPY --from=builder --chown=0:0 /fips/libs/*.jar /usr/share/elasticsearch/lib/
COPY --from=builder --chown=0:0 /opt /opt

ENV ES_PLUGIN_ARCHIVE_DIR=/opt/plugins/archive
ENV PATH=/usr/share/elasticsearch/bin:\$PATH
ENV SHELL=/bin/bash
COPY ${bin_dir}/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

# 1. Sync the user and group permissions of /etc/passwd
# 2. Set correct permissions of the entrypoint
# 3. Ensure that there are no files with setuid or setgid, in order to mitigate "stackclash" attacks.
#    We've already run this in previous layers so it ought to be a no-op.
# 4. Replace OpenJDK's built-in CA certificate keystore with the one from the OS
#    vendor. The latter is superior in several ways.
#    REF: https://github.com/elastic/elasticsearch-docker/issues/171
# 5. Tighten up permissions on the ES home dir (the permissions of the contents are handled earlier)
# 6. You can't install plugins that include configuration when running as `elasticsearch` and the `config`
#    dir is owned by `root`, because the installed tries to manipulate the permissions on the plugin's
#    config directory.
RUN chmod g=u /etc/passwd && \\
    chmod 0555 /usr/local/bin/docker-entrypoint.sh && \\
    find / -xdev -perm -4000 -exec chmod ug-s {} + && \\
    chmod 0775 /usr/share/elasticsearch && \\
    chown elasticsearch bin config config/jvm.options.d data logs plugins

RUN ln -sf /etc/ssl/certs/java/cacerts /usr/share/elasticsearch/jdk/lib/security/cacerts

# Convert cacerts (PKCS12) to BCFKS format using POSIX-compatible shell syntax
RUN printf "\\n" | jdk/bin/keytool -importkeystore \
  -srckeystore /usr/share/elasticsearch/jdk/lib/security/cacerts \
  -srcstoretype PKCS12 \
  -destkeystore config/cacerts.bcfks \
  -deststorepass passwordcacert \
  -deststoretype BCFKS \
  -providerclass org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider \
  -providerpath lib/bc-fips-1.0.2.5.jar \
  -destprovidername BCFIPS


## Add fips specific JVM options
RUN cat <<EOF > /usr/share/elasticsearch/config/jvm.options.d/fips.options
-Djavax.net.ssl.keyStoreType=BCFKS
-Dorg.bouncycastle.fips.approved_only=true
-Djava.security.properties=config/fips_java.security
-Djava.security.policy=config/fips_java.policy
-Djavax.net.ssl.trustStore=config/cacerts.bcfks
-Djavax.net.ssl.trustStorePassword=passwordcacert
EOF

EXPOSE 9200 9300

LABEL org.label-schema.build-date="${build_date}" \\
  org.label-schema.license="${license}" \\
  org.label-schema.name="Elasticsearch" \\
  org.label-schema.schema-version="1.0" \\
  org.label-schema.url="https://www.elastic.co/products/elasticsearch" \\
  org.label-schema.usage="https://www.elastic.co/guide/en/elasticsearch/reference/index.html" \\
  org.label-schema.vcs-ref="${git_revision}" \\
  org.label-schema.vcs-url="https://github.com/elastic/elasticsearch" \\
  org.label-schema.vendor="Elastic" \\
  org.label-schema.version="${version}" \\
  org.opencontainers.image.created="${build_date}" \\
  org.opencontainers.image.documentation="https://www.elastic.co/guide/en/elasticsearch/reference/index.html" \\
  org.opencontainers.image.licenses="${license}" \\
  org.opencontainers.image.revision="${git_revision}" \\
  org.opencontainers.image.source="https://github.com/elastic/elasticsearch" \\
  org.opencontainers.image.title="Elasticsearch" \\
  org.opencontainers.image.url="https://www.elastic.co/products/elasticsearch" \\
  org.opencontainers.image.vendor="Elastic" \\
  org.opencontainers.image.version="${version}"

LABEL name="Elasticsearch" \\
  maintainer="infra@elastic.co" \\
  vendor="Elastic" \\
  version="${version}" \\
  release="1" \\
  summary="Elasticsearch" \\
  description="You know, for search."

RUN mkdir /licenses && ln LICENSE.txt /licenses/LICENSE

# Generate a stub command that will be overwritten at runtime
RUN mkdir /app && \\
    echo -e '#!/bin/bash\\nexec /usr/local/bin/docker-entrypoint.sh eswrapper' > /app/elasticsearch.sh && \\
    chmod 0555 /app/elasticsearch.sh

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["/app/elasticsearch.sh"]

USER 1000:0

################################################################################
# End of multi-stage Dockerfile
################################################################################
