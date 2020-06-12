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
*/ %>

################################################################################
# Step 1. Build curl statically. Installing it from RPM on CentOS pulls in too
# many dependencies.
################################################################################
FROM alpine:latest AS curl
COPY bin/build-curl.sh /
RUN /build-curl.sh


################################################################################
# Step 2. Create a minimal root filesystem directory. This will form the basis
# for our image.
################################################################################
FROM centos:7 AS rootfs
COPY bin/build-rootfs.sh /
RUN /build-rootfs.sh /rootfs
COPY --from=curl /work/curl /rootfs/usr/bin/curl


################################################################################
# Step 3. Fetch the Elasticsearch distribution and configure it for Docker
################################################################################
FROM centos:7 AS builder

RUN mkdir -p /usr/share/elasticsearch
WORKDIR /usr/share/elasticsearch

# Fetch the appropriate Elasticsearch distribution for this architecture
${source_elasticsearch}

RUN tar zxf /opt/elasticsearch.tar.gz --strip-components=1

# Configure the distribution for Docker
RUN sed -i -e 's/ES_DISTRIBUTION_TYPE=tar/ES_DISTRIBUTION_TYPE=docker/' /usr/share/elasticsearch/bin/elasticsearch-env
RUN mkdir -p config config/jvm.options.d data logs
RUN chmod 0775 config config/jvm.options.d data logs
COPY config/elasticsearch.yml config/log4j2.properties config/
RUN chmod 0660 config/elasticsearch.yml config/log4j2.properties

################################################################################
# Step 4. Build the final image, using the rootfs above as the basis, and
# copying in the Elasticsearch distribution
################################################################################
FROM scratch

# Setup the initial filesystem.
COPY --from=rootfs /rootfs /

ENV ELASTIC_CONTAINER true

RUN addgroup -g 1000 elasticsearch && \\
    adduser -D -u 1000 -G elasticsearch -g elasticsearch -h /usr/share/elasticsearch elasticsearch && \\
    addgroup elasticsearch root && \\
    chmod 0775 /usr/share/elasticsearch && \\
    chgrp 0 /usr/share/elasticsearch

WORKDIR /usr/share/elasticsearch
COPY --from=builder --chown=1000:0 /usr/share/elasticsearch /usr/share/elasticsearch

# Replace OpenJDK's built-in CA certificate keystore with the one from the OS
# vendor. The latter is superior in several ways.
# REF: https://github.com/elastic/elasticsearch-docker/issues/171
RUN ln -sf /etc/pki/ca-trust/extracted/java/cacerts /usr/share/elasticsearch/jdk/lib/security/cacerts

ENV PATH /usr/share/elasticsearch/bin:\$PATH

COPY bin/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

# 1. Sync the user and group permissions of /etc/passwd
# 2. Set correct permissions of the entrypoint
# 3. Ensure that there are no files with setuid or setgid, in order to mitigate "stackclash" attacks.
RUN chmod g=u /etc/passwd && \\
    chmod 0775 /usr/local/bin/docker-entrypoint.sh && \\
    find / -xdev -perm -4000 -exec chmod ug-s {} +

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

USER elasticsearch:root

# Our actual entrypoint is `tini`, a minimal but functional init program. It
# calls the entrypoint we provide, while correctly forwarding signals.
ENTRYPOINT ["/bin/tini", "--", "/usr/local/bin/docker-entrypoint.sh"]

# Dummy overridable parameter parsed by entrypoint
CMD ["eswrapper"]

################################################################################
# End of multi-stage Dockerfile
################################################################################
