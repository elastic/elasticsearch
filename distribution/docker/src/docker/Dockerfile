################################################################################
# This Dockerfile was generated from the template at distribution/src/docker/Dockerfile
#
# Beginning of multi stage Dockerfile
################################################################################

################################################################################
# Build stage 0 `builder`:
# Extract elasticsearch artifact
# Install required plugins
# Set gid=0 and make group perms==owner perms
################################################################################

FROM centos:7 AS builder

RUN for iter in {1..10}; do yum update --setopt=tsflags=nodocs -y && \
    yum install --setopt=tsflags=nodocs -y gzip shadow-utils tar && \
    yum clean all && exit_code=0 && break || exit_code=\$? && echo "yum error: retry \$iter in 10s" && sleep 10; done; \
    (exit \$exit_code)

ENV PATH /usr/share/elasticsearch/bin:\$PATH

RUN groupadd -g 1000 elasticsearch && \
    adduser -u 1000 -g 1000 -d /usr/share/elasticsearch elasticsearch

WORKDIR /usr/share/elasticsearch

${source_elasticsearch}

RUN tar zxf /opt/${elasticsearch} --strip-components=1
RUN grep ES_DISTRIBUTION_TYPE=tar /usr/share/elasticsearch/bin/elasticsearch-env \
    && sed -i -e 's/ES_DISTRIBUTION_TYPE=tar/ES_DISTRIBUTION_TYPE=docker/' /usr/share/elasticsearch/bin/elasticsearch-env
RUN mkdir -p config data logs
RUN chmod 0775 config data logs
COPY config/elasticsearch.yml config/log4j2.properties config/
RUN chmod 0660 config/elasticsearch.yml config/log4j2.properties

# `tini` is a tiny but valid init for containers. This is used to cleanly
# control how ES and any child processes are shut down.
#
# The tini GitHub page gives instructions for verifying the binary using
# gpg, but the keyservers are slow to return the key and this can fail the
# build. Instead, we check the binary against a checksum that we have
# computed.
ADD https://github.com/krallin/tini/releases/download/v0.18.0/tini /tini
COPY config/tini.sha512 /tini.sha512
RUN sha512sum -c /tini.sha512 && chmod +x /tini

################################################################################
# Build stage 1 (the actual elasticsearch image):
# Copy elasticsearch from stage 0
# Add entrypoint
################################################################################

FROM centos:7

ENV ELASTIC_CONTAINER true

COPY --from=builder /tini /tini

RUN for iter in {1..10}; do yum update --setopt=tsflags=nodocs -y && \
    yum install --setopt=tsflags=nodocs -y nc shadow-utils zip unzip && \
    yum clean all && exit_code=0 && break || exit_code=\$? && echo "yum error: retry \$iter in 10s" && sleep 10; done; \
    (exit \$exit_code)

RUN groupadd -g 1000 elasticsearch && \
    adduser -u 1000 -g 1000 -G 0 -d /usr/share/elasticsearch elasticsearch && \
    chmod 0775 /usr/share/elasticsearch && \
    chgrp 0 /usr/share/elasticsearch

WORKDIR /usr/share/elasticsearch
COPY --from=builder --chown=1000:0 /usr/share/elasticsearch /usr/share/elasticsearch

# Replace OpenJDK's built-in CA certificate keystore with the one from the OS
# vendor. The latter is superior in several ways.
# REF: https://github.com/elastic/elasticsearch-docker/issues/171
RUN ln -sf /etc/pki/ca-trust/extracted/java/cacerts /usr/share/elasticsearch/jdk/lib/security/cacerts

ENV PATH /usr/share/elasticsearch/bin:\$PATH

COPY bin/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

RUN chmod g=u /etc/passwd && \
    chmod 0775 /usr/local/bin/docker-entrypoint.sh

# Ensure that there are no files with setuid or setgid, in order to mitigate "stackclash" attacks.
RUN find / -xdev -perm -4000 -exec chmod ug-s {} +

EXPOSE 9200 9300

LABEL org.label-schema.build-date="${build_date}" \
  org.label-schema.license="${license}" \
  org.label-schema.name="Elasticsearch" \
  org.label-schema.schema-version="1.0" \
  org.label-schema.url="https://www.elastic.co/products/elasticsearch" \
  org.label-schema.usage="https://www.elastic.co/guide/en/elasticsearch/reference/index.html" \
  org.label-schema.vcs-ref="${git_revision}" \
  org.label-schema.vcs-url="https://github.com/elastic/elasticsearch" \
  org.label-schema.vendor="Elastic" \
  org.label-schema.version="${version}" \
  org.opencontainers.image.created="${build_date}" \
  org.opencontainers.image.documentation="https://www.elastic.co/guide/en/elasticsearch/reference/index.html" \
  org.opencontainers.image.licenses="${license}" \
  org.opencontainers.image.revision="${git_revision}" \
  org.opencontainers.image.source="https://github.com/elastic/elasticsearch" \
  org.opencontainers.image.title="Elasticsearch" \
  org.opencontainers.image.url="https://www.elastic.co/products/elasticsearch" \
  org.opencontainers.image.vendor="Elastic" \
  org.opencontainers.image.version="${version}"

USER elasticsearch:root

ENTRYPOINT ["/tini", "--", "/usr/local/bin/docker-entrypoint.sh"]
# Dummy overridable parameter parsed by entrypoint
CMD ["eswrapper"]

################################################################################
# End of multi-stage Dockerfile
################################################################################
