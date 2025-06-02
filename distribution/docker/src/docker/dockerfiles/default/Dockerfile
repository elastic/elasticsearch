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

RUN microdnf install -y findutils tar gzip

# `tini` is a tiny but valid init for containers. This is used to cleanly
# control how ES and any child processes are shut down.
#
# The tini GitHub page gives instructions for verifying the binary using
# gpg, but the keyservers are slow to return the key and this can fail the
# build. Instead, we check the binary against the published checksum.
RUN set -eux; \\
    arch="\$(rpm --query --queryformat='%{ARCH}' rpm)"; \\
    case "\$(arch)" in \\
        aarch64) tini_bin='tini-arm64'; tini_sum='07952557df20bfd2a95f9bef198b445e006171969499a1d361bd9e6f8e5e0e81' ;; \\
        x86_64)  tini_bin='tini-amd64'; tini_sum='93dcc18adc78c65a028a84799ecf8ad40c936fdfc5f2a57b1acda5a8117fa82c' ;; \\
        *) echo >&2 "Unsupported architecture \$arch"; exit 1 ;; \\
    esac ; \\
    curl -f --retry 10 -S -L -o /tmp/tini https://github.com/krallin/tini/releases/download/v0.19.0/\${tini_bin}; \\
    echo "\${tini_sum}  /tmp/tini" | sha256sum -c -; \\
    mv /tmp/tini /bin/tini; \\
    chmod 0555 /bin/tini

WORKDIR /usr/share/elasticsearch
RUN arch="\$(rpm --query --queryformat='%{ARCH}' rpm)" && curl -f --retry 10 -S -L --output /tmp/elasticsearch.tar.gz https://artifacts-no-kpi.elastic.co/downloads/elasticsearch/elasticsearch-${version}-linux-\$(arch).tar.gz
RUN tar -zxf /tmp/elasticsearch.tar.gz --strip-components=1 && \\
# Configure the distribution for Docker
    sed -i -e 's/ES_DISTRIBUTION_TYPE=tar/ES_DISTRIBUTION_TYPE=docker/' bin/elasticsearch-env && \\
# Create required directory
    mkdir data && \\
# Reset permissions on all directories
    find . -type d -exec chmod 0555 {} + && \\
# keep default elasticsearch log4j config
    mv config/log4j2.properties config/log4j2.file.properties && \\
# Reset permissions on all files
    find . -type f -exec chmod 0444 {} + && \\
# Make CLI tools executable
    chmod 0555 bin/* jdk/bin/* jdk/lib/jspawnhelper modules/x-pack-ml/platform/linux-*/bin/* && \\
# Make some directories writable. `bin` must be writable because
# plugins can install their own CLI utilities.
    chmod 0775 bin config config/jvm.options.d data logs plugins && \\
# Make some files writable
    find config -type f -exec chmod 0664 {} + && \\
# Tighten up permissions on the ES home dir (the permissions of the contents are handled below)
    chmod 0775 . && \\
# You can't install plugins that include configuration when running as `elasticsearch` and the `config`
# dir is owned by `root`, because the installed tries to manipulate the permissions on the plugin's
# config directory.
    chown 1000:1000 bin config config/jvm.options.d data logs plugins

# The distribution includes a `config` directory, no need to create it
COPY --chmod=664 config/elasticsearch.yml config/log4j2.properties config/


################################################################################
# Build stage 2 (the actual Elasticsearch image):
#
# Copy elasticsearch from stage 1
# Add entrypoint
################################################################################

FROM ${base_image}

RUN microdnf install --setopt=tsflags=nodocs -y \\
    nc shadow-utils zip unzip findutils procps-ng && \\
    microdnf clean all

RUN groupadd -g 1000 elasticsearch && \\
    adduser -u 1000 -g 1000 -G 0 -d /usr/share/elasticsearch elasticsearch && \\
    chown -R 0:0 /usr/share/elasticsearch

ENV ELASTIC_CONTAINER=true

COPY --from=builder /bin/tini /bin/tini

WORKDIR /usr/share/elasticsearch

COPY --from=builder --chown=0:0 /usr/share/elasticsearch .

# Replace OpenJDK's built-in CA certificate keystore with the one from the OS
# vendor. The latter is superior in several ways.
# REF: https://github.com/elastic/elasticsearch-docker/issues/171
RUN ln -sf /etc/pki/ca-trust/extracted/java/cacerts jdk/lib/security/cacerts

ENV PATH=/usr/share/elasticsearch/bin:\$PATH
ENV SHELL=/bin/bash

COPY --chmod=0555 bin/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

RUN chmod g=u /etc/passwd && \\
    chmod 0555 /usr/local/bin/docker-entrypoint.sh && \\
    find / -xdev -perm -4000 -exec chmod ug-s {} + && \\
    chmod 0775 /usr/share/elasticsearch && \\
    chown elasticsearch bin config config/jvm.options.d data logs plugins


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

# Our actual entrypoint is `tini`, a minimal but functional init program. It
# calls the entrypoint we provide, while correctly forwarding signals.
ENTRYPOINT ["/bin/tini", "--", "/usr/local/bin/docker-entrypoint.sh"]
# Dummy overridable parameter parsed by entrypoint
CMD ["eswrapper"]

USER 1000:0

################################################################################
# End of multi-stage Dockerfile
################################################################################
