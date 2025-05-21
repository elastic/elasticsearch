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

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y curl

# `tini` is a tiny but valid init for containers. This is used to cleanly
# control how ES and any child processes are shut down.
#
# The tini GitHub page gives instructions for verifying the binary using
# gpg, but the keyservers are slow to return the key and this can fail the
# build. Instead, we check the binary against the published checksum.
RUN set -eux ; \\
    tini_bin="" ; \\
    case "\$(arch)" in \\
        aarch64) tini_bin='tini-arm64' ;; \\
        x86_64)  tini_bin='tini-amd64' ;; \\
        *) echo >&2 ; echo >&2 "Unsupported architecture \$(arch)" ; echo >&2 ; exit 1 ;; \\
    esac ; \\
    curl --retry 10 -S -L -O https://github.com/krallin/tini/releases/download/v0.19.0/\${tini_bin} ; \\
    curl --retry 10 -S -L -O https://github.com/krallin/tini/releases/download/v0.19.0/\${tini_bin}.sha256sum ; \\
    sha256sum -c \${tini_bin}.sha256sum ; \\
    rm \${tini_bin}.sha256sum ; \\
    mv \${tini_bin} /bin/tini ; \\
    chmod 0555 /bin/tini

WORKDIR /usr/share/elasticsearch
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

# The distribution includes a `config` directory, no need to create it
COPY --chmod=664 config/elasticsearch.yml config/log4j2.properties config/


################################################################################
# Build stage 2 (the actual Elasticsearch image):
#
# Copy elasticsearch from stage 1
# Add entrypoint
################################################################################

FROM ${base_image}

# Change default shell to bash, then install required packages with retries.
RUN yes no | dpkg-reconfigure dash && \\
      export DEBIAN_FRONTEND=noninteractive && \\
      apt-get update && \\
      apt-get upgrade -y && \\
      apt-get install -y --no-install-recommends \\
        ca-certificates curl netcat p11-kit unzip zip  && \\
      apt-get clean && \\
      rm -rf /var/lib/apt/lists/*

RUN groupadd -g 1000 elasticsearch && \\
    adduser --uid 1000 --gid 1000 --home /usr/share/elasticsearch elasticsearch && \\
    adduser elasticsearch root && \\
    chown -R 0:0 /usr/share/elasticsearch

ENV ELASTIC_CONTAINER=true

COPY --from=builder /bin/tini /bin/tini

WORKDIR /usr/share/elasticsearch

COPY --from=builder --chown=0:0 /usr/share/elasticsearch .

# Update "cacerts" bundle to use Ubuntu's CA certificates (and make sure it
# stays up-to-date with changes to Ubuntu's store)
COPY bin/docker-openjdk /etc/ca-certificates/update.d/docker-openjdk
RUN /etc/ca-certificates/update.d/docker-openjdk

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
