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
# Build stage 0 `builder`:
# Extract Elasticsearch artifact
################################################################################

${build_args}

FROM ${base_image} AS builder

<% if (docker_base == 'ubi') { %>
# Install required packages to extract the Elasticsearch distribution
RUN <%= retry.loop(package_manager, "${package_manager} install -y findutils tar gzip") %>
<% } %>

<% if (docker_base == 'iron_bank') { %>
# `tini` is a tiny but valid init for containers. This is used to cleanly
# control how ES and any child processes are shut down.
COPY tini /bin/tini
RUN chmod 0755 /bin/tini
<% } else { %>

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
    chmod +x /bin/tini

<% } %>

RUN mkdir /usr/share/elasticsearch
WORKDIR /usr/share/elasticsearch

${source_elasticsearch}

RUN tar -zxf /opt/elasticsearch.tar.gz --strip-components=1

# The distribution includes a `config` directory, no need to create it
COPY ${config_dir}/elasticsearch.yml config/
COPY ${bin_dir}/transform-log4j-config-${version}.jar /tmp/

# 1. Configure the distribution for Docker
# 2. Ensure directories are created. Most already are, but make sure
# 3. Apply correct permissions
# 4. Move the distribution's default logging config aside
# 5. Generate a docker logging config, to be used by default
# 6. Apply more correct permissions
# 7. The JDK's directories' permissions don't allow `java` to be executed under a different
#    group to the default. Fix this.
# 8. Ensure that there are no files with setuid or setgid, in order to mitigate "stackclash" attacks.
# 9. Ensure all files are world-readable by default. It should be possible to
#    examine the contents of the image under any UID:GID
RUN sed -i -e 's/ES_DISTRIBUTION_TYPE=tar/ES_DISTRIBUTION_TYPE=docker/' bin/elasticsearch-env && \\
    mkdir -p config/jvm.options.d data logs plugins && \\
    chmod 0775 config config/jvm.options.d data logs plugins && \\
    mv config/log4j2.properties config/log4j2.file.properties && \\
    jdk/bin/java -jar /tmp/transform-log4j-config-${version}.jar config/log4j2.file.properties > config/log4j2.properties && \\
    chmod 0660 config/elasticsearch.yml config/log4j2*.properties && \\
    find ./jdk -type d -exec chmod 0755 {} + && \\
    find . -xdev -perm -4000 -exec chmod ug-s {} + && \\
    find . -type f -exec chmod o+r {} +

################################################################################
# Build stage 1 (the actual Elasticsearch image):
#
# Copy elasticsearch from stage 0
# Add entrypoint
################################################################################

FROM ${base_image}

<% if (docker_base == "iron_bank") { %>
<%
/* Reviews of the Iron Bank Dockerfile said that they preferred simpler */
/* scripting so this version doesn't have the retry loop featured below. */
%>
RUN ${package_manager} update --setopt=tsflags=nodocs -y && \\
    ${package_manager} install --setopt=tsflags=nodocs -y \\
      nc shadow-utils zip unzip && \\
    ${package_manager} clean all

<% } else { %>

RUN <%= retry.loop(
    package_manager,
      "${package_manager} update --setopt=tsflags=nodocs -y && \n" +
      "      ${package_manager} install --setopt=tsflags=nodocs -y \n" +
      "      nc shadow-utils zip unzip ${docker_base == 'ubi' ? 'findutils procps-ng' : ''} && \n" +
      "      ${package_manager} clean all"
    ) %>

<% } %>

RUN groupadd -g 1000 elasticsearch && \\
    adduser -u 1000 -g 1000 -G 0 -d /usr/share/elasticsearch elasticsearch && \\
    chmod 0775 /usr/share/elasticsearch && \\
    chown -R 1000:0 /usr/share/elasticsearch

ENV ELASTIC_CONTAINER true

WORKDIR /usr/share/elasticsearch
COPY --from=builder --chown=1000:0 /usr/share/elasticsearch /usr/share/elasticsearch
COPY --from=builder --chown=0:0 /bin/tini /bin/tini

ENV PATH /usr/share/elasticsearch/bin:\$PATH

COPY ${bin_dir}/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

# 1. Sync the user and group permissions of /etc/passwd
# 2. Set correct permissions of the entrypoint
# 3. Ensure that there are no files with setuid or setgid, in order to mitigate "stackclash" attacks.
#    We've already run this in previous layers so it ought to be a no-op.
# 4. Replace OpenJDK's built-in CA certificate keystore with the one from the OS
#    vendor. The latter is superior in several ways.
#    REF: https://github.com/elastic/elasticsearch-docker/issues/171
RUN chmod g=u /etc/passwd && \\
    chmod 0775 /usr/local/bin/docker-entrypoint.sh && \\
    find / -xdev -perm -4000 -exec chmod ug-s {} + && \\
    ln -sf /etc/pki/ca-trust/extracted/java/cacerts /usr/share/elasticsearch/jdk/lib/security/cacerts

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

<% if (docker_base == 'ubi' || docker_base == 'iron_bank') { %>
LABEL name="Elasticsearch" \\
  maintainer="infra@elastic.co" \\
  vendor="Elastic" \\
  version="${version}" \\
  release="1" \\
  summary="Elasticsearch" \\
  description="You know, for search."
<% } %>

<% if (docker_base == 'ubi') { %>
RUN mkdir /licenses && cp LICENSE.txt /licenses/LICENSE
<% } else if (docker_base == 'iron_bank') { %>
RUN mkdir /licenses && cp LICENSE.txt /licenses/LICENSE
COPY LICENSE /licenses/LICENSE.addendum
<% } %>

ENTRYPOINT ["/bin/tini", "--", "/usr/local/bin/docker-entrypoint.sh"]
# Dummy overridable parameter parsed by entrypoint
CMD ["eswrapper"]

<% if (docker_base == 'iron_bank') { %>
HEALTHCHECK --interval=10s --timeout=5s --start-period=1m --retries=5 CMD curl -I -f --max-time 5 http://localhost:9200 || exit 1
<% } %>

################################################################################
# End of multi-stage Dockerfile
################################################################################
