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

<% if (docker_base == 'iron_bank') { %>
ARG BASE_REGISTRY=registry1.dso.mil
ARG BASE_IMAGE=ironbank/redhat/ubi/ubi9
ARG BASE_TAG=9.4
<% } %>

################################################################################
# Build stage 1 `builder`:
# Extract Elasticsearch artifact
################################################################################

FROM ${base_image} AS builder

<% if (docker_base == 'iron_bank') { %>
# `tini` is a tiny but valid init for containers. This is used to cleanly
# control how ES and any child processes are shut down.
COPY tini /bin/tini
RUN chmod 0555 /bin/tini

<% } else { %>

# Install required packages to extract the Elasticsearch distribution
<% if (docker_base == "wolfi") { %>
RUN <%= retry.loop(package_manager, "export DEBIAN_FRONTEND=noninteractive && ${package_manager} update && ${package_manager} update && ${package_manager} add --no-cache curl") %>
<% } else { %>
RUN <%= retry.loop(package_manager, "${package_manager} install -y findutils tar gzip") %>
<% } %>

<% if (docker_base != 'wolfi') { %>
    # `tini` is a tiny but valid init for containers. This is used to cleanly
    # control how ES and any child processes are shut down.
    # For wolfi we pick it from the blessed wolfi package registry.
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
<% } %>

<% } %>

RUN mkdir /usr/share/elasticsearch
WORKDIR /usr/share/elasticsearch

<% if (docker_base == "iron_bank") {
  // Iron Bank always copies the local artifact. It uses `arch` from the
  // template context variables.
%>
COPY elasticsearch-${version}-linux-${arch}.tar.gz /tmp/elasticsearch.tar.gz
<% } else {
  // Fetch the appropriate Elasticsearch distribution for this architecture.
  // Keep this command on one line - it is replaced with a `COPY` during local builds.
  // It uses the `arch` shell command to fetch the correct distro for the build machine,
  // which is needed for Docker Hub builds.
%>
RUN curl --retry 10 -S -L --output /tmp/elasticsearch.tar.gz https://artifacts-no-kpi.elastic.co/downloads/elasticsearch/elasticsearch-${version}-linux-\$(arch).tar.gz
<% } %>

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

################################################################################
# Build stage 2 (the actual Elasticsearch image):
#
# Copy elasticsearch from stage 1
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
      nc shadow-utils zip findutils unzip procps-ng && \\
    ${package_manager} clean all

<% } else if (docker_base == "wolfi") { %>
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

# Optionally set Bash as the default shell in the container at runtime
CMD ["/bin/bash"]

<% } else { %>

RUN <%= retry.loop(
    package_manager,
      "${package_manager} update --setopt=tsflags=nodocs -y && \n" +
      "      ${package_manager} install --setopt=tsflags=nodocs -y \n" +
      "        nc shadow-utils zip unzip findutils procps-ng && \n" +
      "      ${package_manager} clean all"
    ) %>

<% } %>


<% if (docker_base == "wolfi") { %>
RUN groupadd -g 1000 elasticsearch && \
    adduser -G elasticsearch -u 1000 elasticsearch -D --home /usr/share/elasticsearch elasticsearch && \
    adduser elasticsearch root && \
    chown -R 0:0 /usr/share/elasticsearch
<% } else { %>
RUN groupadd -g 1000 elasticsearch && \\
    adduser -u 1000 -g 1000 -G 0 -d /usr/share/elasticsearch elasticsearch && \\
    chown -R 0:0 /usr/share/elasticsearch
<% } %>

ENV ELASTIC_CONTAINER true

WORKDIR /usr/share/elasticsearch

COPY --from=builder --chown=0:0 /usr/share/elasticsearch /usr/share/elasticsearch
<% if (docker_base != "wolfi") { %>
COPY --from=builder --chown=0:0 /bin/tini /bin/tini
<% } %>

ENV PATH /usr/share/elasticsearch/bin:\$PATH
ENV SHELL /bin/bash
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

<% if (docker_base == 'wolfi') { %>
RUN ln -sf /etc/ssl/certs/java/cacerts /usr/share/elasticsearch/jdk/lib/security/cacerts
<% } else { %>
RUN ln -sf /etc/pki/ca-trust/extracted/java/cacerts /usr/share/elasticsearch/jdk/lib/security/cacerts
<% } %>

EXPOSE 9200 9300

<% if (docker_base != 'iron_bank') { %>
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
<% } %>

RUN mkdir /licenses && cp LICENSE.txt /licenses/LICENSE
<% if (docker_base == 'iron_bank') { %>
COPY LICENSE /licenses/LICENSE.addendum
<% } %>

<% if (docker_base == "wolfi") { %>
# Our actual entrypoint is `tini`, a minimal but functional init program. It
# calls the entrypoint we provide, while correctly forwarding signals.
ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/docker-entrypoint.sh"]
# Dummy overridable parameter parsed by entrypoint
CMD ["eswrapper"]
<% } else { %>
# Our actual entrypoint is `tini`, a minimal but functional init program. It
# calls the entrypoint we provide, while correctly forwarding signals.
ENTRYPOINT ["/bin/tini", "--", "/usr/local/bin/docker-entrypoint.sh"]
# Dummy overridable parameter parsed by entrypoint
CMD ["eswrapper"]
<% } %>

USER 1000:0

<% if (docker_base == 'iron_bank') { %>
HEALTHCHECK --interval=10s --timeout=5s --start-period=1m --retries=5 CMD curl -I -f --max-time 5 http://localhost:9200 || exit 1
<% } %>

################################################################################
# End of multi-stage Dockerfile
################################################################################
