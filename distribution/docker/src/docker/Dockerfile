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

<% if (docker_base == "ubi") { %>
################################################################################
# Build stage 0 `builder`:
# Extract Elasticsearch artifact
################################################################################
FROM ${base_image} AS builder

# Install required packages to extract the Elasticsearch distribution
RUN ${package_manager} install -y tar gzip

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
    curl --retry 8 -S -L -O https://github.com/krallin/tini/releases/download/v0.19.0/\${tini_bin} ; \\
    curl --retry 8 -S -L -O https://github.com/krallin/tini/releases/download/v0.19.0/\${tini_bin}.sha256sum ; \\
    sha256sum -c \${tini_bin}.sha256sum ; \\
    rm \${tini_bin}.sha256sum ; \\
    mv \${tini_bin} /bin/tini ; \\
    chmod +x /bin/tini

<% } else if (docker_base == 'iron_bank') { %>
${build_args}

FROM ${base_image} AS builder

# `tini` is a tiny but valid init for containers. This is used to cleanly
# control how ES and any child processes are shut down.
COPY tini /bin/tini
RUN chmod 0755 /bin/tini

<% } else { %>

<% /* CentOS builds are actaully a custom base image with a minimal set of dependencies */ %>

################################################################################
# Stage 1. Build curl statically. Installing it from RPM on CentOS pulls in too
# many dependencies.
################################################################################
FROM alpine:latest AS curl

ENV VERSION 7.71.0
ENV TARBALL_URL https://curl.haxx.se/download/curl-\${VERSION}.tar.xz
ENV TARBALL_PATH curl-\${VERSION}.tar.xz

# Install dependencies
RUN apk add gnupg gcc make musl-dev openssl-dev openssl-libs-static file

RUN mkdir /work
WORKDIR /work

# Fetch curl sources and files for validation. Note that alpine's `wget` doesn't have retry options.
RUN function retry_wget() { \\
      local URL="\$1" ; \\
      local DEST="\$2" ; \\
      for iter in {1..10}; do \\
        wget "\$URL" -O "\$DEST" && \\
        exit_code=0 && break || exit_code=\$? && echo "wget error fetching \$URL: retry \$iter in 10s" && sleep 10; \\
      done; \\
      return \$exit_code ; \\
    } ; \\
    retry_wget "https://daniel.haxx.se/mykey.asc" "curl-gpg.pub" && \\
    retry_wget "\${TARBALL_URL}.asc" "\${TARBALL_PATH}.asc" && \\
    retry_wget "\${TARBALL_URL}" "\${TARBALL_PATH}"

# Validate source
RUN gpg --import --always-trust "curl-gpg.pub" && \\
    gpg --verify "\${TARBALL_PATH}.asc" "\${TARBALL_PATH}"

# Unpack and build
RUN tar xfJ "\${TARBALL_PATH}" && \\
    cd "curl-\${VERSION}" && \\
    ./configure --disable-shared --with-ca-fallback --with-ca-bundle=/etc/pki/tls/certs/ca-bundle.crt && \\
    make curl_LDFLAGS="-all-static" && \\
    cp src/curl /work/curl && \\
    strip /work/curl

################################################################################
# Step 2. Create a minimal root filesystem directory. This will form the basis
# for our image.
################################################################################
FROM ${base_image} AS rootfs

ENV BUSYBOX_VERSION 1.31.0
ENV TINI_VERSION 0.19.0

# Start off with an up-to-date system
RUN ${package_manager} update --setopt=tsflags=nodocs -y

# Create a directory into which we will install files
RUN mkdir /rootfs

# Create required devices
RUN mkdir -m 755 /rootfs/dev && \\
    mknod -m 600 /rootfs/dev/console c 5 1 && \\
    mknod -m 600 /rootfs/dev/initctl p && \\
    mknod -m 666 /rootfs/dev/full c 1 7 && \\
    mknod -m 666 /rootfs/dev/null c 1 3 && \\
    mknod -m 666 /rootfs/dev/ptmx c 5 2 && \\
    mknod -m 666 /rootfs/dev/random c 1 8 && \\
    mknod -m 666 /rootfs/dev/tty c 5 0 && \\
    mknod -m 666 /rootfs/dev/tty0 c 4 0 && \\
    mknod -m 666 /rootfs/dev/urandom c 1 9 && \\
    mknod -m 666 /rootfs/dev/zero c 1 5

# Install a minimal set of dependencies, and some for Elasticsearch
RUN ${package_manager} --installroot=/rootfs --releasever=/ --setopt=tsflags=nodocs \\
      --setopt=group_package_types=mandatory -y  \\
      --skip-broken \\
      install basesystem bash zip zlib

# `tini` is a tiny but valid init for containers. This is used to cleanly
# control how ES and any child processes are shut down.
#
# The tini GitHub page gives instructions for verifying the binary using
# gpg, but the keyservers are slow to return the key and this can fail the
# build. Instead, we check the binary against the published checksum.
#
# Also, we use busybox instead of installing utility RPMs, which pulls in
# all kinds of stuff we don't want.
RUN set -e ; \\
    TINI_BIN="" ; \\
    BUSYBOX_ARCH="" ; \\
    case "\$(arch)" in \\
        aarch64) \\
            BUSYBOX_ARCH='armv8l' ; \\
            TINI_BIN='tini-arm64' ; \\
            ;; \\
        x86_64) \\
            BUSYBOX_ARCH='x86_64' ; \\
            TINI_BIN='tini-amd64' ; \\
            ;; \\
        *) echo >&2 "Unsupported architecture \$(arch)" ; exit 1 ;; \\
    esac ; \\
    curl --retry 8 -S -L -O "https://github.com/krallin/tini/releases/download/v0.19.0/\${TINI_BIN}" ; \\
    curl --retry 8 -S -L -O "https://github.com/krallin/tini/releases/download/v0.19.0/\${TINI_BIN}.sha256sum" ; \\
    sha256sum -c "\${TINI_BIN}.sha256sum" ; \\
    rm "\${TINI_BIN}.sha256sum" ; \\
    mv "\${TINI_BIN}" /rootfs/bin/tini ; \\
    chmod +x /rootfs/bin/tini ; \\
    curl --retry 10 -L -o /rootfs/bin/busybox \\
      "https://busybox.net/downloads/binaries/\${BUSYBOX_VERSION}-defconfig-multiarch-musl/busybox-\${BUSYBOX_ARCH}" ; \\
    chmod +x /rootfs/bin/busybox

# Add links for most of the Busybox utilities
RUN set -e ; \\
    for path in \$( /rootfs/bin/busybox --list-full | grep -v bin/sh | grep -v telnet | grep -v unzip); do \\
      ln /rootfs/bin/busybox /rootfs/\$path ; \\
    done

# Curl needs files under here. More importantly, we change Elasticsearch's
# bundled JDK to use /etc/pki/ca-trust/extracted/java/cacerts instead of
# the bundled cacerts.
RUN mkdir -p /rootfs/etc && \\
    cp -a /etc/pki /rootfs/etc/

# Cleanup the filesystem
RUN ${package_manager} --installroot=/rootfs -y clean all && \\
    cd /rootfs && \\
    rm -rf \\
        etc/{X11,centos-release*,csh*,profile*,skel*,yum*} \\
        sbin/sln \\
        usr/bin/rpm \\
        {usr,var}/games \\
        usr/lib/{dracut,systemd,udev} \\
        usr/lib64/X11 \\
        usr/local \\
        usr/share/{awk,centos-release,cracklib,desktop-directories,gcc-*,i18n,icons,licenses,xsessions,zoneinfo} \\
        usr/share/{man,doc,info,games,gdb,ghostscript,gnome,groff,icons,pixmaps,sounds,backgrounds,themes,X11} \\
        usr/{{lib,share}/locale,{lib,lib64}/gconv,bin/localedef,sbin/build-locale-archive} \\
        var/cache/yum \\
        var/lib/{rpm,yum} \\
        var/log/yum.log

# ldconfig
RUN rm -rf /rootfs/etc/ld.so.cache /rootfs/var/cache/ldconfig && \\
    mkdir -p --mode=0755 /rootfs/var/cache/ldconfig

COPY --from=curl /work/curl /rootfs/usr/bin/curl

################################################################################
# Step 3. Fetch the Elasticsearch distribution and configure it for Docker
################################################################################
FROM ${base_image} AS builder

<% } %>

RUN mkdir /usr/share/elasticsearch
WORKDIR /usr/share/elasticsearch

# Fetch the appropriate Elasticsearch distribution for this architecture
${source_elasticsearch}

RUN tar -zxf /opt/elasticsearch.tar.gz --strip-components=1

# Configure the distribution for Docker
RUN sed -i -e 's/ES_DISTRIBUTION_TYPE=tar/ES_DISTRIBUTION_TYPE=docker/' /usr/share/elasticsearch/bin/elasticsearch-env
RUN mkdir -p config config/jvm.options.d data logs plugins
RUN chmod 0775 config config/jvm.options.d data logs plugins
COPY ${config_dir}/elasticsearch.yml ${config_dir}/log4j2.properties config/
RUN chmod 0660 config/elasticsearch.yml config/log4j2.properties

<% if (docker_base == "ubi" || docker_base == "iron_bank") { %>

################################################################################
# Build stage 1 (the actual Elasticsearch image):
#
# Copy elasticsearch from stage 0
# Add entrypoint
################################################################################

FROM ${base_image}

<% if (docker_base == "ubi") { %>

RUN for iter in {1..10}; do \\
      ${package_manager} update --setopt=tsflags=nodocs -y && \\
      ${package_manager} install --setopt=tsflags=nodocs -y \\
        nc shadow-utils zip unzip findutils procps-ng && \\
      ${package_manager} clean all && \\
      exit_code=0 && break || exit_code=\$? && echo "${package_manager} error: retry \$iter in 10s" && \\
      sleep 10; \\
    done; \\
    (exit \$exit_code)

%> } else { %>

<%
/* Reviews of the Iron Bank Dockerfile said that they preferred simpler */
/* scripting so this version doesn't have the retry loop featured above. */
%>
RUN ${package_manager} update --setopt=tsflags=nodocs -y && \\
    ${package_manager} install --setopt=tsflags=nodocs -y \\
      nc shadow-utils zip unzip && \\
    ${package_manager} clean all

<% } %>

RUN groupadd -g 1000 elasticsearch && \\
    adduser -u 1000 -g 1000 -G 0 -d /usr/share/elasticsearch elasticsearch && \\
    chmod 0775 /usr/share/elasticsearch && \\
    chown -R 1000:0 /usr/share/elasticsearch

<% } else { %>

################################################################################
# Stage 4. Build the final image, using the rootfs above as the basis, and
# copying in the Elasticsearch distribution
################################################################################
FROM scratch

# Setup the initial filesystem.
COPY --from=rootfs /rootfs /

RUN addgroup -g 1000 elasticsearch && \\
    adduser -D -u 1000 -G elasticsearch -g elasticsearch -h /usr/share/elasticsearch elasticsearch && \\
    addgroup elasticsearch root && \\
    chmod 0775 /usr/share/elasticsearch && \\
    chgrp 0 /usr/share/elasticsearch

<% } %>

ENV ELASTIC_CONTAINER true

WORKDIR /usr/share/elasticsearch
COPY --from=builder --chown=1000:0 /usr/share/elasticsearch /usr/share/elasticsearch

<% if (docker_base == "ubi" || docker_base == "iron_bank") { %>
COPY --from=builder --chown=0:0 /bin/tini /bin/tini
<% } %>

# Replace OpenJDK's built-in CA certificate keystore with the one from the OS
# vendor. The latter is superior in several ways.
# REF: https://github.com/elastic/elasticsearch-docker/issues/171
RUN ln -sf /etc/pki/ca-trust/extracted/java/cacerts /usr/share/elasticsearch/jdk/lib/security/cacerts

ENV PATH /usr/share/elasticsearch/bin:\$PATH

COPY ${bin_dir}/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

# 1. The JDK's directories' permissions don't allow `java` to be executed under a different
#    group to the default. Fix this.
# 2. Sync the user and group permissions of /etc/passwd
# 3. Set correct permissions of the entrypoint
# 4. Ensure that there are no files with setuid or setgid, in order to mitigate "stackclash" attacks.
# 5. Ensure all files are world-readable by default. It should be possible to
#    examine the contents of the image under any UID:GID
RUN find /usr/share/elasticsearch/jdk -type d -exec chmod 0755 {} + && \\
    chmod g=u /etc/passwd && \\
    chmod 0775 /usr/local/bin/docker-entrypoint.sh && \\
    find / -xdev -perm -4000 -exec chmod ug-s {} + && \\
    find /usr/share/elasticsearch -type f -exec chmod o+r {} +

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

RUN mkdir /licenses && \\
    cp LICENSE.txt /licenses/LICENSE
<% } %>
USER elasticsearch:root

# Our actual entrypoint is `tini`, a minimal but functional init program. It
# calls the entrypoint we provide, while correctly forwarding signals.
ENTRYPOINT ["/bin/tini", "--", "/usr/local/bin/docker-entrypoint.sh"]

# Dummy overridable parameter parsed by entrypoint
CMD ["eswrapper"]

<% if (docker_base == 'iron_bank') { %>
HEALTHCHECK --interval=10s --timeout=5s --start-period=1m --retries=5 CMD curl -I -f --max-time 5 http://localhost:9200 || exit 1
<% } %>

################################################################################
# End of multi-stage Dockerfile
################################################################################
