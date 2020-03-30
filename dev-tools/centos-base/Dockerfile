# Phase 1 - More-or-less statically build curl so that we don't have to
# install all the dependencies that the CentOS version pulls in.

FROM centos:7 AS builder

ENV LDFLAGS=-static

RUN mkdir /work

RUN yum update -y
RUN yum install -y \
      file \
      gcc \
      glibc-static \
      make \
      perl

# Static libz
WORKDIR /work
RUN curl https://www.zlib.net/zlib-1.2.11.tar.gz > zlib-1.2.11.tar.gz \
      && tar xf zlib-1.2.11.tar.gz
WORKDIR /work/zlib-1.2.11
RUN ./configure --static \
      && make \
      && make install

# Static openssl
WORKDIR /work
RUN curl https://www.openssl.org/source/openssl-1.1.1e.tar.gz > openssl-1.1.1e.tar.gz
RUN curl https://www.openssl.org/source/openssl-1.1.1e.tar.gz.sha1 > openssl-1.1.1e.tar.gz.sha1
# The OpenSSL checksum doesn't include the filename, so we can't use `sha1sum -c`
RUN sha1sum openssl-1.1.1e.tar.gz | cut -f1 -d" " | diff - openssl-1.1.1e.tar.gz.sha1
RUN tar xf openssl-1.1.1e.tar.gz
WORKDIR /work/openssl-1.1.1e
RUN ./config -v no-shared \
      && make \
      && make install

# Static curl
WORKDIR /work
RUN curl https://curl.haxx.se/download/curl-7.69.1.tar.gz > curl-7.69.1.tar.gz \
      && tar xf curl-7.69.1.tar.gz
WORKDIR /work/curl-7.69.1
RUN ./configure \
        --with-ssl=/usr/local \
        --disable-shared \
        --enable-static \
        --prefix=/tmp/curl \
        --disable-ldap \
        --disable-sspi \
        --without-librtmp \
        --disable-ftp \
        --disable-file \
        --disable-dict \
        --disable-telnet \
        --disable-tftp \
        --disable-rtsp \
        --disable-pop3 \
        --disable-imap \
        --disable-smtp \
        --disable-gopher \
        --disable-smb \
        --without-libidn \
        --disable-ares \
      && make \
      && make install \
      && strip /tmp/curl/bin/curl

# Phase 2 - build an image that can be executed to build a minimal image.
FROM centos:7

# Install docker
RUN yum install -y yum-utils device-mapper-persistent-data lvm2
RUN yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
RUN yum install -y docker-ce docker-ce-cli containerd.io

# Required for java-latest-openjdk-headless
RUN yum install -y epel-release

RUN mkdir /build

ADD mkimage-elasticsearch.sh /build
COPY --from=builder /tmp/curl/bin/curl /build/curl

# Update the tag if we move off centos:7
CMD ["/build/mkimage-elasticsearch.sh", "-t", "7", "elasticsearch/centos-base"]
