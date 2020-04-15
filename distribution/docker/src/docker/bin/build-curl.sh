#!/bin/bash

set -ex

cd /work

# 1. Download everything up-front, since there's no point continuing if we
# can't fetch everything.
curl -o zlib-1.2.11.tar.gz https://www.zlib.net/zlib-1.2.11.tar.gz
curl -o openssl-1.1.1f.tar.gz https://www.openssl.org/source/openssl-1.1.1f.tar.gz
curl -o openssl-1.1.1f.tar.gz.sha256 https://www.openssl.org/source/openssl-1.1.1f.tar.gz.sha256
curl -o curl-7.69.1.tar.gz https://curl.haxx.se/download/curl-7.69.1.tar.gz

export LDFLAGS=-static

# The OpenSSL checksum doesn't include the filename, so we can't use `sha256sum -c`
sha256sum openssl-1.1.1f.tar.gz | cut -f1 -d" " | diff - openssl-1.1.1f.tar.gz.sha256

tar xf zlib-1.2.11.tar.gz
tar xf openssl-1.1.1f.tar.gz
tar xf curl-7.69.1.tar.gz

yum update -y
yum install -y \
  file \
  gcc \
  glibc-static \
  make \
  perl

# Statically build libz
cd /work/zlib-1.2.11
./configure --static
make
make install

# Statically build openssl
cd /work/openssl-1.1.1f
./config -v no-shared
make
make install

# Statically build curl
cd /work/curl-7.69.1
./configure \
  --with-ssl=/usr/local \
  --disable-shared \
  --enable-static \
  --prefix=/work/build \
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
  --disable-ares
make
make install
strip /work/build/bin/curl
