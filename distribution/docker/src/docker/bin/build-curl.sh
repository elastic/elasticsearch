#!/bin/sh

# This script is based upon the following URL, but it has been modified so that
# Docker is run externally to this script.
#
# https://github.com/dtschan/curl-static/blob/ab8ccc3ff140af860065c04b1e9bcd20bbe2c2d2/build.sh
#
# For license and copyright information, see:
#
# https://github.com/dtschan/curl-static/blob/ab8ccc3ff1/LICENSE

# This script must run in an Alpine Linux environment

set -e

VERSION=7.70.0
# If you prefer to just build the latest version, use the following line:
# VERSION=LATEST
GPG_KEY_URL="https://daniel.haxx.se/mykey.asc"
GPG_KEY_PATH="/work/curl-gpg.pub"


mkdir /work

# Print failure message if we exit unexpectedly
trap 'RC="$?"; echo "*** FAILED! RC=${RC}"; exit ${RC}' EXIT

# Fetch a url to a location unless it already exists
conditional_fetch () {
  local URL=$1
  local OUTPUT_PATH=$2
  if [ -e ${OUTPUT_PATH} ]; then
    echo "Found existing ${OUTPUT_PATH}; reusing..."
  else
    echo "Fetching ${URL} to ${OUTPUT_PATH}..."
    wget "${URL}" -O "${OUTPUT_PATH}"
  fi
}

# Determine tarball filename
if [ "$VERSION" = 'LATEST' ]; then
  echo "Determining latest version..."
  TARBALL_FILENAME=$(wget "https://curl.haxx.se/download/?C=M;O=D" -q -O- | grep -w -m 1 -o 'curl-.*\.tar\.xz"' | sed 's/"$//')
else
  TARBALL_FILENAME=curl-${VERSION}.tar.xz
fi

# Set some variables (depends on tarball filename determined above)
TARBALL_URL=https://curl.haxx.se/download/${TARBALL_FILENAME}
TARBALL_PATH=/work/${TARBALL_FILENAME}
FINAL_BIN_PATH=/work/curl

echo "*** Fetching ${TARBALL_FILENAME} and files to validate it..."
conditional_fetch "${GPG_KEY_URL}" "${GPG_KEY_PATH}"
conditional_fetch "${TARBALL_URL}.asc" "${TARBALL_PATH}.asc"
conditional_fetch "${TARBALL_URL}" "${TARBALL_PATH}"

echo "*** Validating source..."
apk add gnupg
gpg --import --always-trust ${GPG_KEY_PATH}
gpg --verify ${TARBALL_PATH}.asc ${TARBALL_PATH}

echo "*** Unpacking source..."
tar xfJ ${TARBALL_PATH}
cd curl-*

echo "*** Installing build dependencies..."
apk add gcc make musl-dev openssl-dev openssl-libs-static file

echo "*** configuring..."
# The bundle path is set so that our `curl` can fetch an https URL on
# CentOS
./configure --disable-shared --with-ca-fallback --with-ca-bundle=/etc/pki/tls/certs/ca-bundle.crt
echo "making..."
make curl_LDFLAGS=-all-static

echo "*** Finishing up..."
cp src/curl ${FINAL_BIN_PATH}
strip ${FINAL_BIN_PATH}
chown $(id -u):$(id -g) ${FINAL_BIN_PATH}

# Clear the trap so when we exit there is no failure message
trap - EXIT
echo SUCCESS
ls -ld ${FINAL_BIN_PATH}
du -h ${FINAL_BIN_PATH}
