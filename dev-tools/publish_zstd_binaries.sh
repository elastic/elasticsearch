#!/usr/bin/env bash
#
 # Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 # or more contributor license agreements. Licensed under the "Elastic License
 # 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 # Public License v 1"; you may not use this file except in compliance with, at
 # your election, the "Elastic License 2.0", the "GNU Affero General Public
 # License v3.0 only", or the "Server Side Public License, v 1".
#

set -e

if [ "$#" -ne 1 ]; then
    printf 'Usage: %s <version>\n' "$(basename "$0")"
    exit 0;
fi

if [ $(docker buildx inspect --bootstrap | grep -c 'Platforms:.*linux/arm64') -ne 1 ]; then
  echo 'Error: No Docker support for linux/arm64 detected'
  echo 'For more information see https://docs.docker.com/build/building/multi-platform'
  exit 1;
fi

if [ -z "$ARTIFACTORY_API_KEY" ]; then
  echo 'Error: The ARTIFACTORY_API_KEY environment variable must be set.'
  exit 1;
fi

VERSION="$1"
ARTIFACTORY_REPOSITORY="${ARTIFACTORY_REPOSITORY:-https://artifactory.elastic.dev/artifactory/elasticsearch-native/}"
TEMP=$(mktemp -d)

fetch_homebrew_artifact() {
  DIGEST=$(curl -sS --retry 3 -H "Accept: application/vnd.oci.image.index.v1+json" -H "Authorization: Bearer QQ==" \
      --location "https://ghcr.io/v2/homebrew/core/zstd/manifests/$VERSION" | jq -r \
      ".manifests[] | select(.platform.os == \"darwin\" and .platform.architecture == \"$1\" and .platform.\"os.version\" == \"macOS 13\") | .annotations.\"sh.brew.bottle.digest\"")

  OUTPUT_FILE="$TEMP/zstd-$VERSION-darwin-$1.tar.gz"
  curl -sS --retry 3 -H "Authorization: Bearer QQ==" --output "$OUTPUT_FILE" --location "https://ghcr.io/v2/homebrew/core/zstd/blobs/sha256:$DIGEST"
  echo $OUTPUT_FILE
}

download_license() {
  curl -sS --retry 3 --location https://raw.githubusercontent.com/facebook/zstd/v${VERSION}/LICENSE --output $1
}

echo 'Downloading MacOS zstd binaries...'
DARWIN_ARM_BREW=$(fetch_homebrew_artifact 'arm64')
DARWIN_X86_BREW=$(fetch_homebrew_artifact 'amd64')

build_darwin_jar() {
  ARTIFACT="$TEMP/zstd-$VERSION-darwin-$2.jar"
  TAR_DIR="$TEMP/darwin-$2"
  mkdir $TAR_DIR
  tar zxf $1 --strip-components=2 --include="*/LICENSE" --include="*/libzstd.$VERSION.dylib" -C $TAR_DIR && rm $1
  mv $TAR_DIR/lib/libzstd.$VERSION.dylib $TAR_DIR/libzstd.dylib && rm -rf $TAR_DIR/lib
  FILE_COUNT=$(ls -1 $TAR_DIR | wc -l | xargs)
  if [ "$FILE_COUNT" -ne 2 ]; then
    >&2 echo "ERROR: Expected 2 files in $TAR_DIR but found $FILE_COUNT"
    exit 1
  fi
  (cd $TAR_DIR/../ && zip -rq - $(basename $TAR_DIR)) > $ARTIFACT && rm -rf $TAR_DIR
  echo $ARTIFACT
}

echo 'Building MacOS jars...'
DARWIN_ARM_JAR=$(build_darwin_jar $DARWIN_ARM_BREW "aarch64")
DARWIN_X86_JAR=$(build_darwin_jar $DARWIN_X86_BREW "x86-64")

build_linux_jar() {
  ARTIFACT="$TEMP/zstd-$VERSION-linux-$2.jar"
  OUTPUT_DIR="$TEMP/linux-$2"
  mkdir $OUTPUT_DIR
  DOCKER_IMAGE=$(docker build --build-arg="ZSTD_VERSION=1.5.5" --file zstd.Dockerfile --platform $1 --quiet .)
  docker run --platform $1 $DOCKER_IMAGE > $OUTPUT_DIR/libzstd.so
  download_license $OUTPUT_DIR/LICENSE
  (cd $OUTPUT_DIR/../ && zip -rq - $(basename $OUTPUT_DIR)) > $ARTIFACT && rm -rf $OUTPUT_DIR
  echo $ARTIFACT
}

echo 'Building Linux jars...'
LINUX_ARM_JAR=$(build_linux_jar "linux/arm64" "aarch64")
LINUX_X86_JAR=$(build_linux_jar "linux/amd64" "x86-64")

build_windows_jar() {
  ARTIFACT="$TEMP/zstd-$VERSION-windows-x86-64.jar"
  OUTPUT_DIR="$TEMP/win32-x86-64"
  mkdir $OUTPUT_DIR
  curl -sS --retry 3 --location https://github.com/facebook/zstd/releases/download/v${VERSION}/zstd-v${VERSION}-win64.zip --output $OUTPUT_DIR/zstd.zip
  unzip -jq $OUTPUT_DIR/zstd.zip zstd-v${VERSION}-win64/dll/libzstd.dll -d $OUTPUT_DIR && rm $OUTPUT_DIR/zstd.zip
  mv $OUTPUT_DIR/libzstd.dll $OUTPUT_DIR/zstd.dll
  download_license $OUTPUT_DIR/LICENSE
  (cd $OUTPUT_DIR/../ && zip -rq - $(basename $OUTPUT_DIR)) > $ARTIFACT && rm -rf $OUTPUT_DIR
  echo $ARTIFACT
}

echo 'Building Windows jar...'
WINDOWS_X86_JAR=$(build_windows_jar)

upload_artifact() {
  curl -sS -X PUT -H "X-JFrog-Art-Api: ${ARTIFACTORY_API_KEY}" --data-binary "@$1" --location "${ARTIFACTORY_REPOSITORY}/org/elasticsearch/zstd/${VERSION}/$(basename $1)"
}

echo 'Uploading artifacts...'
upload_artifact ${DARWIN_ARM_JAR}
upload_artifact ${DARWIN_X86_JAR}
upload_artifact ${LINUX_ARM_JAR}
upload_artifact ${LINUX_X86_JAR}
upload_artifact ${WINDOWS_X86_JAR}

rm -rf $TEMP
