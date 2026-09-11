#!/usr/bin/env bash
#
 # Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 # or more contributor license agreements. Licensed under the "Elastic License
 # 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 # Public License v 1"; you may not use this file except in compliance with, at
 # your election, the "Elastic License 2.0", the "GNU Affero General Public
 # License v3.0 only", or the "Server Side Public License, v 1".
#
# Builds and publishes prebuilt zstd native libraries to Artifactory.
# Linux binaries are built via Docker (see zstd.Dockerfile); macOS binaries
# are sourced from Homebrew; Windows binaries from the official GitHub release.
#
# Usage:
#   ./publish_zstd_binaries.sh                 Build and upload to Artifactory
#   ./publish_zstd_binaries.sh --local-only    Build and install locally (no upload)
#
# Local development:
#   The --local-only flag builds all platform artifacts and installs them into
#   libs/native/libraries/build/platform/ so they can be used for tests and
#   benchmarks without publishing to Artifactory. Requires Docker with
#   multi-platform support (linux/amd64 + linux/arm64).
#
#   Example:
#     cd dev-tools
#     ./publish_zstd_binaries.sh --local-only
#     cd ..
#     LOCAL_ZSTD_BINARY=1 ./gradlew :libs:native:test
#     LOCAL_ZSTD_BINARY=1 ./gradlew -p benchmarks run --args "ZstdDecompressBenchmark"
#

set -e

VERSION="1.5.7"
BUILD_REVISION="1"
ARTIFACT_VERSION="${VERSION}-${BUILD_REVISION}"

LOCAL_ONLY=false
if [ "${1:-}" = "--local-only" ]; then
  LOCAL_ONLY=true
fi

for cmd in zip unzip curl jq; do
  if ! command -v $cmd &>/dev/null; then
    echo "Error: $cmd is not installed or not on PATH"
    exit 1;
  fi
done

if ! docker buildx inspect 2>/dev/null | grep -q 'linux/arm64'; then
  echo 'Error: No Docker support for linux/arm64 detected'
  echo 'For more information see https://docs.docker.com/build/building/multi-platform'
  exit 1;
fi

if [ "$LOCAL_ONLY" = false ] && [ -z "$ARTIFACTORY_API_KEY" ]; then
  echo 'Error: The ARTIFACTORY_API_KEY environment variable must be set.'
  exit 1;
fi

ARTIFACTORY_REPOSITORY="${ARTIFACTORY_REPOSITORY:-https://artifactory.elastic.dev/artifactory/elasticsearch-native/}"
TEMP=$(mktemp -d)

echo "Source version: $VERSION"
echo "Artifact version: $ARTIFACT_VERSION"
if [ "$LOCAL_ONLY" = true ]; then
  echo "Mode: local-only (build artifacts, skip upload)"
fi

fetch_homebrew_artifact() {
  DIGEST=$(curl -sS --retry 3 -H "Accept: application/vnd.oci.image.index.v1+json" -H "Authorization: Bearer QQ==" \
      --location "https://ghcr.io/v2/homebrew/core/zstd/manifests/$VERSION" | jq -r \
      ".manifests[] | select(.platform.os == \"darwin\" and .platform.architecture == \"$1\" and .platform.\"os.version\" == \"macOS 13.7\") | .annotations.\"sh.brew.bottle.digest\"")

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
  ARTIFACT="$TEMP/zstd-$ARTIFACT_VERSION-darwin-$2.jar"
  TAR_DIR="$TEMP/darwin-$2"
  mkdir $TAR_DIR
  tar zxf $1 --strip-components=2 -C $TAR_DIR "zstd/$VERSION/LICENSE" "zstd/$VERSION/lib/libzstd.$VERSION.dylib" && rm $1
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
  ARTIFACT="$TEMP/zstd-$ARTIFACT_VERSION-linux-$2.jar"
  OUTPUT_DIR="$TEMP/linux-$2"
  mkdir $OUTPUT_DIR
  DOCKER_IMAGE=$(docker build --build-arg="ZSTD_VERSION=${VERSION}" --file zstd.Dockerfile --platform $1 --quiet .)
  docker run --platform $1 $DOCKER_IMAGE > $OUTPUT_DIR/libzstd.so
  download_license $OUTPUT_DIR/LICENSE
  (cd $OUTPUT_DIR/../ && zip -rq - $(basename $OUTPUT_DIR)) > $ARTIFACT && rm -rf $OUTPUT_DIR
  echo $ARTIFACT
}

echo 'Building Linux jars...'
LINUX_ARM_JAR=$(build_linux_jar "linux/arm64" "aarch64")
LINUX_X86_JAR=$(build_linux_jar "linux/amd64" "x86-64")

build_windows_jar() {
  ARTIFACT="$TEMP/zstd-$ARTIFACT_VERSION-windows-x86-64.jar"
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
  curl -sS -X PUT -H "X-JFrog-Art-Api: ${ARTIFACTORY_API_KEY}" --data-binary "@$1" --location "${ARTIFACTORY_REPOSITORY}/org/elasticsearch/zstd/${ARTIFACT_VERSION}/$(basename $1)"
}

install_locally() {
  local platform_dir="$(cd "$(dirname "$0")" && pwd)/../libs/native/libraries/build/platform"

  echo ''
  echo 'Installing locally built libraries...'
  for jar in "$TEMP"/*.jar; do
    unzip -oq "$jar" -d "$platform_dir"
  done
  # Match the directory renames that extractLibs applies
  for src_suffix in "linux-x86-64:linux-x64" "darwin-x86-64:darwin-x64" "win32-x86-64:windows-x86-64"; do
    local src="${src_suffix%%:*}" dst="${src_suffix##*:}"
    if [ -d "$platform_dir/$src" ]; then
      mkdir -p "$platform_dir/$dst"
      cp -f "$platform_dir/$src"/* "$platform_dir/$dst/"
      rm -rf "$platform_dir/$src"
    fi
  done
  rm -rf "$TEMP"

  echo ''
  echo 'Installed to:'
  find "$platform_dir" -type f -name 'libzstd*' -o -name 'zstd*' | sort | while read f; do
    echo "  $f ($(du -h "$f" | cut -f1 | xargs))"
  done
  echo ''
  echo 'Run tests/benchmarks with: LOCAL_ZSTD_BINARY=1 ./gradlew ...'
}

if [ "$LOCAL_ONLY" = true ]; then
  install_locally
else
  echo 'Uploading artifacts...'
  upload_artifact ${DARWIN_ARM_JAR}
  upload_artifact ${DARWIN_X86_JAR}
  upload_artifact ${LINUX_ARM_JAR}
  upload_artifact ${LINUX_X86_JAR}
  upload_artifact ${WINDOWS_X86_JAR}
  rm -rf $TEMP
fi
