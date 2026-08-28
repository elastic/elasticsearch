#!/usr/bin/env bash
#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".
#

# Builds libes_simdjson for all platforms and uploads the artifact to Artifactory.
#
# Usage:
#   ./publish_simdjson_binaries.sh                       # build all platforms and upload to Artifactory
#   ./publish_simdjson_binaries.sh --local               # build all platforms, package zip, skip upload
#   ./publish_simdjson_binaries.sh --local --force-upload # build locally, then upload to Artifactory
#
# Environment:
#   TOOLCHAIN_IMAGE      Docker image for cross-compilation
#                        (default: es-simdjson-cross-toolchain:local, built on demand;
#                         or docker.elastic.co/elasticsearch-infra/es-simdjson-cross-toolchain:1)
#   ARTIFACTORY_API_KEY  Required for upload (non --local, or --force-upload)

set -euo pipefail

VERSION="0.1.0"
LOCAL_TOOLCHAIN_IMAGE="es-simdjson-cross-toolchain:local"
REMOTE_TOOLCHAIN_IMAGE="docker.elastic.co/elasticsearch-infra/es-simdjson-cross-toolchain:1"
DEFAULT_TOOLCHAIN_IMAGE="${LOCAL_TOOLCHAIN_IMAGE}"

LOCAL=false
FORCE_UPLOAD=false
for arg in "$@"; do
  case "$arg" in
    --local)                      LOCAL=true ;;
    --force-upload)               FORCE_UPLOAD=true ;;
    *) echo "Unknown option: $arg"; exit 1 ;;
  esac
done

UPLOAD=false
if [ "$LOCAL" = false ] || [ "$FORCE_UPLOAD" = true ]; then
  UPLOAD=true
fi

if ! command -v zip > /dev/null; then
  echo 'Error: zip must be installed.'
  exit 1;
fi

if ! command -v docker > /dev/null; then
  echo 'Error: docker must be installed.'
  exit 1;
fi

if [ "$UPLOAD" = true ] && [ -z "${ARTIFACTORY_API_KEY:-}" ]; then
  echo 'Error: The ARTIFACTORY_API_KEY environment variable must be set.'
  exit 1;
fi

TOOLCHAIN_IMAGE="${TOOLCHAIN_IMAGE:-$DEFAULT_TOOLCHAIN_IMAGE}"

ensure_toolchain_image() {
  if docker image inspect "$TOOLCHAIN_IMAGE" > /dev/null 2>&1; then
    return
  fi
  if [ "$TOOLCHAIN_IMAGE" = "$LOCAL_TOOLCHAIN_IMAGE" ]; then
    echo "Building local simdjson toolchain image ${LOCAL_TOOLCHAIN_IMAGE} ..."
    "$(dirname "$0")/build_cross_toolchain_image.sh" --local
    return
  fi
  echo "Toolchain image not found locally; pulling ${TOOLCHAIN_IMAGE} ..."
  docker pull "$TOOLCHAIN_IMAGE"
}

# Older published toolchain images may lack curl/xz, which the Darwin target needs to
# fetch the macOS SDK via xmac. Install them on demand inside the container.
run_make_all_in_toolchain() {
  ensure_toolchain_image
  docker run --rm \
    -v "$(pwd)":/workspace \
    -w /workspace \
    "$TOOLCHAIN_IMAGE" \
    bash -lc '
      set -euo pipefail
      if ! command -v curl >/dev/null 2>&1 \
        || ! command -v xz >/dev/null 2>&1 \
        || ! command -v bzip2 >/dev/null 2>&1; then
        if ! command -v apt-get >/dev/null 2>&1; then
          echo "Error: curl, xz, and bzip2 are required to fetch the macOS SDK but are missing from the toolchain image."
          exit 1
        fi
        echo "Installing macOS SDK fetch dependencies (curl, xz-utils, bzip2) ..."
        export DEBIAN_FRONTEND=noninteractive
        apt-get update
        apt-get install -y --no-install-recommends curl xz-utils bzip2 ca-certificates
        rm -rf /var/lib/apt/lists/*
      fi
      make all verify-linux-abi
    '
}

ARTIFACTORY_REPOSITORY="${ARTIFACTORY_REPOSITORY:-https://artifactory.elastic.dev/artifactory/elasticsearch-native/}"
TEMP=$(mktemp -d)

if [ "$UPLOAD" = true ]; then
  if curl -sS -I --fail --location "${ARTIFACTORY_REPOSITORY}/org/elasticsearch/es-simdjson/${VERSION}/es-simdjson-${VERSION}.zip" > /dev/null 2>&1; then
    echo "Error: Artifacts already exist for version '${VERSION}'. Bump version before republishing."
    exit 1;
  fi
fi

echo 'Building all binaries (darwin-aarch64 + linux-aarch64 + linux-x64)...'
run_make_all_in_toolchain

mkdir -p "$TEMP/darwin-aarch64"
mkdir -p "$TEMP/linux-aarch64"
mkdir -p "$TEMP/linux-x64"
cp build/libs/es_simdjson/shared/aarch64/libes_simdjson.dylib "$TEMP/darwin-aarch64/"
cp build/libs/es_simdjson/shared/aarch64/libes_simdjson.so    "$TEMP/linux-aarch64/"
cp build/libs/es_simdjson/shared/amd64/libes_simdjson.so      "$TEMP/linux-x64/"

TEMP_DBG=$(mktemp -d)
mkdir -p "$TEMP_DBG/darwin-aarch64"
mkdir -p "$TEMP_DBG/linux-aarch64"
mkdir -p "$TEMP_DBG/linux-x64"
cp -r build/libs/es_simdjson/shared/aarch64/libes_simdjson.dylib.dSYM  "$TEMP_DBG/darwin-aarch64/"
cp    build/libs/es_simdjson/shared/aarch64/libes_simdjson.so.debug   "$TEMP_DBG/linux-aarch64/"
cp    build/libs/es_simdjson/shared/amd64/libes_simdjson.so.debug     "$TEMP_DBG/linux-x64/"

if [ "$UPLOAD" = true ]; then
  echo 'Uploading to Artifactory...'
  (cd "$TEMP" && zip -rq - .) | curl -sSf -X PUT -H "X-JFrog-Art-Api: ${ARTIFACTORY_API_KEY}" --data-binary @- --location "${ARTIFACTORY_REPOSITORY}/org/elasticsearch/es-simdjson/${VERSION}/es-simdjson-${VERSION}.zip"
  (cd "$TEMP_DBG" && zip -rq - .) | curl -sSf -X PUT -H "X-JFrog-Art-Api: ${ARTIFACTORY_API_KEY}" --data-binary @- --location "${ARTIFACTORY_REPOSITORY}/org/elasticsearch/es-simdjson/${VERSION}/es-simdjson-${VERSION}-debuginfo.zip"
  rm -rf "$TEMP" "$TEMP_DBG"
else
  ZIP="$(pwd)/es-simdjson-${VERSION}-local.zip"
  DBG_ZIP="$(pwd)/es-simdjson-${VERSION}-debuginfo-local.zip"
  (cd "$TEMP" && zip -rq "$ZIP" .)
  (cd "$TEMP_DBG" && zip -rq "$DBG_ZIP" .)
  rm -rf "$TEMP" "$TEMP_DBG"
  echo "Local build complete. Artifact: $ZIP"
  echo "Debug info:  $DBG_ZIP"
  echo "For local Gradle builds, either:"
  echo "  cd libs/simdjson/native && make install   # current platform only"
  echo "  or set LOCAL_SIMDJSON_BINARY=1 and copy from the zip into libs/native/libraries/build/platform/<os>-<arch>/"
fi
