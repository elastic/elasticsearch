#!/usr/bin/env bash
#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".
#

# Builds libsimdjson for all platforms and uploads the artifact to Artifactory.
#
# Usage:
#   ./publish_simdjson_binaries.sh                       # build all platforms and upload to Artifactory
#   ./publish_simdjson_binaries.sh --local               # build all platforms, package zip, skip upload
#   ./publish_simdjson_binaries.sh --local --force-upload # build locally, then upload to Artifactory
#
# Environment:
#   TOOLCHAIN_IMAGE      Docker image for cross-compilation
#                        (default: es-native-cross-toolchain:local with --local, built on demand;
#                         or docker.elastic.co/elasticsearch-infra/es-native-cross-toolchain:6)
#   ARTIFACTORY_API_KEY  Required for upload (non --local, or --force-upload)

set -euo pipefail

VERSION="0.2.0"
ARTIFACT_ID="libsimdjson"
VEC_NATIVE_DIR="$(cd "$(dirname "$0")/../../simdvec/native" && pwd)"
LOCAL_TOOLCHAIN_IMAGE="es-native-cross-toolchain:local"
REMOTE_TOOLCHAIN_IMAGE="docker.elastic.co/elasticsearch-infra/es-native-cross-toolchain:6"

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

if [ "$LOCAL" = true ]; then
  TOOLCHAIN_IMAGE="${TOOLCHAIN_IMAGE:-$LOCAL_TOOLCHAIN_IMAGE}"
else
  TOOLCHAIN_IMAGE="${TOOLCHAIN_IMAGE:-$REMOTE_TOOLCHAIN_IMAGE}"
fi

ensure_toolchain_image() {
  if docker image inspect "$TOOLCHAIN_IMAGE" > /dev/null 2>&1; then
    return
  fi
  if [ "$TOOLCHAIN_IMAGE" = "$LOCAL_TOOLCHAIN_IMAGE" ]; then
    echo "Building local native toolchain image ${LOCAL_TOOLCHAIN_IMAGE} ..."
    "${VEC_NATIVE_DIR}/build_cross_toolchain_image.sh" --local
    return
  fi
  echo "Toolchain image not found locally; pulling ${TOOLCHAIN_IMAGE} ..."
  docker pull "$TOOLCHAIN_IMAGE"
}

run_make_all_in_toolchain() {
  ensure_toolchain_image
  docker run --rm \
    -v "$(pwd)":/workspace \
    -w /workspace \
    "$TOOLCHAIN_IMAGE" \
    make all verify-linux-abi
}

ARTIFACTORY_REPOSITORY="${ARTIFACTORY_REPOSITORY:-https://artifactory.elastic.dev/artifactory/elasticsearch-native/}"
TEMP=$(mktemp -d)

if [ "$UPLOAD" = true ]; then
  if curl -sS -I --fail --location "${ARTIFACTORY_REPOSITORY}/org/elasticsearch/${ARTIFACT_ID}/${VERSION}/${ARTIFACT_ID}-${VERSION}.zip" > /dev/null 2>&1; then
    echo "Error: Artifacts already exist for version '${VERSION}'. Bump version before republishing."
    exit 1;
  fi
fi

echo 'Building all binaries (darwin-aarch64 + linux-aarch64 + linux-x64)...'
run_make_all_in_toolchain

mkdir -p "$TEMP/darwin-aarch64"
mkdir -p "$TEMP/linux-aarch64"
mkdir -p "$TEMP/linux-x64"
cp build/libs/simdjson/shared/aarch64/libsimdjson.dylib "$TEMP/darwin-aarch64/"
cp build/libs/simdjson/shared/aarch64/libsimdjson.so    "$TEMP/linux-aarch64/"
cp build/libs/simdjson/shared/amd64/libsimdjson.so      "$TEMP/linux-x64/"

TEMP_DBG=$(mktemp -d)
mkdir -p "$TEMP_DBG/darwin-aarch64"
mkdir -p "$TEMP_DBG/linux-aarch64"
mkdir -p "$TEMP_DBG/linux-x64"
cp -r build/libs/simdjson/shared/aarch64/libsimdjson.dylib.dSYM  "$TEMP_DBG/darwin-aarch64/"
cp    build/libs/simdjson/shared/aarch64/libsimdjson.so.debug   "$TEMP_DBG/linux-aarch64/"
cp    build/libs/simdjson/shared/amd64/libsimdjson.so.debug     "$TEMP_DBG/linux-x64/"

if [ "$UPLOAD" = true ]; then
  echo 'Uploading to Artifactory...'
  (cd "$TEMP" && zip -rq - .) | curl -sSf -X PUT -H "X-JFrog-Art-Api: ${ARTIFACTORY_API_KEY}" --data-binary @- --location "${ARTIFACTORY_REPOSITORY}/org/elasticsearch/${ARTIFACT_ID}/${VERSION}/${ARTIFACT_ID}-${VERSION}.zip"
  (cd "$TEMP_DBG" && zip -rq - .) | curl -sSf -X PUT -H "X-JFrog-Art-Api: ${ARTIFACTORY_API_KEY}" --data-binary @- --location "${ARTIFACTORY_REPOSITORY}/org/elasticsearch/${ARTIFACT_ID}/${VERSION}/${ARTIFACT_ID}-${VERSION}-debuginfo.zip"
  rm -rf "$TEMP" "$TEMP_DBG"
else
  ZIP="$(pwd)/${ARTIFACT_ID}-${VERSION}-local.zip"
  DBG_ZIP="$(pwd)/${ARTIFACT_ID}-${VERSION}-debuginfo-local.zip"
  (cd "$TEMP" && zip -rq "$ZIP" .)
  (cd "$TEMP_DBG" && zip -rq "$DBG_ZIP" .)
  rm -rf "$TEMP" "$TEMP_DBG"
  echo "Local build complete. Artifact: $ZIP"
  echo "Debug info:  $DBG_ZIP"
  echo "For local Gradle builds, either:"
  echo "  SIMDJSON_NATIVE_BUILD=host ./gradlew :libs:simdjson:test"
  echo "  or SIMDJSON_NATIVE_BUILD=docker ./gradlew :libs:simdjson:buildNativeLibrary"
fi
