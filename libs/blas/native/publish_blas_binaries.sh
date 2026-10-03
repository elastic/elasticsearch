#!/usr/bin/env bash
#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".
#

# Builds libopenblas for all platforms and uploads the artifact to Artifactory.
#
# Usage:
#   ./publish_blas_binaries.sh                       # build all platforms and upload to Artifactory
#   ./publish_blas_binaries.sh --local               # build all platforms, package zip, skip upload
#   ./publish_blas_binaries.sh --local --force-upload # build locally, then upload to Artifactory
#
# Environment:
#   TOOLCHAIN_IMAGE      Docker image for cross-compilation
#                        (default: es-native-cross-toolchain:local with --local, built on demand;
#                         or docker.elastic.co/elasticsearch-infra/es-native-cross-toolchain:7)
#   ARTIFACTORY_API_KEY  Required for upload (non --local, or --force-upload)

set -euo pipefail

VERSION="0.3.34-1"
ARTIFACT_ID="openblas"
VEC_NATIVE_DIR="$(cd "$(dirname "$0")/../../simdvec/native" && pwd)"
LOCAL_TOOLCHAIN_IMAGE="es-native-cross-toolchain:local"
REMOTE_TOOLCHAIN_IMAGE="docker.elastic.co/elasticsearch-infra/es-native-cross-toolchain:7"

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

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DIST_DIR="${SCRIPT_DIR}/dist/${VERSION}"
rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"

ensure_toolchain_image

echo "Building libopenblas with image: ${TOOLCHAIN_IMAGE}"

docker run --rm \
  -v "${SCRIPT_DIR}:/blas" \
  -w /blas \
  "$TOOLCHAIN_IMAGE" \
  make fetch all

echo "Packaging artifacts ..."

for platform in linux-aarch64 linux-x64 windows-x64; do
  case "$platform" in
    linux-aarch64)  lib_dir=aarch64 lib_name=libopenblas.so ;;
    linux-x64)      lib_dir=amd64   lib_name=libopenblas.so ;;
    windows-x64)    lib_dir=windows-x64 lib_name=openblas.dll ;;
  esac
  src="${SCRIPT_DIR}/build/libs/openblas/shared/${lib_dir}/${lib_name}"
  if [ ! -f "$src" ]; then
    echo "Missing: $src" >&2
    exit 1
  fi
  mkdir -p "${DIST_DIR}/${platform}"
  cp "$src" "${DIST_DIR}/${platform}/${lib_name}"
done

ZIP_NAME="${ARTIFACT_ID}-${VERSION}.zip"
ZIP_PATH="${DIST_DIR}/${ZIP_NAME}"
(cd "$DIST_DIR" && zip -r "$ZIP_NAME" linux-aarch64 linux-x64 windows-x64)
echo "Created: ${ZIP_PATH}"

if [ "$UPLOAD" = false ]; then
  echo "Skipping upload (--local)."
  exit 0
fi

ARTIFACTORY_URL="https://artifactory.elastic.dev/artifactory/elasticsearch-native/org/elasticsearch/${ARTIFACT_ID}/${VERSION}/${ZIP_NAME}"
echo "Uploading to: ${ARTIFACTORY_URL}"

# Fail if the version already exists — re-publishing over an existing release breaks reproducibility.
HTTP_STATUS=$(curl -s -o /dev/null -w '%{http_code}' -H "X-JFrog-Art-Api: ${ARTIFACTORY_API_KEY}" "${ARTIFACTORY_URL}")
if [ "$HTTP_STATUS" = "200" ]; then
  echo "Error: artifact ${VERSION} already exists in Artifactory. Bump VERSION before re-publishing."
  exit 1
fi

curl -f -H "X-JFrog-Art-Api: ${ARTIFACTORY_API_KEY}" \
  -T "$ZIP_PATH" \
  "$ARTIFACTORY_URL"

echo "Upload complete."
