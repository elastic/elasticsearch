#!/usr/bin/env bash
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".

set -euo pipefail

if [ ! -v ARTIFACTORY_API_KEY ]; then
  echo "Missing ARTIFACTORY_API_KEY env variable"
  exit 1
fi

# Move to the "native" directory
cd $(dirname $0)/..

# Use the native code hash as the "version"
VERSION=$(cd .. && ../../../gradlew -q printNativeCodeHash | grep '^native-code-hash:' | cut -d: -f2)

REPOSITORY="https://artifactory.elastic.dev/artifactory/elasticsearch-native"
LIB_NAME="libesql_parquet_rs"
URL="${REPOSITORY}/org/elasticsearch/${LIB_NAME}/${VERSION}/${LIB_NAME}-${VERSION}.zip"

if curl -sS -I --fail --location ${URL} > /dev/null 2>&1; then
  echo "Error: Artifacts already exist for version '${VERSION}'."
  exit 1;
fi

./build-tools/build-libraries.sh

TEMP=$(mktemp -d)
trap 'rm -rf -- "$TEMP"' EXIT

mkdir -p "$TEMP/aarch64-apple-darwin"
mkdir -p "$TEMP/aarch64-unknown-linux-gnu"
mkdir -p "$TEMP/x86_64-unknown-linux-gnu"
cp target/aarch64-apple-darwin/release/${LIB_NAME}.dylib   "$TEMP/aarch64-apple-darwin/"
cp target/aarch64-unknown-linux-gnu/release/${LIB_NAME}.so "$TEMP/aarch64-unknown-linux-gnu/"
cp target/x86_64-unknown-linux-gnu/release/${LIB_NAME}.so  "$TEMP/x86_64-unknown-linux-gnu/"

echo
echo 'Uploading to Artifactory...'
(cd "$TEMP" && zip -rq - .) | curl -sSf -X PUT -H "X-JFrog-Art-Api: ${ARTIFACTORY_API_KEY}" --data-binary @- --location "${URL}"
