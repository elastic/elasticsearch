#!/bin/bash
set -e
WORKSPACE="${WORKSPACE:-$PWD}"
ARTIFACT="${ARTIFACT:-$1}"
BRANCH='master'
RELEASE_VERSION="8.6.0-SNAPSHOT"
LOCAL_MANIFEST_FILE="$WORKSPACE/release-manifest-${ARTIFACT}.json"

curl -sS https://artifacts-snapshot.elastic.co/$ARTIFACT/latest/$BRANCH.json \
  | grep build_id \
  | sed "s/build_id/$ARTIFACT/g" \
  | sed 's/\"//g' \
  | sed 's/,//' \
  | sed 's/ : /=/' \
  | xargs
