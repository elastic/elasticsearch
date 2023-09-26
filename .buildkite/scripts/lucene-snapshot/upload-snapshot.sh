#!/bin/bash

set -euo pipefail

LUCENE_BUILD_ID=${LUCENE_BUILD_ID:-}

if [[ -z "$LUCENE_BUILD_ID" ]]; then
  build_json=$(curl -sH "Authorization: Bearer $BUILDKITE_API_TOKEN" "https://api.buildkite.com/v2/organizations/elastic/pipelines/$BUILDKITE_PIPELINE_SLUG/builds/$BUILDKITE_BUILD_NUMBER")
  LUCENE_BUILD_ID=$(jq -r '.jobs[] | select(.step_key == "lucene-build").triggered_build.id' <<< "$build_json")
fi

export LUCENE_BUILD_ID

LUCENE_SHA=$(buildkite-agent meta-data get --build "$LUCENE_BUILD_ID" lucene-snapshot-sha)
export LUCENE_SHA

LUCENE_SNAPSHOT_VERSION=$(buildkite-agent meta-data get --build "$LUCENE_BUILD_ID" lucene-snapshot-version)
export LUCENE_SNAPSHOT_VERSION

echo --- Downloading lucene snapshot

mkdir lucene-snapshot
cd lucene-snapshot
buildkite-agent artifact download --build "$LUCENE_BUILD_ID" lucene-snapshot.tar.gz .
tar -xvf lucene-snapshot.tar.gz
cd -

echo --- Upload lucene snapshot to S3

if ! which aws; then
  curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
  unzip awscliv2.zip
  sudo ./aws/install
  rm -rf awscliv2.zip aws
fi

aws s3 sync lucene-snapshot/ "s3://download.elasticsearch.org/lucenesnapshots/$LUCENE_SHA/" --acl public-read

if [[ "${UPDATE_ES_LUCENE_SNAPSHOT:-}" ]]; then
  .buildkite/scripts/lucene-snapshot/update-es-snapshot.sh
fi
