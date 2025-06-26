#!/bin/bash
set -e

strip_version() {
  echo $1 | sed -E "s/(.+)-[0-9a-f]+/\1/g"
}

fetch_build() {
  curl -sS https://artifacts-$1.elastic.co/$2/latest/$3.json \
    | jq -r '.build_id'
}

ARTIFACT="${ARTIFACT:-$1}"
ES_VERSION="${ES_VERSION:-$2}"
WORKFLOW=${WORKFLOW:-$3}

LATEST_BUILD=$(fetch_build $WORKFLOW $ARTIFACT $ES_VERSION)
LATEST_VERSION=$(strip_version $LATEST_BUILD)

if [ "$LATEST_VERSION" != "$ES_VERSION" ]; then
  echo "warning: Latest build for '$ARTIFACT' is version $LATEST_VERSION but expected version $ES_VERSION."
fi

echo $LATEST_BUILD
