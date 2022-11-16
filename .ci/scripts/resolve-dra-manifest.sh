#!/bin/bash
set -e

strip_version() {
  echo $1 | sed -E "s/(.+)-[0-9a-f]+/\1/g"
}

fetch_build() {
  curl -sS https://artifacts-snapshot.elastic.co/$1/latest/$2.json \
    | jq -r '.build_id'
}

ARTIFACT="${ARTIFACT:-$1}"
BRANCH="${BRANCH:-$2}"
ES_VERSION="${ES_VERSION:-$3}"

LATEST_BUILD=$(fetch_build $ARTIFACT $BRANCH)
LATEST_VERSION=$(strip_version $LATEST_BUILD)

# If the latest artifact version doesn't match what we expect, try the corresponding version branch.
# This can happen when the version of artifact has been bumped on the master branch.
if [ "$LATEST_VERSION" != "$ES_VERSION" ]; then
  echo "Latest build for '$ARTIFACT' is version $LATEST_VERSION but expected version $ES_VERSION." 1>&2
  NEW_BRANCH=$(echo $ES_VERSION | sed -E "s/([0-9]+\.[0-9]+)\.[0-9]/\1/g")
  echo "Using branch $NEW_BRANCH instead of $BRANCH." 1>&2
  LATEST_BUILD=$(fetch_build $ARTIFACT $NEW_BRANCH)
fi

echo $LATEST_BUILD
