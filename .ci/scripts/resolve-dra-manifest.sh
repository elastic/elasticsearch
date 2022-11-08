#!/bin/bash
set -e
ARTIFACT="${ARTIFACT:-$1}"
BRANCH="${BRANCH:-$2}"

curl -sS https://artifacts-snapshot.elastic.co/$ARTIFACT/latest/$BRANCH.json \
  | jq -r '.build_id'
