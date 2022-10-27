#!/bin/bash
set -e
ARTIFACT="${ARTIFACT:-$1}"
BRANCH="${BRANCH:-$2}"

curl -sS https://artifacts-snapshot.elastic.co/$ARTIFACT/latest/$BRANCH.json \
  | grep build_id \
  | sed "s/build_id/$ARTIFACT/g" \
  | sed 's/\"//g' \
  | sed 's/,//' \
  | sed 's/ : /=/' \
  | xargs

# https://artifacts-snapshot.elastic.co/beats/0.1.0-283a66b2/manifest-0.1.0-SNAPSHOT.json