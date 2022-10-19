#!/bin/bash
#!/bin/bash

set -e
WORKSPACE="${WORKSPACE:-$PWD}"
ARTIFACT="${ARTIFACT:-$1}"
BRANCH='master'
RELEASE_VERSION="8.6.0-SNAPSHOT"

echo $WORKSPACE
LOCAL_MANIFEST_FILE="$WORKSPACE/release-manifest-${ARTIFACT}.json"
wget https://artifacts-snapshot.elastic.co/$ARTIFACT/latest/$BRANCH.json -O $LOCAL_MANIFEST_FILE

cat $LOCAL_MANIFEST_FILE | grep build_id | sed "s/build_id/$ARTIFACT/g" | sed 's/\"//g' | sed 's/,//'| sed 's/ : /=/' | xargs >> deps.properties
