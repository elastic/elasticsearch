#!/bin/bash

set -euo pipefail

echo "--- Updating cuvs-java version"

git checkout "$BUILDKITE_BRANCH"
git pull --ff-only origin "$BUILDKITE_BRANCH"

# Replace `cuvs_java = <version>` string in version.properties and maintain the same indentation
sed -E "s/^(cuvs_java *= *[^ ]*  *).*\$/\1$CUVS_JAVA_VERSION/" build-tools-internal/version.properties > new-version.properties
mv new-version.properties build-tools-internal/version.properties

python3 .buildkite/scripts/lucene-snapshot/remove-verification-metadata.py
./gradlew --write-verification-metadata sha256

if [[ "${SKIP_TESTING:-}" != "true" ]]; then
  echo "--- Testing cuvs-java before committing"
  ./gradlew -Druntime.java=24 :x-pack:plugin:gpu:yamlRestTest -S
fi

if git diff-index --quiet HEAD --; then
  echo 'No changes to commit.'
  exit 0
fi

echo "--- Committing changes"

git config --global user.name elasticsearchmachine
git config --global user.email 'infra-root+elasticsearchmachine@elastic.co'

git add build-tools-internal/version.properties
git add gradle/verification-metadata.xml

git commit -m "[Automated] Update cuvs-java to $CUVS_JAVA_VERSION"
git push origin "$BUILDKITE_BRANCH"
