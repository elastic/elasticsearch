#!/bin/bash

set -euo pipefail

VALIDATION_SCRIPTS_VERSION=2.5.1
GRADLE_ENTERPRISE_ACCESS_KEY=$(vault kv get -field=value secret/ci/elastic-elasticsearch/gradle-enterprise-api-key)
export GRADLE_ENTERPRISE_ACCESS_KEY

curl -s -L -O https://github.com/gradle/gradle-enterprise-build-validation-scripts/releases/download/v$VALIDATION_SCRIPTS_VERSION/gradle-enterprise-gradle-build-validation-$VALIDATION_SCRIPTS_VERSION.zip && unzip -q -o gradle-enterprise-gradle-build-validation-$VALIDATION_SCRIPTS_VERSION.zip
cd gradle-enterprise-gradle-build-validation

./03-validate-local-build-caching-different-locations.sh -r https://github.com/elastic/elasticsearch.git -b $BUILDKITE_BRANCH --gradle-enterprise-server https://gradle-enterprise.elastic.co -t licenseHeaders -t precommit