#!/bin/bash

set -euo pipefail

curl -s -L -O https://github.com/gradle/gradle-enterprise-build-validation-scripts/releases/download/v2.5.1/gradle-enterprise-gradle-build-validation-2.5.1.zip && unzip -q -o gradle-enterprise-gradle-build-validation-2.5.1.zip
cd gradle-enterprise-gradle-build-validation

./03-validate-local-build-caching-different-locations.sh -r https://github.com/elastic/elasticsearch.git -b $BUILDKITE_BRANCH --gradle-enterprise-server https://gradle-enterprise.elastic.co -t tasks