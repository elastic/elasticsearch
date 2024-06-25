#!/bin/bash

set -euo pipefail

VALIDATION_SCRIPTS_VERSION=2.5.1
GRADLE_ENTERPRISE_ACCESS_KEY=$(vault kv get -field=value secret/ci/elastic-elasticsearch/gradle-enterprise-api-key)
export GRADLE_ENTERPRISE_ACCESS_KEY

curl -s -L -O https://github.com/gradle/gradle-enterprise-build-validation-scripts/releases/download/v$VALIDATION_SCRIPTS_VERSION/gradle-enterprise-gradle-build-validation-$VALIDATION_SCRIPTS_VERSION.zip && unzip -q -o gradle-enterprise-gradle-build-validation-$VALIDATION_SCRIPTS_VERSION.zip
cd gradle-enterprise-gradle-build-validation

./03-validate-local-build-caching-different-locations.sh -r https://github.com/elastic/elasticsearch.git -b $BUILDKITE_BRANCH --gradle-enterprise-server https://gradle-enterprise.elastic.co -t licenseHeaders -t precommit --fail-if-not-fully-cacheable

# Capture the return value
retval=$?

# Check if the command was successful
if [ $retval -eq 0 ]; then
    echo "Experiment completed successfully"
elif [ $retval -eq 1 ]; then
    echo "An invalid input was provided while attempting to run the experiment"
elif [ $retval -eq 2 ]; then
    echo "One of the builds that is part of the experiment failed"
elif [ $retval -eq 3 ]; then
    echo "The build was not fully cacheable for the given task graph"
elif [ $retval -eq 3 ]; then
    echo "An unclassified, fatal error happened while running the experiment"
fi

exit $retval

