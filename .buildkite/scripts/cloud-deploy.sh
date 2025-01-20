#!/bin/bash

set -euo pipefail

.ci/scripts/run-gradle.sh buildCloudEssDockerImage

ES_VERSION=$(grep 'elasticsearch' build-tools-internal/version.properties | awk '{print $3}')
DOCKER_TAG="docker.elastic.co/elasticsearch-ci/elasticsearch-cloud-ess:${ES_VERSION}-${BUILDKITE_COMMIT:0:7}"
docker tag elasticsearch-cloud-ess:test "$DOCKER_TAG"

echo "$DOCKER_REGISTRY_PASSWORD" | docker login -u "$DOCKER_REGISTRY_USERNAME" --password-stdin docker.elastic.co
unset DOCKER_REGISTRY_USERNAME DOCKER_REGISTRY_PASSWORD

docker push "$DOCKER_TAG"
