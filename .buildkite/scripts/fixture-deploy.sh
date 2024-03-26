#!/bin/bash

set -euo pipefail

echo "$DOCKER_REGISTRY_PASSWORD" | docker login -u "$DOCKER_REGISTRY_USERNAME" --password-stdin docker.elastic.co
unset DOCKER_REGISTRY_USERNAME DOCKER_REGISTRY_PASSWORD

docker buildx create --use
.ci/scripts/run-gradle.sh deployFixtureDockerImages
