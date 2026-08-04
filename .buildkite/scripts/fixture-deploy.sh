#!/bin/bash

set -euo pipefail

echo "$DOCKER_REGISTRY_PASSWORD" | docker login -u "$DOCKER_REGISTRY_USERNAME" --password-stdin docker.elastic.co
unset DOCKER_REGISTRY_USERNAME DOCKER_REGISTRY_PASSWORD

# Register QEMU emulation against this agent's live kernel and create a
# multi-platform capable buildx builder before building. binfmt_misc
# registration does not survive the image bake -> boot cycle, so it must
# be redone here on every job run. See
# https://github.com/elastic/ci-agent-images/pull/2907 for details.
docker run --privileged --rm tonistiigi/binfmt:qemu-v9.2.2 --install all
docker buildx create --driver docker-container --use --bootstrap
.ci/scripts/run-gradle.sh deployFixtureDockerImages
