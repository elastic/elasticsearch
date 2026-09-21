#!/bin/bash

set -euo pipefail

echo "$DOCKER_REGISTRY_PASSWORD" | docker login -u "$DOCKER_REGISTRY_USERNAME" --password-stdin docker.elastic.co
unset DOCKER_REGISTRY_USERNAME DOCKER_REGISTRY_PASSWORD

# Register QEMU emulation against this agent's live kernel and create a
# multi-platform capable buildx builder before building. binfmt_misc
# registration does not survive the image bake -> boot cycle, so it must
# be redone here on every job run. See
# https://github.com/elastic/ci-agent-images/pull/2907 for details.
#
# NOTE: qemu-v9.2.2 mishandles openat2(O_NOFOLLOW) on aarch64 (glibc tar's
# CVE-2025-45582 fix triggers this), causing "tar: ...: Cannot open: Invalid
# argument" during linux/arm64 cross builds. qemu-v10.2.3 has the upstream
# fix. See https://github.com/tonistiigi/binfmt/issues/285.
docker run --privileged --rm tonistiigi/binfmt:qemu-v10.2.3 --install all
docker buildx create --driver docker-container --use --bootstrap
.ci/scripts/run-gradle.sh deployFixtureDockerImages
