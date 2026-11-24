#!/bin/bash

set -euo pipefail

WORKFLOW="${DRA_WORKFLOW:-snapshot}"

# Resolved by dra-generate-steps.sh, which also passes them to the dra-prep
# plugin, so the build and the DRA manifest use the same dependency builds.
BEATS_BUILD_ID="${BEATS_BUILD_ID:?BEATS_BUILD_ID is required}"
ML_CPP_BUILD_ID="${ML_CPP_BUILD_ID:?ML_CPP_BUILD_ID is required}"

echo --- Preparing

# TODO move this to image
sudo NEEDRESTART_MODE=l apt-get update -y
sudo NEEDRESTART_MODE=l apt-get install -y libxml2-utils python3.10-venv

ES_VERSION=$(grep elasticsearch build-tools-internal/version.properties | sed "s/elasticsearch *= *//g")
echo "ES_VERSION=$ES_VERSION"

VERSION_SUFFIX=""
if [[ "$WORKFLOW" == "snapshot" ]]; then
  VERSION_SUFFIX="-SNAPSHOT"
fi

if [[ -n "${VERSION_QUALIFIER:-}" ]]; then
  ES_VERSION="${ES_VERSION}-${VERSION_QUALIFIER}"
  echo "Version qualifier specified. ES_VERSION=${ES_VERSION}."
fi

LICENSE_KEY_ARG=""
BUILD_SNAPSHOT_ARG=""
VERSION_QUALIFIER_ARG=""

if [[ "$WORKFLOW" == "staging" ]]; then
  LICENSE_KEY=$(mktemp -d)/license.key
  # Notice that only the public key is being read here, which isn't really secret
  vault read -field pubkey secret/ci/elastic-elasticsearch/migrated/license | base64 --decode > "$LICENSE_KEY"
  LICENSE_KEY_ARG="-Dlicense.key=$LICENSE_KEY"

  BUILD_SNAPSHOT_ARG="-Dbuild.snapshot=false"
fi

if [[ -n "${VERSION_QUALIFIER:-}" ]]; then
  VERSION_QUALIFIER_ARG="-Dbuild.version_qualifier=$VERSION_QUALIFIER"
fi

echo --- install qemu for aarch64 docker image builds
# NOTE: qemu-v9.2.2 mishandles openat2(O_NOFOLLOW) on aarch64 (glibc tar's
# CVE-2025-45582 fix triggers this), causing "tar: ...: Cannot open: Invalid
# argument" during linux/arm64 cross builds. qemu-v10.2.3 has the upstream
# fix. See https://github.com/tonistiigi/binfmt/issues/285.
docker run --privileged --rm tonistiigi/binfmt:qemu-v10.2.3 --install all
docker buildx create --driver docker-container --use --bootstrap

echo --- Building release artifacts

# Push to the remote build cache so expensive cacheable outputs (notably the QEMU-emulated
# :distribution:tools:server-launcher:nativeImageLinuxAarch64, ~28 min) are reused by later DRA builds.
.ci/scripts/run-gradle.sh -Ddra.artifacts=true \
  -Dorg.elasticsearch.build.cache.push=true \
  -Ddra.artifacts.dependency.beats="${BEATS_BUILD_ID}" \
  -Ddra.artifacts.dependency.ml-cpp="${ML_CPP_BUILD_ID}" \
  -Ddra.workflow="$WORKFLOW" \
  -Dcsv="$WORKSPACE/build/distributions/dependencies-${ES_VERSION}${VERSION_SUFFIX}.csv" \
  $LICENSE_KEY_ARG \
  $BUILD_SNAPSHOT_ARG \
  $VERSION_QUALIFIER_ARG \
  buildReleaseArtifacts \
  exportCompressedDockerImages \
  exportDockerContexts \
  :nmcpZipAggregation \
  :prepareDraSnapshotMavenAggregation \
  :distribution:generateDependenciesReport

PATH="$PATH:${JAVA_HOME}/bin" # Required by the following script
if [[ -z "${VERSION_QUALIFIER:-}" ]]; then
x-pack/plugin/sql/connectors/tableau/package.sh asm qualifier="$VERSION_SUFFIX"
else
x-pack/plugin/sql/connectors/tableau/package.sh asm qualifier="-$VERSION_QUALIFIER"
fi

# remove the stale checksum — dractl generates a fresh one during artifact classification
rm "build/distributions/elasticsearch-jdbc-${ES_VERSION}${VERSION_SUFFIX}.taco.sha512"

# Allow other users access to read the artifacts so they are readable in the
# container
find "$WORKSPACE" -type f -path "*/build/distributions/*" -exec chmod a+r {} \;

# Allow other users write access to create checksum files
find "$WORKSPACE" -type d -path "*/build/distributions" -exec chmod a+w {} \;

# Publish the exploded maven aggregation tree to snapshots.elastic.co /
# artifacts.elastic.co. The maven tree is published directly from the workspace;
# GCS publication of all other artifacts is handled by the dra-prep plugin, which
# runs in this step's post-command hook on the artifacts/ directory staged below.
echo --- Publishing maven aggregation to S3
DRA_WORKFLOW="$WORKFLOW" \
  .buildkite/scripts/dra-maven-publish.sh

echo --- Consolidating distribution artifacts for DRA staging
mkdir -p artifacts

# Each cp block below mirrors a non-maven entry in the release-manager project
# config at elastic/infra/cd/release/release-manager/project-configs/master/elasticsearch.gradle.
# Keep this in sync with that file when artifact types or paths change.
# The Maven Central compliant aggregation zip (elasticsearch-maven-aggregation) is included below
# so it stays part of the DRA artifacts. The DRA-shaped maven/javadoc trees are published to S3
# separately via dra-maven-publish.sh.

# Binary distributions
cp distribution/archives/windows-zip/build/distributions/*.zip                artifacts/
cp distribution/archives/linux-tar/build/distributions/*.tar.gz               artifacts/
cp distribution/archives/linux-aarch64-tar/build/distributions/*.tar.gz       artifacts/
cp distribution/archives/darwin-aarch64-tar/build/distributions/*.tar.gz      artifacts/
cp distribution/packages/deb/build/distributions/*.deb                        artifacts/
cp distribution/packages/aarch64-deb/build/distributions/*.deb                artifacts/
cp distribution/packages/rpm/build/distributions/*.rpm                        artifacts/
cp distribution/packages/aarch64-rpm/build/distributions/*.rpm                artifacts/

# Generic ZIPs and TACO
cp build/distributions/elasticsearch-maven-aggregation-*.zip                   artifacts/
cp x-pack/rest-resources-zip/build/distributions/*.zip                        artifacts/
cp build/distributions/rolling-upgrade-compatible-*.zip                        artifacts/
cp build/distributions/elasticsearch-jdbc-*.taco                               artifacts/

# Official plugins (plugins/ and x-pack/extras/plugins/)
for plugin in \
  analysis-icu analysis-kuromoji analysis-nori analysis-phonetic analysis-smartcn \
  analysis-stempel analysis-ukrainian discovery-azure-classic discovery-ec2 \
  discovery-gce mapper-annotated-text mapper-murmur3 mapper-size repository-hdfs store-smb
do
  cp "plugins/$plugin/build/distributions/"*.zip                               artifacts/
done
cp x-pack/extras/plugins/microsoft-graph-authz/build/distributions/*.zip      artifacts/

# Docker images and build contexts
cp distribution/docker/build/distributions/*.tar.gz                            artifacts/

# CSV dependency report
cp "build/distributions/dependencies-${ES_VERSION}${VERSION_SUFFIX}.csv"      artifacts/

echo "Artifacts to be staged:"
ls -1 artifacts/
