#!/bin/bash

# Publishes the maven aggregation zip produced by :zipAggregation to the
# per-build DRA S3 layout on snapshots.elastic.co (snapshot workflow) or
# artifacts.elastic.co (staging workflow).
#
# For each javadoc jar in the aggregation zip we also unpack the browsable
# HTML tree under `<buildId>/javadoc/<groupPath>/<artifact>/<version>/`,
# mirroring the layout unified-release / release-manager writes today via
# `uploadSnapshotUnzippedJavadoc`. See
# https://github.com/elastic/platform-engineering-productivity/issues/2790#issuecomment-4781360993
# for the analysis this layout is based on.
#
# This script is intended to run inline in the DRA workflow job so it can pick
# the aggregation zip straight out of the workspace `build/distributions`.
# The `MAVEN_AGGREGATION_ZIP` env var overrides the default zip location so a
# future stand-alone publish step can point at a downloaded buildkite artifact
# instead.
#
# Required environment:
#   DRA_WORKFLOW           snapshot|staging
#   ES_VERSION             version incl. optional -<qualifier>, e.g. 9.3.0-alpha1
#   VERSION_SUFFIX         "-SNAPSHOT" for snapshots, empty for staging
#   BUILDKITE_COMMIT       full commit sha (used to derive the DRA build id)
#   AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY [/ AWS_SESSION_TOKEN]
#                          exported via USE_MAVEN_S3_CREDENTIALS in pre-command

set -euo pipefail

# Default matches dra-workflow.sh's `WORKFLOW="${DRA_WORKFLOW:-snapshot}"` so
# this script is safe to run standalone.
DRA_WORKFLOW="${DRA_WORKFLOW:-snapshot}"
: "${ES_VERSION:?ES_VERSION must be set}"
: "${BUILDKITE_COMMIT:?BUILDKITE_COMMIT must be set}"
VERSION_SUFFIX="${VERSION_SUFFIX-}"

case "$DRA_WORKFLOW" in
  snapshot) BUCKET="snapshots.elastic.co" ;;
  staging)  BUCKET="artifacts.elastic.co" ;;
  *) echo "unsupported DRA_WORKFLOW='$DRA_WORKFLOW'" >&2; exit 2 ;;
esac

# DRA build id convention across all pipelines (beats, ml-cpp, elasticsearch):
# `<version>-<commit-short>` — no `-SNAPSHOT` suffix in the build-id itself,
# even for snapshot workflows. E.g.
# artifacts-snapshot.elastic.co/elasticsearch/9.6.0-55eee08c/maven/...
# The `-SNAPSHOT` (or version qualifier) still lives inside the maven tree at
# the artifact version directory (`.../9.6.0-SNAPSHOT/foo-9.6.0-SNAPSHOT.jar`).
# Overridable so we can align with whatever RM reports if it ever diverges.
COMMIT_SHORT="${BUILDKITE_COMMIT:0:8}"
BUILD_ID="${DRA_BUILD_ID:-${ES_VERSION}-${COMMIT_SHORT}}"

ZIP="${MAVEN_AGGREGATION_ZIP:-build/distributions/elasticsearch-dra-maven-aggregation-${ES_VERSION}${VERSION_SUFFIX}.zip}"
if [[ ! -f "$ZIP" ]]; then
  echo "DRA aggregation zip not found: $ZIP" >&2
  echo "  (produced by :zipDraSnapshotMavenAggregation; must not be confused with" >&2
  echo "   :zipAggregation output at elasticsearch-maven-aggregation-*.zip which is" >&2
  echo "   Maven Central compliant and unsuitable for the DRA snapshot layout)" >&2
  exit 1
fi

WORK_DIR="$(mktemp -d -t es-maven-publish.XXXXXX)"
trap 'rm -rf "$WORK_DIR"' EXIT

MAVEN_DIR="$WORK_DIR/maven"
JAVADOC_DIR="$WORK_DIR/javadoc"
mkdir -p "$MAVEN_DIR" "$JAVADOC_DIR"

echo "--- Unpacking $ZIP"
unzip -q "$ZIP" -d "$MAVEN_DIR"

echo "--- Expanding javadoc jars"
# Layout of maven tree is standard: <group-with-slashes>/<artifact>/<version>/<artifact>-<version>[-classifier].jar
# Walk *-javadoc.jar entries and mirror them under javadoc/<group>/<artifact>/<version>/.
find "$MAVEN_DIR" -type f -name '*-javadoc.jar' -print0 | while IFS= read -r -d '' jar; do
  rel="${jar#"$MAVEN_DIR/"}"
  # rel = <groupPath>/<artifact>/<version>/<artifact>-<version>-javadoc.jar
  dir="$(dirname "$rel")"                    # <groupPath>/<artifact>/<version>
  target="$JAVADOC_DIR/$dir"
  mkdir -p "$target"
  unzip -q -o "$jar" -d "$target"
done

echo "--- Publishing to s3://$BUCKET/$BUILD_ID/{maven,javadoc}/"
# Use `cp --recursive` rather than `sync`: sync needs s3:ListBucket to diff the
# remote against the local tree, which the `unified-release-maven` role does
# not grant (only object-level Put/Get on `maven/*` and `javadoc/*`). Each
# <buildId>/ path is fresh per DRA build, so re-uploading unconditionally is
# equivalent to sync's outcome here.
aws s3 cp --recursive --no-progress --only-show-errors \
  "$MAVEN_DIR/"   "s3://$BUCKET/$BUILD_ID/maven/"
aws s3 cp --recursive --no-progress --only-show-errors \
  "$JAVADOC_DIR/" "s3://$BUCKET/$BUILD_ID/javadoc/"

echo "Published build $BUILD_ID:"
echo "  https://$BUCKET/$BUILD_ID/maven/"
echo "  https://$BUCKET/$BUILD_ID/javadoc/"
