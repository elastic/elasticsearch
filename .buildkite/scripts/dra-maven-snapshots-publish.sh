#!/bin/bash

# Publishes the exploded maven tree produced by :prepareDraSnapshotMavenAggregation
# straight into the consumer-facing root prefixes on snapshots.elastic.co
# (snapshot workflow) or artifacts.elastic.co (staging workflow):
#
#   s3://<bucket>/maven/<groupPath>/<artifact>/<version>/<file>
#   s3://<bucket>/javadoc/<groupPath>/<artifact>/<version>/<html-tree>
#
# This matches the endpoints Gradle/Maven consumers already resolve against
# (see docs/java-rest/*, docs/Versions.asciidoc) and the scope of the
# `unified-release-maven` AWS role provisioned in
# https://github.com/elastic/platform-engineering-productivity/issues/2840.
# There is deliberately no per-buildId staging prefix: snapshots are ephemeral
# and last-writer-wins matches release-manager's current behavior; staging
# writes land under version-specific artifact directories so concurrent
# releases do not collide.
#
# For each `*-javadoc.jar` in the maven tree we also unpack the browsable HTML
# tree under `javadoc/<groupPath>/<artifact>/<version>/`, mirroring the layout
# unified-release / release-manager writes today via
# `uploadSnapshotUnzippedJavadoc`. See
# https://github.com/elastic/platform-engineering-productivity/issues/2790#issuecomment-4781360993
# for the analysis this layout is based on.
#
# Intended to run inline in the DRA workflow job so it can pick the exploded
# maven tree straight out of the workspace `build/dra-maven-aggregation`. The
# Gradle task emits an exploded directory (not a zip) precisely so this step can
# upload it without a zip/unzip round-trip. The `MAVEN_AGGREGATION_DIR` env var
# overrides the source location so a future stand-alone publish step can point at
# a downloaded buildkite artifact instead.
#
# The version is already encoded in the exploded maven tree's directory layout
# and the S3 target is the root `maven/` prefix, so no version env var is needed.
#
# Required environment:
#   DRA_WORKFLOW           snapshot|staging (default: snapshot)
#   AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY [/ AWS_SESSION_TOKEN]
#                          exported via USE_MAVEN_S3_CREDENTIALS in pre-command

set -euo pipefail

retry_with_backoff() {
  local description="$1"
  shift

  local max_attempts=4
  local delays=(5 15 30)
  local attempt=1

  while true; do
    if "$@"; then
      return 0
    fi

    local exit_code=$?
    if (( attempt >= max_attempts )); then
      echo "$description failed after $attempt attempts" >&2
      return "$exit_code"
    fi

    local delay="${delays[$((attempt - 1))]}"
    echo "$description failed with exit code $exit_code (attempt $attempt/$max_attempts); retrying in ${delay}s..." >&2
    sleep "$delay"
    ((attempt++))
  done
}

publish_recursive_tree() {
  local source_dir="$1"
  local destination="$2"

  aws s3 cp --recursive --no-progress --only-show-errors \
    "$source_dir" "$destination"
}

publish_javadoc_tree() {
  local source_dir="$1"
  local destination="$2"
  local aws_config_file="$WORK_DIR/aws-javadoc-config"

  # `index-all.html` can be large enough to trigger multipart uploads, and the
  # multipart completion path is where we've seen intermittent missing-ETag
  # failures from `aws s3 cp`. Force single-part uploads for the expanded
  # javadoc tree; these files are comfortably below S3's 5 GiB single PUT limit.
  (
    export AWS_CONFIG_FILE="$aws_config_file"
    aws configure set default.s3.multipart_threshold 5GB >/dev/null
    publish_recursive_tree "$source_dir" "$destination"
  )
}

# Default matches dra-workflow.sh's `WORKFLOW="${DRA_WORKFLOW:-snapshot}"` so
# this script is safe to run standalone.
DRA_WORKFLOW="${DRA_WORKFLOW:-snapshot}"

case "$DRA_WORKFLOW" in
  snapshot) BUCKET="snapshots.elastic.co" ;;
  staging)  BUCKET="artifacts.elastic.co" ;;
  *) echo "unsupported DRA_WORKFLOW='$DRA_WORKFLOW'" >&2; exit 2 ;;
esac

MAVEN_DIR="${MAVEN_AGGREGATION_DIR:-build/dra-maven-aggregation}"
if [[ ! -d "$MAVEN_DIR" ]]; then
  echo "DRA maven aggregation tree not found: $MAVEN_DIR" >&2
  echo "  (produced by :prepareDraSnapshotMavenAggregation; must not be confused with" >&2
  echo "   :nmcpZipAggregation output at elasticsearch-maven-aggregation-*.zip which is" >&2
  echo "   Maven Central compliant and unsuitable for the DRA snapshot layout)" >&2
  exit 1
fi

WORK_DIR="$(mktemp -d -t es-maven-publish.XXXXXX)"
trap 'rm -rf "$WORK_DIR"' EXIT

# The maven tree is uploaded read-only straight from the Gradle output; only the
# expanded javadoc HTML tree needs a scratch directory.
JAVADOC_DIR="$WORK_DIR/javadoc"
mkdir -p "$JAVADOC_DIR"

echo "--- Expanding javadoc jars"
# Layout of the maven tree is standard:
#   <group-with-slashes>/<artifact>/<version>/<artifact>-<version>[-classifier].jar
# Walk *-javadoc.jar entries and mirror them under
#   javadoc/<groupPath>/<artifact>/<version>/
find "$MAVEN_DIR" -type f -name '*-javadoc.jar' -print0 | while IFS= read -r -d '' jar; do
  rel="${jar#"$MAVEN_DIR/"}"
  dir="$(dirname "$rel")"
  target="$JAVADOC_DIR/$dir"
  mkdir -p "$target"
  unzip -q -o "$jar" -d "$target"
done

echo "--- Publishing to s3://$BUCKET/{maven,javadoc}/"
# Use `cp --recursive` rather than `sync`: sync needs s3:ListBucket to diff the
# remote against the local tree, which the `unified-release-maven` role does
# not grant (only object-level Put/Get on `maven/*` and `javadoc/*`).
retry_with_backoff "maven tree upload" \
  publish_recursive_tree "$MAVEN_DIR/" "s3://$BUCKET/maven/"
retry_with_backoff "javadoc tree upload" \
  publish_javadoc_tree "$JAVADOC_DIR/" "s3://$BUCKET/javadoc/"

echo "Published to:"
echo "  https://$BUCKET/maven/"
echo "  https://$BUCKET/javadoc/"
