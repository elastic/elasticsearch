#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".
#

#!/bin/bash

# Allow overriding the time window (in hours) to check for new builds, defaults to 24
RECENT_TIME_WINDOW=${RECENT_TIME_WINDOW:-24}

# Extract current JDK major version from bundled_jdk in version.properties
CURRENT_JDK=$(grep "^bundled_jdk =" build-tools-internal/version.properties | cut -d'=' -f2 | tr -d ' ' | cut -d'+' -f1 | cut -d'.' -f1)
TARGET_JDK=$((CURRENT_JDK + 1))

echo "Current JDK major version: $CURRENT_JDK"
echo "Target JDK major version: $TARGET_JDK"

# Query Elasticsearch JDK archive for available JDKs
JDK_ARCHIVE_URL="https://builds.es-jdk-archive.com/jdks/openjdk/recent.json"
echo "Querying JDK archive: $JDK_ARCHIVE_URL"

# Fetch JDK info and filter for target major version
JDK_DATA=$(curl -s "$JDK_ARCHIVE_URL")

if [[ -z "$JDK_DATA" ]]; then
  echo "Failed to fetch JDK data from archive"
  exit 1
fi

# Find the latest build for the target JDK version
LATEST_BUILD=$(echo "$JDK_DATA" | jq -r --arg target "$TARGET_JDK" '
  .majors[$target].builds |
  sort_by(.archived_at) |
  last'
)

if [[ "$LATEST_BUILD" == "null" || -z "$LATEST_BUILD" ]]; then
  echo "No builds found for JDK $TARGET_JDK"
  exit 1
fi

# Extract timestamp and JDK identifier
TIMESTAMP=$(echo "$LATEST_BUILD" | jq -r '.archived_at')
JDK_IDENTIFIER=$(echo "$LATEST_BUILD" | jq -r '.id')

echo "Latest JDK ${TARGET_JDK} build from ES archive:"
echo "  Timestamp: $TIMESTAMP"
echo "  JDK Identifier: $JDK_IDENTIFIER"

# Check if timestamp is within last 24 hours
CURRENT_TIME=$(date +%s)
BUILD_TIME=$(date -d "$TIMESTAMP" +%s 2>/dev/null || date -j -f "%Y-%m-%dT%H:%M:%S" "${TIMESTAMP%Z}" +%s 2>/dev/null || echo "0")

if [[ "$BUILD_TIME" == "0" ]]; then
  echo "Failed to parse timestamp: $TIMESTAMP"
  SHOULD_TRIGGER="false"
else
  TIME_DIFF=$((CURRENT_TIME - BUILD_TIME))
  TIME_WINDOW=$((RECENT_TIME_WINDOW * 60 * 60))

  if [[ $TIME_DIFF -lt $TIME_WINDOW ]]; then
    echo "Build is recent (less than ${RECENT_TIME_WINDOW}h old)"
    SHOULD_TRIGGER="true"
  else
    echo "Build is older than ${RECENT_TIME_WINDOW} hours"
    SHOULD_TRIGGER="false"
  fi
fi

echo "SHOULD_TRIGGER: $SHOULD_TRIGGER"


if [[ "$SHOULD_TRIGGER" == "true" ]]; then
  EFFECTIVE_START_DATE=$(date -u -d "@$BUILD_TIME" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u -r "$BUILD_TIME" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || echo "")

  # Use "master" branch for elasticsearch-performance-esbench-jdk when BUILDKITE_BRANCH is "main"
  TRIGGER_BRANCH="$BUILDKITE_BRANCH"
  if [[ "$BUILDKITE_BRANCH" == "main" ]]; then
    TRIGGER_BRANCH="master"
  fi

  echo "Triggering performance-esbench-jdk for new jdk build $JDK_IDENTIFIER"
  cat << EOF | buildkite-agent pipeline upload
steps:
- trigger: elasticsearch-performance-esbench-jdk
  label: Triggering performance-esbench-jdk for new jdk build $JDK_IDENTIFIER
  async: true
  build:
    branch: "$TRIGGER_BRANCH"
    env:
      EFFECTIVE_START_DATE: "$EFFECTIVE_START_DATE"
      EXECUTION_MODE: "start-run"
      JDK_VERSION: "$TARGET_JDK"
      JDK_IDENTIFIER: "$JDK_IDENTIFIER"
EOF
fi
