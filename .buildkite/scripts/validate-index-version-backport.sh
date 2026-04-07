#!/bin/bash
#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".
#

set -euo pipefail

echo "--- Looking for index version changes"

index_versions_main=$(curl -s "https://raw.githubusercontent.com/elastic/elasticsearch/main/server/src/main/java/org/elasticsearch/index/IndexVersions.java")
index_versions_local=$(grep "def([0-9_]*," "${WORKSPACE}/server/src/main/java/org/elasticsearch/index/IndexVersions.java")

while IFS= read -r version; do
  if ! echo "${index_versions_main}" | grep -F "${version}"; then
    echo "Changes to index version [$(echo ${version:37} | cut -d' ' -f1)] missing from main branch."
    echo "Index version changes must first be merged to main before being backported."
    exit 1
  fi
done <<< "${index_versions_local}"

echo "All index version changes exist in main branch."
