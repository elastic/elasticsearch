#!/usr/bin/env bash
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".

# Move to the "native" directory
cd $(dirname $0)/..

# Compute the sha1 of normalized source files (strips CR that may be present on Windows).
# It will be used to version the artifacts
# Keep in sync with src-sha1.ps1
(find .cargo -type f; find src -type f; echo 'Cargo.toml'; echo 'Cargo.lock') \
  | sort | xargs cat | tr -d '\r' \
  | sha1sum | cut -d " " -f 1
