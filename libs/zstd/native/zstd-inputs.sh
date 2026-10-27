#!/usr/bin/env bash
#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License, v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".

ZSTD_VERSION=1.5.7
ZSTD_SOURCE_URL=https://codeload.github.com/facebook/zstd/tar.gz/refs/tags/v${ZSTD_VERSION}
ZSTD_SOURCE_SHA256=37d7284556b20954e56e1ca85b80226768902e2edabd3b649e9e72c0c9012ee3

DARWIN_AARCH64_BOTTLE_URL=https://ghcr.io/v2/homebrew/core/zstd/blobs/sha256:56faac95df2a57e866c23339db384c5b3fe7d58984314c1a0bba2823123e0972
DARWIN_AARCH64_BOTTLE_SHA256=56faac95df2a57e866c23339db384c5b3fe7d58984314c1a0bba2823123e0972
DARWIN_X64_BOTTLE_URL=https://ghcr.io/v2/homebrew/core/zstd/blobs/sha256:a109e4645ddd9a1f091f082b525b5ef5032d3568c016c3ec657f63068c068cc4
DARWIN_X64_BOTTLE_SHA256=a109e4645ddd9a1f091f082b525b5ef5032d3568c016c3ec657f63068c068cc4

WINDOWS_X64_ARCHIVE_URL=https://github.com/facebook/zstd/releases/download/v${ZSTD_VERSION}/zstd-v${ZSTD_VERSION}-win64.zip
WINDOWS_X64_ARCHIVE_SHA256=acb4e8111511749dc7a3ebedca9b04190e37a17afeb73f55d4425dbf0b90fad9
