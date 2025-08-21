#!/bin/bash

#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".
#

# Script to download all .exe files from protobuf protoc repository and generate SHA256 checksums
# URL to download from
VERSION="4.32.0"
URL="https://repo1.maven.org/maven2/com/google/protobuf/protoc/${VERSION}/"
DOWNLOAD_DIR="protoc-${VERSION}-executables"

# Create download directory if it doesn't exist
mkdir -p "${DOWNLOAD_DIR}"
cd "${DOWNLOAD_DIR}" || { echo "Failed to create/enter download directory"; exit 1; }

# Get the HTML content, extract links to .exe files (but not .exe.md5 etc.)
# Using grep with lookahead assertion to ensure we don't match .exe followed by something else
curl -s "${URL}" | grep -o 'href="[^"]*\.exe"' | grep -v -E 'jsonl' | grep -v -E '\.exe\.[^"]+' | sed 's/href="//g' | sed 's/"//g' > exe_files.txt

if [ ! -s exe_files.txt ]; then
    echo "No .exe files found at ${URL}"
    exit 1
fi

echo "Found $(wc -l < exe_files.txt | tr -d ' ') .exe files. Downloading..."

# Download each file
while IFS= read -r file; do
    curl -s -O "${URL}${file}"
done < exe_files.txt

echo "Generating SHA256 checksums..."

# Generate SHA256 checksums for all downloaded .exe files
if command -v shasum &> /dev/null; then
    # macOS/some Linux
    shasum -a 256 *.exe > SHA256SUMS.txt
elif command -v sha256sum &> /dev/null; then
    # Most Linux distributions
    sha256sum *.exe > SHA256SUMS.txt
else
    echo "Neither shasum nor sha256sum command found. Cannot generate checksums."
    exit 1
fi

# Print the checksums
cat SHA256SUMS.txt

cd ..
rm -rf "${DOWNLOAD_DIR}"
