#!/bin/bash

set -euo pipefail

git config --global user.name elasticsearchmachine
git config --global user.email 'infra-root+elasticsearchmachine@elastic.co'

TEST_BRANCH=perms-test-branch-260505-01

git checkout -b "$TEST_BRANCH"
echo "" >> README.asciidoc
git commit -m "Testing"
git push -u origin "$TEST_BRANCH"
