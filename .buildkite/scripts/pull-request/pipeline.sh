#!/bin/bash

set -euo pipefail

git config --global user.name elasticsearchmachine
git config --global user.email 'infra-root+elasticsearchmachine@elastic.co'

# This is temporary until we work out issues with the OIDC tokens
if [[ "${ELASTICSEARCH_MACHINE_GITHUB_TOKEN:-}" != "" ]]; then
  export VAULT_GITHUB_TOKEN="$ELASTICSEARCH_MACHINE_GITHUB_TOKEN"
fi

TEST_BRANCH=perms-test-branch-260505-02

git checkout -b "$TEST_BRANCH"
echo "" >> README.asciidoc
git add README.asciidoc
git commit -m "Testing"
git push -u origin "$TEST_BRANCH"
