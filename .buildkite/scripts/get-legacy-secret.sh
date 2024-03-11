#!/bin/bash

set -euo pipefail

# WARNING: this script will echo the credentials to the console. It is meant to be called from another script and captured in a variable.
# It should really only be used inside .buildkite/hooks/pre-command

source .buildkite/scripts/setup-legacy-vault.sh

vault read -format=json "$1"
