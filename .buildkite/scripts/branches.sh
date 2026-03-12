#!/bin/bash

# This determines which branches will have pipelines triggered periodically, for dra workflows.
BRANCHES=( $(cat branches.json | jq -r '.branches[].branch') )

# Sort them to make ordering predictable
IFS=$'\n' BRANCHES=($(sort <<<"${BRANCHES[*]}"))
unset IFS
