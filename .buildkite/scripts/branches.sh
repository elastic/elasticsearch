#!/bin/bash

# This determines which branches will have pipelines triggered periodically, for dra workflows.
BRANCHES=( $(cat branches.json | jq -r '.branches[].branch') )
