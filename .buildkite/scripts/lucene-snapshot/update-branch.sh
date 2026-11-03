#!/bin/bash

# Unused. The daily pipeline only triggers elasticsearch-agentic-workflow.
# lucene-merge-conflicts finish pushes a clean merge and then uploads the
# snapshot-tests trigger. A PR does not upload that trigger.

set -euo pipefail
echo "update-branch.sh is unused; lucene-merge-conflicts finish owns the test trigger." >&2
exit 1
