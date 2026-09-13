#!/bin/bash
#
# Fail if the coverage instrument recorded nothing.
#
#   check-nonzero.sh <exec-dir> <jacoco-cli-jar>
#
# An empty exec file is worse than a missing one: it reports as 0% and reads as a finding, when it
# actually means the agent never attached or the data never left the process. That mistake has been
# made twice on this codebase, both times costing hours.
#
# Every exec file is read with the JaCoCo CLI (execinfo) and judged by its executed-probe count.
# File size is not a proxy: real runs have produced healthy per-module exec files of ~220 bytes
# (two classes, real hits) and a sessions-only file can grow arbitrarily large while recording
# nothing.
#
# Layer semantics:
#   unit/, internal-cluster/  every exec file must record >0 executed probes. A task that ran and
#                             recorded nothing means the agent or the includes filter is wrong.
#   cluster/nodes.exec        the collector's output; must exist and record >0 probes. The two
#                             failure modes are distinguished because they have different fixes:
#                               - missing            -> no agent connected (injection is wrong)
#                               - present, 0 probes  -> connected but nothing recorded
#                                                       (filter or dump is wrong)
#   cluster/ (other files)    forked test-runner JVMs. They execute test code, not node code, so
#                             low or zero probe counts are expected. Reported, never gated.
#
set -euo pipefail
EXEC_DIR="$1"
CLI="$2"

# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

fail=0

while IFS= read -r f; do
  hits=$(coverage_exec_hits "$CLI" "$f")
  sessions=$(coverage_exec_sessions "$CLI" "$f")
  rel="${f#"$EXEC_DIR"/}"

  if [[ "$rel" == cluster/* && "$(basename "$f")" != "nodes.exec" ]]; then
    echo "cluster runner JVM $rel: $hits probes, $sessions sessions (informational)"
    continue
  fi

  if [[ "$hits" -eq 0 ]]; then
    echo "ZERO COVERAGE: $rel records no executed probes ($sessions sessions)."
    if [[ "$(basename "$f")" == "nodes.exec" ]]; then
      if [[ "$sessions" -gt 0 ]]; then
        echo "  Cluster layer: nodes connected to the collector but recorded nothing."
        echo "  Check the includes filter matches classes as loaded in the node."
      else
        echo "  Cluster layer: the collector wrote a file but no node ever sent data."
        echo "  Check tests.jvm.argline reaches the node and the collector port matches."
      fi
    else
      echo "  The agent attached (the file exists) but nothing inside the includes filter ran."
    fi
    fail=1
  else
    echo "$rel: $hits probes, $sessions sessions"
  fi
done < <(find "$EXEC_DIR" -name '*.exec' 2>/dev/null | sort)

if [[ -d "$EXEC_DIR/cluster" && ! -f "$EXEC_DIR/cluster/nodes.exec" ]]; then
  echo "ZERO COVERAGE: cluster layer ran but exec/cluster/nodes.exec does not exist -"
  echo "  no agent connected to the collector."
  echo "  Check tests.jvm.argline reaches the node and the collector port matches."
  fail=1
fi

if [[ "$fail" -eq 1 ]]; then
  echo "--- coverage instrument is broken; refusing to publish numbers"
  exit 1
fi
echo "--- all gated exec files contain data"
