#!/bin/bash
#
# Ship the coverage numbers to the metrics cluster so they accumulate into a trend instead of
# living inside one build's artifacts.
#
# This follows the same route the microbenchmarks take (index-micro-benchmark-results.sh): the
# credentials come from Vault via the pre-command hook when a step sets USE_PERF_CREDENTIALS, and
# the documents go in over HTTP. There is no coverage bucket to write to - the only S3 bucket this
# CI touches is the public download.elasticsearch.org one, which is the wrong place for this - so
# the cluster is where the numbers live and the browsable HTML stays a build artifact.
#
# Writes one document per layer with the headline figures, plus one per package for the merged
# layer, which is what makes a per-component breakdown queryable later.
#
# Never fails the caller. Coverage has already been measured and uploaded by the time this runs;
# a metrics cluster that is unreachable, or credentials that lack write access to the index, is a
# reporting problem and not a reason to lose the build.
#
set -uo pipefail
REPORT="${1:?usage: index-results.sh <report-dir>}"
INDEX="${COVERAGE_METRICS_INDEX:-metrics-code-coverage-default}"

if [[ -z "${PERF_METRICS_HOST:-}" ]]; then
  echo "--- not indexing coverage: no metrics credentials (set USE_PERF_CREDENTIALS on the step)"
  exit 0
fi

DOCS=$(python3 - "$REPORT" <<'PY'
import csv, glob, json, os, sys

report = sys.argv[1]
env = os.environ.get


def totals(rows):
    f = lambda k: sum(int(r[k]) for r in rows)
    lc, lm, bc, bm = f('LINE_COVERED'), f('LINE_MISSED'), f('BRANCH_COVERED'), f('BRANCH_MISSED')
    return {
        'line': {'covered': lc, 'missed': lm, 'total': lc + lm,
                 'pct': round(100 * lc / (lc + lm), 2) if lc + lm else 0.0},
        'branch': {'covered': bc, 'missed': bm, 'total': bc + bm,
                   'pct': round(100 * bc / (bc + bm), 2) if bc + bm else 0.0},
    }


common = {
    'scope': env('COVERAGE_PROJECTS', ''),
    'includes': env('COVERAGE_INCLUDES', ''),
    'git': {k: v for k, v in (('sha', env('BUILDKITE_COMMIT', '')),
                              ('branch', env('BUILDKITE_BRANCH', '')),
                              ('pull_request', env('BUILDKITE_PULL_REQUEST', ''))) if v and v != 'false'},
    'build': {k: v for k, v in (('pipeline', env('BUILDKITE_PIPELINE_SLUG', '')),
                                ('number', env('BUILDKITE_BUILD_NUMBER', '')),
                                ('url', env('BUILDKITE_BUILD_URL', ''))) if v},
}

docs = []
for csv_path in sorted(glob.glob(os.path.join(report, '*', 'coverage.csv'))):
    layer = os.path.basename(os.path.dirname(csv_path))
    rows = list(csv.DictReader(open(csv_path)))
    if not rows:
        continue
    docs.append({**common, 'layer': layer, 'granularity': 'total', **totals(rows)})
    # Per-package rows only for the union. The per-layer packages are noise: what a single layer
    # contributes to one package is not a coverage figure for it, only the merged number is.
    if layer == 'merged':
        by_pkg = {}
        for r in rows:
            by_pkg.setdefault(r['PACKAGE'], []).append(r)
        for pkg, prows in sorted(by_pkg.items()):
            docs.append({**common, 'layer': layer, 'granularity': 'package',
                         'package': pkg, **totals(prows)})

json.dump(docs, sys.stdout)
PY
)

count=$(printf '%s' "$DOCS" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)))' 2>/dev/null || echo 0)
if [[ "$count" == "0" ]]; then
  echo "--- not indexing coverage: no CSV reports under $REPORT"
  exit 0
fi

echo "--- indexing $count coverage documents into $INDEX"
BULK=$(printf '%s' "$DOCS" | python3 -c '
import json, sys, time
ts = int(time.time() * 1000)
out = []
for d in json.load(sys.stdin):
    out.append(json.dumps({"create": {}}))
    out.append(json.dumps({**d, "@timestamp": ts}))
print("\n".join(out))
')

resp=$(curl -sS -X POST "https://$PERF_METRICS_HOST/$INDEX/_bulk" \
  -u "$PERF_METRICS_USERNAME:$PERF_METRICS_PASSWORD" \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary "$BULK
" 2>&1) || true

if printf '%s' "$resp" | grep -q '"errors":false'; then
  echo "    indexed $count documents"
else
  # Loud but not fatal. The most likely cause is that the index does not exist yet or the
  # credentials cannot write to it, which needs a one-time grant from whoever owns that cluster.
  echo "    could not index coverage (non-fatal). Response:"
  printf '%s\n' "$resp" | head -c 800
  echo
fi
exit 0
