# simdjson ESCF Benchmarking Plan

Step-by-step guide for measuring simdjson-backed JSON encoding (`EscfEncoder` →
`SimdJsonDirectWalker`) against the Jackson baseline. Uses two complementary tiers:

| Tier | Tool | What it measures | When to use |
|------|------|------------------|-------------|
| **1 — Micro** | JMH (`SimdJsonParserBenchmark`) | ESCF encode throughput in isolation | Fast iteration while optimizing H2/H3/etc. |
| **2 — Macro** | esbench + Rally | End-to-end bulk indexing (ClickBench) | Validate real indexing impact before merge |

Results and hypotheses live in [`PERF_HYPOTHESES.md`](PERF_HYPOTHESES.md). This document is
the **runbook** — follow it in order for each experiment cycle.

---

## 0. One-time setup

### Elasticsearch repo (this checkout)

- Branch: `simdjson_stage1` (or your feature branch), pushed to a remote GitHub fork.
- JDK **26+** for AWS runs (JDK 21 works but ARM throughput is ~10–20% lower).
- Native stage1 library is built automatically by Gradle for Linux targets; for manual
  JMH runs you can also build locally:

```bash
cd libs/simdjson/native
make local CLANG_CXX=clang++
# optional: copy into release/ layout for -Des.nativelibs.path=…
mkdir -p release/linux-$(uname -m | sed 's/x86_64/x64/')
cp build/linux-*/libes_simdjson.so release/linux-*/
```

### elasticsearch-benchmarks repo (esbench)

Repo path: `~/git/elasticsearch-benchmarks`

```bash
cd ~/git/elasticsearch-benchmarks
make install
# add to ~/.bashrc or ~/.zshrc:
alias esbench='uv run --locked --project ~/git/elasticsearch-benchmarks esbench'
```

Authenticate per [esbench quick start](https://github.com/elastic/elasticsearch-benchmarks/blob/master/docs/get-started/esbench-quickstart.md):

- Infra Vault (`esbench list environments` should succeed)
- AWS (required for the instance types below)
- SSH key: `~/.ssh/esbench` (shared) or your own key passed with `--ssh-key`

### Recommended hardware (AWS)

Run **two sequential full environments** — one for ARM, one for x64. Each `esbench start`
provisions a fresh loaddriver VM and ES node. Use the **same loaddriver instance type** for both;
esbench does not allow mixed CPU architectures in one cluster.

| Role | ARM run | x64 run |
|------|---------|---------|
| Loaddriver | `aws::m6gd_8xlarge-32-128-1x1900GB_nvme_local` | same type (new VM) |
| ES node | `aws::c8gd_8xlarge-32-64-1x1900GB_nvme_local` | `aws::c6gd_8xlarge-32-64-1x1900GB_nvme_local` |
| Region | `eu-west-1` (typical) | same |

`m6gd` is Graviton (ARM). For the **x64** environment, `esrally build
--source-build-method=docker` on that loaddriver cross-compiles the x86_64 tarball for the
`c6gd` ES node.

List valid names:

```bash
esbench info --use-case=simple | rg 'm6gd|c8gd|c6gd'
```

### Native simdjson library (checked into repo temporarily)

Until `org.elasticsearch:es-simdjson:0.1.0` is on Artifactory, this branch ships prebuilt
`libes_simdjson.*` under `libs/native/libraries/prebuild/platform/` (same layout as the
`make install` target under `build/platform/`). Gradle copies them during `:extractLibs`.

```
libs/native/libraries/prebuild/platform/
  darwin-aarch64/libes_simdjson.dylib
  linux-aarch64/libes_simdjson.so
  linux-x64/libes_simdjson.so
```

**No loaddriver staging or `LOCAL_SIMDJSON_BINARY=1` is needed for esbench.**

After changing native C++ (`libs/simdjson/native/src/**`), regenerate and commit:

```bash
cd libs/simdjson/native
docker login docker.elastic.co   # once; https://docker-auth.elastic.co
./publish_simdjson_binaries.sh --local --install-to-gradle-platform
git add -f ../../native/libraries/prebuild/platform/*/libes_simdjson.*
```

For local JMH iteration on one platform only:

```bash
cd libs/simdjson/native
make install CLANG_CXX=clang++   # overwrites the checked-in file for your platform
```

Verify native load after ES starts (no unexpected Jackson fallback spam at DEBUG).

---

## 1. Pre-flight checklist (every experiment)

Run locally before spending cloud time.

```bash
cd /path/to/elasticsearch

# Unit tests for simdjson + ESCF
./gradlew :libs:simdjson:test :server:test \
  --tests 'org.elasticsearch.escf.EscfEncoderSimdJsonTests' \
  --tests 'org.elasticsearch.escf.EscfBatchBuilderTests.testBeginRowResetsUnfinishedRow'

# Compile benchmarks
./gradlew :benchmarks:compileJava
```

Record metadata for the run log:

- Git commit: `git rev-parse HEAD`
- JDK: `java -version`
- CPU: `uname -m` and `/proc/cpuinfo` flags on the target host
- Feature flags (snapshot builds enable these by default):
  - `batch_indexing`
  - `simdjson_escf`
- Native library loaded: setup line in JMH prints `nativeStage1=true`

**Push** your branch before esbench builds from `--es-repo-url` / remote revision.

---

## 2. Tier 1 — JMH microbenchmark

Benchmark class:
`benchmarks/src/main/java/org/elasticsearch/benchmark/xcontent/SimdJsonParserBenchmark.java`

Compares `simdJsonEncode` (default `EscfEncoder`) vs `jacksonEncode` (`allowSimd=false`).

Document shapes (via `-p shape=…`):

| Shape | Size | Fields | Notes |
|-------|------|--------|-------|
| `clickbench_flat` | ~2.5 KB | ~100 flat numerics/strings | Primary regression shape |
| `otel_nested` | ~700–900 B | ~20, 3-level nesting | Field-name + nesting stress |
| `small_sparse` | ~100–150 B | 6–7, 3 rotating variants | Per-doc overhead |

### 2a. Quick local smoke test

```bash
cd benchmarks
../gradlew run --args \
  'org.elasticsearch.benchmark.xcontent.SimdJsonParserBenchmark \
   -t 1 -wi 1 -i 1 -p shape=clickbench_flat -p docCount=1000'
```

Expect `[setup] … nativeStage1=true` in output. If `false`, native lib is missing.

### 2b. Full run on AWS (manual SSH or esbench node)

On the target Linux host, clone/build Elasticsearch (or rsync your checkout), then:

```bash
cd benchmarks
NATIVE_LIBS="$PWD/../libs/simdjson/native/release/linux-$(uname -m | sed 's/x86_64/x64/')"
THREADS=8   # x64: 8, ARM: 4 is a reasonable starting point

../gradlew --no-daemon run --args \
  "org.elasticsearch.benchmark.xcontent.SimdJsonParserBenchmark \
   -t ${THREADS} -wi 3 -i 5 \
   -jvmArgs -Des.nativelibs.path=${NATIVE_LIBS} \
   -rf json -rff /tmp/bench/simdjson_${THREADS}t_$(git -C .. rev-parse --short HEAD).json" \
  | tee /tmp/bench/simdjson_${THREADS}t.log
```

Run **both** x64 and ARM if possible. Repeat after each meaningful commit.

### 2c. CPU / allocation profiling (optional, Tier 1)

```bash
PROF_LIB="$HOME/async-profiler-4.5-linux-$(uname -m)/lib/libasyncProfiler.so"
sudo sysctl -w kernel.perf_event_paranoid=1

../gradlew --no-daemon run --args \
  "org.elasticsearch.benchmark.xcontent.SimdJsonParserBenchmark \
   -t ${THREADS} -wi 3 -i 5 \
   -prof async:output=flamegraph;dir=/tmp/bench;event=cpu;libPath=${PROF_LIB} \
   -jvmArgs -Des.nativelibs.path=${NATIVE_LIBS} \
   -rf json -rff /tmp/bench/cpu_profile.json"
```

Inspect top frames: `resolveFieldName`, `drainScratchValue`, `es_stage1_run`, `commitScratchTo`.
Update [`PERF_HYPOTHESES.md`](PERF_HYPOTHESES.md) with before/after tables.

### 2d. JMH on esbench-provisioned nodes

SSH to the ES node or loaddriver (same arch), clone your fork, run the native + JMH steps from §0
and §2b. Terminate the environment when finished.

---

## 3. Tier 2 — esbench + Rally (ClickBench macro)

This exercises the **production bulk path**: `BulkOperation` → `BulkBatchEncoders` →
`EscfEncoder.parseToScratch()` → simdjson direct walker (when eligible).

### 3a. Benchmark definition

The params files (`simdjson-esbench-*.json`) embed the Rally track inline — equivalent to
nightly **`clickbench-columnar-mode-columnar-stored`**:

| Param | Value | Why |
|-------|-------|-----|
| `track.name` | `clickbench` | ClickBench corpus |
| `track.params.scenario` | `doc-values-only` | Columnar indexing mode |
| `track.params.use_columnar_stored_source_mode` | `true` | ESCF encode path (simdjson target) |
| `track.params.run_searches` | `false` | Index-only race |
| `exclude.tasks` | search/esql/composite/mget | Skip non-index tasks |

Other ClickBench variants (for follow-up only):

| Benchmark name | Why |
|----------------|-----|
| `clickbench-columnar-mode` | Doc-values columnar without columnar stored source |
| `clickbench` | Vanilla baseline |

### 3b. Run ARM then x64 (two sequential environments)

Params files in `~/git/elasticsearch-benchmarks/` (infra + Rally track + cluster settings):

- `simdjson-esbench-arm64.json` — c8gd ES + m6gd loaddriver
- `simdjson-esbench-x64.json` — c6gd ES + m6gd loaddriver

(`c8gd` is ARM-only — use `c6gd` for the x64 ES node.)

**ARM environment:**

```bash
cd ~/git/elasticsearch-benchmarks

esbench start --use-case=simple --params=simdjson-esbench-arm64.json
# note env-id
esbench ssh --env-id <arm-env-id>
cd ~/scripts && ./benchmark.py
esbench terminate --env-id <arm-env-id>
```

**x64 environment** (new loaddriver + ES VMs, same loaddriver instance type):

```bash
esbench start --use-case=simple --params=simdjson-esbench-x64.json
esbench ssh --env-id <x64-env-id>
# on loaddriver:
cd ~/scripts && ./benchmark.py
esbench terminate --env-id <x64-env-id>
```

Push your fork before each `esbench start` if the revision is a branch name.

### 3c. Elasticsearch build on the loaddriver

`./benchmark.py` runs `esrally build` via Ansible (`copy_artifact` role) using the default
**docker** source build. With checked-in simdjson binaries (§0), no loaddriver prep is required.

```bash
esbench ssh --env-id <env-id>
cd ~/scripts && ./benchmark.py
```

Verify the distribution tar contains the native library (optional):

```bash
tar -tzf ~/.rally/benchmarks/distributions/elasticsearch-*-linux-aarch64*.tar.gz | grep libes_simdjson
# x64 environment: elasticsearch-*-linux-x86_64*.tar.gz
```

To inspect a failed build:

```bash
grep -i error ~/.rally/logs/rally*.log | tail -30
```

Subsequent `./benchmark.py` runs on the **same** environment reuse the cached tarball until
you delete it or change revision (§3d).

### 3d. Switching commits on an existing loaddriver

Esbench stores revision and repo URL in Ansible inventory on the loaddriver. To benchmark a
different commit **without** reprovisioning VMs:

```bash
esbench ssh --env-id <env-id>    # loaddriver

ENV=$HOME/.esbench/environments/<env-id>
cd "$ENV/ansible"
grep revision inventory-private-ip.yaml    # note current value

# Update revision (commit SHA recommended; branch name tracks tip on each build)
sed -i 's/revision: <old>/revision: <new-sha>/' inventory-private-ip.yaml

# If switching forks, also update Rally's remote and drop stale checkout:
sed -i 's|remote.repo.url = .*|remote.repo.url = https://github.com/<fork>/elasticsearch.git|' ~/.rally/rally.ini
rm -rf ~/.rally/benchmarks/src/elasticsearch

# Rebuild ES at the new revision (push branch with checked-in simdjson binaries first)
rm -f ~/.rally/benchmarks/distributions/elasticsearch-*.tar.gz

# Run benchmark
cd ~/scripts && ./benchmark.py
```

**Revision semantics** (from
[esbench custom-commit docs](https://github.com/elastic/elasticsearch-benchmarks/blob/master/docs/how-to/workflows/benchmark-custom-commit.md)):

| Param value | Behaviour |
|-------------|-----------|
| Commit SHA | Pinned; same commit on every `./benchmark.py` until you change it |
| Branch name | `./benchmark.py` fetches latest tip each time |
| `main@2026-08-26T10:00:00Z` | Frozen branch tip at that UTC timestamp |

Always **`git push`** your fork before pointing the loaddriver at a new SHA.

### 3e. Params file reference

Each `simdjson-esbench-*.json` bundles infra, track, and cluster settings. Key fields:

```json
{
  "elasticsearch.remote.repo.url": "https://github.com/ChrisHegarty/elasticsearch.git",
  "elasticsearch.remote.repo.revision": "simdjson_stage1",
  "elasticsearch.cluster.settings": {
    "indices.batch_indexing": true
  },
  "async_profiler.autocollect": false,
  "stats.telemetry": ["node-stats"],
  "track.name": "clickbench",
  "track.repository": "internal",
  "track.challenge": "index-and-search",
  "track.params": {
    "bulk_indexing_clients": 24,
    "bulk_size": 1000,
    "scenario": "doc-values-only",
    "run_searches": false,
    "use_columnar_stored_source_mode": true
  },
  "exclude.tasks": ["type:search", "type:esql", "type:composite", "mget"]
}
```

**`indices.batch_indexing: true`** is required — snapshot builds enable the
`batch_indexing` and `simdjson_escf` feature flags, but the node setting defaults to
`false`. Without it, simdjson is not used on the bulk path.

Set `async_profiler.autocollect: true` in the params file if you want automatic CPU profiles
(nightly columnar benchmarks use `false` by default).

Optional logging to detect Jackson fallbacks — add to the params file:

```json
"elasticsearch.jvm.options": [
  "-Dlogger.org.elasticsearch.escf.EscfEncoder=DEBUG"
]
```

### 3f. Run baseline vs contender (paired comparison)

Run **separately on ARM and x64** — repeat §3b for each architecture with baseline then
contender revision (or use `esbench compare` for PRs):

```bash
cd ~/git/elasticsearch-benchmarks
esbench compare --pr https://github.com/<fork>/elasticsearch/pull/<num> \
  clickbench-columnar-mode-columnar-stored
```

Manual paired runs per architecture — update `elasticsearch.remote.repo.revision` in the
params file (or §3d on the loaddriver) and repeat §3b for baseline then contender:

```bash
BASELINE=<merge-base-sha>
CONTENDER=<your-branch-sha>

# ARM: two sequential esbench start cycles (§3b), revision = BASELINE then CONTENDER
# x64: same with simdjson-esbench-x64.json
```

Apply §3c on each loaddriver if Artifactory lacks `es-simdjson`. To swap revision on an
existing loaddriver without reprovisioning, use §3d.

### 3g. Terminate environments

```bash
esbench list environments
esbench terminate --env-id <env-id>
```

Always terminate when done — esbench environments cost real money.

---

## 4. Analyze results

### Tier 1 (JMH)

- Primary metric: **throughput (ops/s)** for `simdJsonEncode` vs `jacksonEncode`.
- Report per shape and thread count.
- Speedup = `simd / jackson - 1`.
- Copy numbers into [`PERF_HYPOTHESES.md`](PERF_HYPOTHESES.md).

### Tier 2 (Rally / esbench)

- Open the [PR Race Comparison dashboard](https://esbench-metrics.kb.us-east-2.aws.elastic-cloud.com:9243/app/dashboards#/view/d9079962-5866-49ef-b9f5-145f2141cd31)
  with KQL: `fields.env_id: <baseline-id> or fields.env_id: <contender-id>`.
- Focus on **`index`** throughput and latency — that is where simdjson applies.
- Compare against [historic nightlies](https://elasticsearch-benchmarks.elastic.co/) for noise context.
- Download async-profiler artifacts:

```bash
esbench download profiles --env-id <env-id> --all
```

### Validate simdjson was actually used (macro)

On an ES node during/after the run:

1. Logs: no unexpected volume of `Direct walk failed, falling back to Jackson` (DEBUG/WARN).
2. ClickBench docs are ~2.8 KB JSON objects — well under `MAX_DOC_BYTES` (16 KiB).
3. Snapshot build logs show `feature flag [batch_indexing] is enabled` and native simdjson loaded.

---

## 5. Experiment cycle (recommended workflow)

For each optimization (e.g. H2 field-name cache, H3 SWAR numbers):

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Implement + unit tests (EscfEncoderSimdJsonTests, etc.)  │
└───────────────────────────┬─────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. Tier 1 JMH on x64 + ARM — record ops/s in PERF_HYPOTHESES│
└───────────────────────────┬─────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. If Tier 1 shows ≥~2% on clickbench_flat: Tier 2 esbench  │
│    clickbench-columnar-mode-columnar-stored (baseline/contender)│
└───────────────────────────┬─────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. Profile if regression or unexpected flat result           │
└───────────────────────────┬─────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. Update PERF_HYPOTHESES.md; commit                         │
└─────────────────────────────────────────────────────────────┘
```

**When to skip Tier 2:** pure micro-optimizations that only touch test helpers, or changes
with no JMH movement on `clickbench_flat`.

**When Tier 2 is mandatory:** anything touching `EscfEncoder`, `BulkBatchEncoders`,
`SimdJsonDirectWalker`, native stage1, or feature-flag defaults.

---

## 6. Baseline reference numbers (JDK 26, native stage1)

Most recent JMH snapshot (see [`PERF_HYPOTHESES.md`](PERF_HYPOTHESES.md) for full history):

| Shape | x64 simd | ARM simd | vs Jackson (approx) |
|-------|----------|----------|---------------------|
| clickbench_flat | ~103 ops/s | ~51 ops/s | +6–11% |
| otel_nested | ~411 ops/s | ~190 ops/s | flat / variable |
| small_sparse | ~1115 ops/s | ~548 ops/s | +6% |

Current hotspots after H2: `lookupField`, `scanFieldName`, native stage1, ESCF
`commitScratchTo` / `drainScratchValue` (~19–25% combined).

---

## 7. Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `nativeStage1=false` in JMH setup | Native lib not on path | §0 checked-in binaries, `make install`, or `-Des.nativelibs.path=…` |
| `esrally build` / Gradle fails resolving `es-simdjson` | Checked-in prebuilds missing from branch | §0: commit `libs/native/libraries/prebuild/platform/*/libes_simdjson.*` |
| `benchmark.py` fails at `copy_artifact : build elasticsearch` | Stale tarball or missing native in tar | `rm ~/.rally/benchmarks/distributions/*`; verify §0 binaries committed |
| simd ≈ jackson in JMH | Feature flag off or SIMD disabled | Snapshot build; check `SimdJsonPool.isEnabled()` |
| Macro run shows no ingest diff | `indices.batch_indexing` false | Add cluster setting (§3e) |
| `./benchmark.py` still runs old commit | Cached tarball + pinned inventory | §3d: `sed` revision, `rm ~/.rally/benchmarks/distributions/*` |
| `./benchmark.py` does not pick up new branch tip | Revision is a SHA not branch | Use branch name or update SHA in inventory |
| Wrong CPU binary on ES node | Built wrong `--target-arch` tarball | `esrally build --target-arch=aarch64` vs `x86_64` explicitly |
| x64 build on m6gd loaddriver fails | Docker not available on loaddriver | esbench loaddriver needs Docker for default `source-build-method=docker` |
| `:benchmarks:run` fails on Mac | Native lib is Linux-only | Run JMH on AWS Linux node |
| High fallback log volume | JSON edge cases in corpus | Check track; fix walker or accept fallback |
| esbench auth failure | Vault token expired | Re-login via Vault OIDC |

---

## 8. Optional: PR nightly benchmark (post-merge validation)

After opening a PR against `elastic/elasticsearch`:

```bash
esbench compare --pr https://github.com/elastic/elasticsearch/pull/<num> \
  clickbench-columnar-mode-columnar-stored
```

Or comment on the PR (deprecated but still supported):

```
Buildkite benchmark this with clickbench-columnar-mode-columnar-stored please
```

---

## 9. Files to keep in sync

| File | Purpose |
|------|---------|
| [`PERF_HYPOTHESES.md`](PERF_HYPOTHESES.md) | Results tables, flamegraph notes, hypothesis status |
| [`BENCHMARK_PLAN.md`](BENCHMARK_PLAN.md) | This runbook |
| `benchmarks/.../SimdJsonParserBenchmark.java` | JMH harness + run instructions in Javadoc |
| `~/git/elasticsearch-benchmarks/simdjson-esbench-arm64.json` | esbench params — c8gd + ClickBench columnar-stored track |
| `~/git/elasticsearch-benchmarks/simdjson-esbench-x64.json` | esbench params — c6gd + ClickBench columnar-stored track |
| `libs/simdjson/native/publish_simdjson_binaries.sh` | Regenerate checked-in native libs (`--install-to-gradle-platform`) |
| `EscfEncoderSimdJsonTests.java` | Correctness gate before any benchmark run |

---

## 10. Next benchmarks to plan

After current H2 work lands, prioritize profiling-driven targets:

1. **ESCF commit path** — `commitScratchTo` / `drainScratchValue` (~20%+ on clickbench_flat).
2. **Nested field ordinals** — improve `otel_nested` ARM/x64 parity.
3. **Release-build flag check** — repeat Tier 2 with `-Des.simdjson_escf_feature_flag_enabled=true`
   and `elasticsearch.source.build.release: true` to approximate production defaults.
