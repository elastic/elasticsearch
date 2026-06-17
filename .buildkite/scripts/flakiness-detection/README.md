# flakiness-detection

Detects test flakiness by repeatedly running a focused subset of tests and producing a summary report.

The package generates a list of Gradle invocations from one of three input sources, then either uploads them as a Buildkite sub-pipeline or executes them locally. A JUnit XML analyzer summarises the run as a markdown report.

## How to use it

There are three ways to trigger flakiness detection. All of them share the same internal pipeline; they differ only in **what tests get run** and **where they execute**.

### 1. Automatic PR pipeline (default)

Runs on every pull request. No action needed — the PR build includes the `flakiness-detection` sub-pipeline.

The detector compares the PR branch against its merge base and selects:
- **Changed tests** — every test file (`*Tests.java`, `*IT.java`, `*.yml` under `src/yamlRestTest/resources/`) added or modified in the PR.
- **Unmuted tests** — every entry **removed** from `muted-tests.yml`.

Driver: `entrypoints/pr.ts` invoked from `.buildkite/pipelines/pull-request/flakiness-detection.yml`.

### 2. Manually-triggered Buildkite pipeline

Use when you want to run flakiness detection against a hand-picked list of classes without pushing a branch. Trigger from the Buildkite UI: `elasticsearch / flakiness detection / manual`.

Build environment variables:

| Variable            | Required | Description                                                                                                                                                                                                                                                 |
| ------------------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `FLAKINESS_CLASSES` | yes      | Newline- or comma-separated list of FQCNs. Each spec is one of: `org.foo.BarTests` (whole class), `org.foo.BarTests.testFoo` (resolves to class — method-level filtering deferred), or `org.foo.YamlIT.test {yaml=/10_apm/Test name}` (specific yaml case). |
| `FLAKINESS_ITERS`   | no       | Positive integer applied uniformly to `-Dtests.iters` (unit + internalClusterTest) and `repeat-rest-test.sh` loop count. Defaults: 100 / 20 / 10 respectively.                                                                                              |

Driver: `entrypoints/manual.ts` invoked from `.buildkite/pipelines/flakiness-detection-manual.yml`. Pipeline registered in `catalog-info.yaml`.

### 3. Local CLI

Use when you want to reproduce a flakiness signal on your laptop.

```bash
node .buildkite/scripts/flakiness-detection/entrypoints/local.ts \
    [--iters N] \
    <Class>[ <Class>...]
```

Arguments are passed through `classifyExplicitList` (same parser as `FLAKINESS_CLASSES`). Each batch is executed sequentially via `./gradlew` directly (not the BK-agent wrapper). After the runner finishes, the analyzer scans freshly-written JUnit XML and prints a markdown summary to stdout.

Tips:
- `--iters 5` gives a quick sanity loop. The defaults (100 unit iters / 20 integ iters / 10 REST loops / 1 hour suite timeout) are CI-scale.
- The analyzer filters by file mtime, so it only counts XML written during *this* run — stale reports from prior local runs are ignored.

## How it works

Four modules form a one-way pipeline. Each module owns a single responsibility and a stable contract with its neighbour:

```
  ┌──────────────┐    ┌──────────┐    ┌─────────┐
  │  detectors   │ ─▶ │ commands │ ─▶ │ runners │ ─▶ JUnit XML on disk
  └──────────────┘    └──────────┘    └─────────┘                │
   ClassifiedTest[]   RunnableCommand[]                          ▼
                                                          ┌──────────┐
                                                          │ analyzer │ ─▶ markdown report
                                                          └──────────┘
```

### Module 1: detectors

Each detector takes an input shape specific to its trigger and emits `ClassifiedTest[]` plus an optional list of unresolvable inputs. All three are pure functions of their inputs (no I/O); the calling entrypoint reads files / runs git and passes strings in.

| File                         | Input                                                      | Used by                                         |
| ---------------------------- | ---------------------------------------------------------- | ----------------------------------------------- |
| `detectors/changed-files.ts` | List of file paths (typically from `git diff --name-only`) | `entrypoints/pr.ts`                             |
| `detectors/unmutes.ts`       | Old + new `muted-tests.yml` text + tracked repo files      | `entrypoints/pr.ts`                             |
| `detectors/explicit-list.ts` | Array of spec strings                                      | `entrypoints/manual.ts`, `entrypoints/local.ts` |

A `ClassifiedTest` (defined in `domain.ts`) carries the gradle project, the source set, the test kind, and the targeting hint (FQCN, yaml suite path, or parameterised case descriptor).

### Module 2: commands

`commands.ts` post-processes the merged `ClassifiedTest[]` and emits a runner-agnostic `RunnableCommand[]`:

1. `dedupeTests` — collapses identical entries.
2. `collapseYamlSuites` — when multiple `.yml` test resources share a parent directory, target the directory instead of the individual files (cuts Gradle's `tests.rest.suite` argument length).
3. `deduplicateYamlRunners` — at most one `yamlRestTestRunner` batch per Gradle project (the runner runs the whole source set).
4. `buildCommands` — group by kind in `KIND_ORDER`, slice into batches by `BatchingConfig.capByKind`, call `generateBatchCommand` per batch.

The output is a sequence of `RunnableCommand { kind, label, key, command }`. The `command` is a shell-ready string; the rest is metadata the runner uses to shape its output (BK step keys, log banners, etc.).

`BatchingConfig` (in `domain.ts`) carries all the tuning knobs: per-kind batch caps, per-kind iteration counts, REST-loop iteration count, suite timeout, and the **target** (`"buildkite"` or `"local"` — see "Target switching" below).

### Module 3: runners

Two implementations, one contract — both consume `RunnableCommand[]`:

- `runners/buildkite.ts` — `toBuildkitePipeline` (pure) produces a Buildkite pipeline structure; `uploadBuildkitePipeline` (impure) serializes to YAML and shells out to `buildkite-agent pipeline upload`. The function appends a final `flakiness-detection:analyze` step that depends on every batch step with `allow_failure: true`, so the report runs even when batches fail.
- `runners/local.ts` — `runLocally` executes each command sequentially via `execSync` with inherited stdio. Returns the worst exit code seen (does **not** stop on first failure — the developer sees all batch results).

### Module 4: analyzer

Runs **after** the batches complete. Reads JUnit XML written by Gradle (`*/build/test-results/*/TEST-*.xml`), classifies each failure entry, and aggregates per `(class, method)` summaries.

| File                  | Responsibility                                                                                                                                                                                    |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `analyzer/analyze.ts` | Walk the workspace for JUnit XML, stream-parse via `sax`, classify failures, produce `FlakinessReport`. Streaming keeps peak memory bounded by test count (not file size), so the analyze step survives K8s agents even when a report grows into the hundreds of MiB. Pure; takes an optional `minMtimeMs` to skip pre-existing reports during local runs. |
| `analyzer/render.ts`  | `FlakinessReport → markdown`. `severity()` derives the Buildkite annotation style.                                                                                                                |

Failure classification (`classifyFailure`):

- `"suite-timeout"` — message matches `Test abandoned because suite timeout was reached.` or `Suite timeout exceeded (>= ...`. Informational; not counted as a real failure.
- `"assertion"` — `AssertionError` and subclasses.
- `"error"` — any other Exception / Error.
- `"other"` — everything else.

This mirrors the failure shapes randomised-runner emits when `@TimeoutSuite` fires.

## File layout

```
flakiness-detection/
  README.md
  domain.ts              types, constants, KIND_* tables, DEFAULT_*_CONFIG
  detectors/
    changed-files.ts     git-diff source
    unmutes.ts           muted-tests.yml diff source
    explicit-list.ts     FQCN list source
  commands.ts            dedupe / collapse / batch / emit RunnableCommand[]
  runners/
    buildkite.ts         RunnableCommand[] → BK YAML + upload
    local.ts             RunnableCommand[] → sequential execSync
  analyzer/
    analyze.ts           JUnit XML → FlakinessReport
    render.ts            FlakinessReport → markdown + severity
  entrypoints/
    pr.ts                changed-files + unmutes (PR pipeline)
    manual.ts            env-var driven (manual BK pipeline)
    local.ts             argv driven (developer laptop)
    analyze.ts           final BK step — runs analyzer and posts annotation
```

Per-module test files (`*.test.ts`) sit alongside their source. Run with `cd .buildkite && pnpm test scripts/flakiness-detection`.
