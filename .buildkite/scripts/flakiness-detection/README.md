# flakiness-detection

Detects test flakiness by repeatedly running a focused subset of tests and producing a summary report.

The package gathers input references from one of three sources, hands them to a Java/Gradle resolver that turns them into a plan carrying ready batch commands, then either uploads those commands as a Buildkite sub-pipeline or executes them locally. A JUnit XML analyzer summarises the run as a markdown report.

> **Architecture note (B2).** Resolution of inputs to concrete test targets - which Gradle project / source set / kind a file or class belongs to, whether a class is abstract (and its concrete subclasses), and **which Gradle task actually re-runs it** - is done by a **Java/Gradle resolver** in `build-tools-internal` (`org.elasticsearch.gradle.internal.flakiness`, tasks `flakinessResolveProject` / `flakinessScan`), not by TypeScript path regexes.
> **Batch-command generation** (dedupe, yaml-suite collapse, per-cap batching, and assembly of the per-batch Gradle command string) also lives in Java now: the `flakinessScan` task emits ready commands into `flakiness-plan.json`'s `commands` array.
> Each command carries the literal token `__GRADLE__` wherever the gradle binary belongs; the TS runner layer substitutes it with `.ci/scripts/run-gradle.sh` (Buildkite) or `./gradlew` (local), so Java stays target neutral.
> TS owns only input gathering, gradle-binary substitution, Buildkite orchestration, and JUnit analysis.
> See `JAVA_RESOLVER_NOTES.md` for the design, the friction, and an honest assessment. The two contracts between the layers are `flakiness-refs.json` (gather → resolver) and `flakiness-plan.json` (resolver → generate).

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
|---------------------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
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

Arguments become `explicit` refs (same specs as `FLAKINESS_CLASSES`); `local.ts` writes `flakiness-refs.json`, runs `./gradlew -Pflakiness.resolve flakinessResolveProject` (unqualified, so every project runs it and self-selects), compiles every test source set unqualified (a compile failure prints `buildFailed` and exits 1), runs `./gradlew -Pflakiness.resolve flakinessScan` to produce the plan, then substitutes `__GRADLE__` → `./gradlew` in each of the plan's batch commands and executes them sequentially (directly, not via the BK-agent wrapper). After the runner finishes, the analyzer scans freshly-written JUnit XML and prints a markdown summary to stdout. (Requires the root build to apply `elasticsearch.internal-flakiness-resolve`; it is applied but inert unless `-Pflakiness.resolve` is set.)

Tips:
- `--iters N` overrides the iteration count. Because Java now owns iteration counts (baked into the plan's batch commands), the override is passed through to the scan task as `-Pflakiness.iters=N` rather than applied by TS afterwards. The defaults (100 unit iters / 20 integ iters / 10 REST loops / 1 hour suite timeout) are CI-scale.
- The analyzer filters by file mtime, so it only counts XML written during *this* run — stale reports from prior local runs (including under `flakiness-iters/`, see below) are ignored.

## How it works

The pipeline topology is `bootstrap → [orchestration + generate] → batch + analyze`.
Step 1 (bootstrap, TS) gathers `FlakinessRef[]` into `flakiness-refs.json` and uploads two steps: an orchestration step and a separate generate step.
The orchestration step runs three phases sequentially on ONE gradle agent: `resolve` (refs → one `<project>.json` per project under `build/flakiness/project-targets/`, carrying its resolved targets and its class directories), `compile` (a plain, unqualified invocation of `compileTestJava compileInternalClusterTestJava compileJavaRestTestJava compileYamlRestTestJava`, i.e. every test source set in the repo - its non-zero exit is the only `build_failed` signal), and `scan` (ASM-scans the union of every project's class directories into `flakiness-plan.json`, including the ready batch `commands`).

The compile phase reads nothing from resolve except one guard: it is skipped entirely when resolve produced no targets, so a docs-only PR does not pay a whole-repo test compile to produce an empty plan (`pr.ts` makes every changed file a ref, so the bootstrap's own short-circuit rarely fires). The scan still runs either way, because it is what reports refs no project could claim. Compiling everything is what lets the scan connect an abstract test base to concrete subclasses in *other* Gradle projects, which a subset compile cannot: the ASM scan can only report a class abstract if it visited that class's own `.class` file. Measured on a real CI agent: ~65s with the remote build cache warm (1227 of 1676 tasks from cache), ~2m30s with `--no-build-cache`; the ASM scan that follows is ~9s.
The generate step (TS, on the default node-capable agent) downloads `flakiness-plan.json`, substitutes `__GRADLE__` in each batch command, and uploads the batch + analyze steps.
The analyzer then summarises the JUnit XML from the batch jobs.

Why `resolve`/`compile`/`scan` share one step: Buildkite steps run on fresh agents with no shared workspace, and nothing ships `compile`'s `build/classes` output to a separate `scan` step, so on real agents `scan` would find zero compiled classes.
One agent keeps the compiled output on local disk for `scan` and warms the gradle daemon across the invocations.

Why generate is its own step: generate is node, and the gradle-tuned image the orchestration step pins lacks node.
A separate step with no `agents:` pin uses the default node-capable image; it `depends_on` the orchestration step with `allow_failure: true` so a compile-failed (red) orchestration run still triggers generate, which then uploads the analyze-only pipeline that records the single `build_failed`.

Both orchestration steps are keyed under `flakiness-orchestration:` (`:run` and `:generate`), NOT `flakiness-detection:`.
An external metric predicate treats a job as a flakiness test batch iff its `step_key` starts with `flakiness-detection:` and is not `flakiness-detection:analyze`; keying an orchestration step under that prefix would make a red/failed orchestration run get fallback-recorded as `infra_fail`.
Only the actual test batch steps (`KIND_KEYS`, e.g. `flakiness-detection:unit`) and the analyze step (`flakiness-detection:analyze`) keep the `flakiness-detection:` prefix.

```
  ┌───────────┐  refs.json    ┌────────────────────────────────────────────┐   plan.json   ┌──────────────┐
  │ bootstrap │ ───────────▶  │ orchestration  (one gradle agent)          │ ────────────▶ │ generate     │
  │  (step 1) │  FlakinessRef │   resolve ─▶ compile ─▶ scan               │  (artifact)   │ (node agent) │
  └───────────┘               │   (compile failed → buildFailed plan)      │               └──────┬───────┘
                              └────────────────────────────────────────────┘                     │ __GRADLE__ swap
                                                                                                 │ + upload
                                        ┌─────────┐               batch steps + ┌──────────┐    │
                                        │ runners │ ─▶ JUnit XML ─────────────▶ │ analyzer │ ─▶ report
                                        └─────────┘                             └──────────┘
                                     RunnableCommand[]  (plan.commands, binary-substituted)
```

### Module 1: gatherers (was: detectors)

Each gatherer takes an input shape specific to its trigger and emits `FlakinessRef[]`. They no longer classify/resolve - that moved to the Java resolver - so they are tiny and need no repo file listing.

| File                         | Input                                                 | Emits                         | Used by                                         |
|------------------------------|-------------------------------------------------------|-------------------------------|-------------------------------------------------|
| (inline in `pr.ts`)          | `git diff --name-only` paths                          | `changed-file` refs           | `entrypoints/pr.ts`                             |
| `detectors/unmutes.ts`       | Old + new `muted-tests.yml` text                      | `unmute` refs                 | `entrypoints/pr.ts`                             |
| `detectors/explicit-list.ts` | Array of spec strings                                 | `explicit` refs               | `entrypoints/manual.ts`, `entrypoints/local.ts` |

A `FlakinessRef` (defined in `domain.ts`) is one of: `{source:"changed-file", path}`, `{source:"unmute", className, method?}`, or `{source:"explicit", spec}`.

### Module 1b: Java resolver (`build-tools-internal`)

The resolver is split across two Gradle tasks and a plain compile between them, so the compile is a first-class step whose non-zero exit is the sole `build_failed` signal.
`flakinessResolveProject` is a **per-project** task registered in every project with test sources and invoked **unqualified**, so Gradle runs it everywhere and each project decides for itself whether a ref lands in one of its own source sets' `srcDirs`.
Every project - owners and non-owners alike - writes `build/flakiness/project-targets/<project>.json` carrying its resolved targets (often none), its `classDirs` (test source sets **plus `main`**, since abstract test bases live in `main` source sets) and its `dispositions` (per test source set: the output dir, and the `Test` task paths that really run it).
There is no "owns nothing, exit early" shortcut: owning no ref does not make a project irrelevant, because the scan may need to run a subclass compiled there. Removing that shortcut was measured, not assumed - realizing `Test` tasks in all 342 projects that have them (3,201 tasks) is inside run-to-run variance of the old behaviour. See `JAVA_RESOLVER_NOTES.md`.
Each target also carries `runnableTasks`: the **enabled** `Test` tasks whose `testClassesDirs` overlap the owning source set's output - so a project that disables the conventional bare task and points other tasks at the same output (BWC's `v<version>#bwcTest`, packaging's `destructiveDistroTest.*`) resolves to those real tasks instead of a task Gradle would report `SKIPPED`. Targets with nothing runnable carry a precise `skipReason` (`no-runnable-task`, `requires-packaging-host`).
The `compile` step compiles every test source set regardless of what resolve produced; on failure it writes a `buildFailed` `flakiness-plan.json` and `flakiness-precompile.json` and exits non-zero, skipping `scan`.
`flakinessScan` reads the per-project files directly (there is no merge task), folds them back into ref order, decides which class refs *no* project claimed (`unresolved`), ASM-scans the compiled test classes to flatten abstract bases into concrete subclasses (deterministic, capped), does all batching (dedupe, yaml-suite collapse, per-cap slicing), and writes `flakiness-plan.json` - including a `commands` array of ready per-batch Gradle command strings, each carrying the `__GRADLE__` binary placeholder.
It deliberately never reports `UP-TO-DATE` (`doNotTrackState`): the bytecode it reads is an undeclared input (the directories are only known at execution time), so a verdict based on its declared inputs would serve a stale plan across a recompile. Declaring the class dirs instead would make Gradle content-hash ~59k class files (~7s) purely so ASM could re-read them all (~9s), so the task opts out of state tracking rather than pay double.

**Cross-project subclasses.** Because the scan is repo-wide, expanding an abstract base turns up concrete subclasses in *other* Gradle projects - the thing compiling everything was for. They cannot simply inherit the base target's `runnableTasks`: `:app:test` does not contain a class from `:downstream`, so `:app:test --tests com.downstream.DownstreamTests` would run zero tests and be indistinguishable from a hang. Instead each such subclass is **re-homed** onto the source set that really owns it, using the `dispositions` every project reports: it runs as `:downstream:test --tests com.downstream.DownstreamTests`. The lookup key is the compiled-output directory, not the project path, so a subclass in another *source set* of the same project is handled too. If the owning source set has nothing runnable, its own skip reason is carried through.
Each plan entry carries `disposition:"run"|"skip"` (skip → `not_applicable` downstream). See `JAVA_RESOLVER_NOTES.md`.

### Module 2: commands

`commands.ts` is a thin adapter over the Java-produced plan:

1. `planEntryToSkippedTest` — maps a skipped plan entry (with its `reason`) to the `SkippedTest` record written to `flakiness-skipped.json` for the analyze path.
2. `withGradleBinary` — replaces every `__GRADLE__` token with `.ci/scripts/run-gradle.sh` (buildkite) or `./gradlew` (local).
3. `planCommandsToRunnable` — maps the plan's `commands` (`PlanCommand[]`) to `RunnableCommand[]`, applying `withGradleBinary` for the chosen target.

The output is a sequence of `RunnableCommand { kind, label, key, command }`. The `command` is a shell-ready string; the rest is metadata the runner uses to shape its output (BK step keys, log banners, etc.).

All batching and command assembly moved to the Java `flakinessScan` task; TS no longer dedupes, collapses yaml suites, batches by cap, or builds Gradle strings. The only remaining TS concern is target neutrality: swapping the `__GRADLE__` placeholder for the target's gradle binary.

### Module 3: runners

Two implementations, one contract — both consume `RunnableCommand[]`:

- `runners/buildkite.ts` — `toBuildkitePipeline` (pure) produces a Buildkite pipeline structure; `uploadBuildkitePipeline` (impure) serializes to YAML and shells out to `buildkite-agent pipeline upload`. The function appends a final `flakiness-detection:analyze` step that depends on every batch step with `allow_failure: true`, so the report runs even when batches fail.
- `runners/local.ts` — `runLocally` executes each command sequentially via `execSync` with inherited stdio. Returns the worst exit code seen (does **not** stop on first failure — the developer sees all batch results).

REST kinds (`javaRestTest` / `yamlRestTestRunner` / `yamlRestTestSuite` / `yamlRestTestCase`) re-run by looping a Gradle invocation `restIters` times inside `runners/repeat-rest-test.sh`, rather than via `-Dtests.iters` (unit / integ). Each Gradle run overwrites the same `build/test-results/**/TEST-*.xml` in place, so the loop relocates every iteration's XML into `flakiness-iters/iter-<i>/<original path>` after it runs. This keeps the `build/test-results/` path segment intact — so the artifact glob and the analyzer walker still match — while ensuring the analyzer sees *all* re-runs, not just the last. Consequently `Iterations attempted` counts case-executions across every re-run (cases × iterations) for these kinds. (Batch jobs run on fresh agents; local runs rely on the analyzer's mtime filter to ignore snapshots from earlier runs.)

### Module 4: analyzer

Runs **after** the batches complete. Reads JUnit XML written by Gradle (`*/build/test-results/*/TEST-*.xml`), classifies each failure entry, and aggregates per `(class, method)` summaries.

| File                  | Responsibility                                                                                                                                                                                                                                                                                                                                             |
|-----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `analyzer/analyze.ts` | Walk the workspace for JUnit XML, stream-parse via `sax`, classify failures, produce `FlakinessReport`. Streaming keeps peak memory bounded by test count (not file size), so the analyze step survives K8s agents even when a report grows into the hundreds of MiB. Pure; takes an optional `minMtimeMs` to skip pre-existing reports during local runs. |
| `analyzer/render.ts`  | `FlakinessReport → markdown`. `severity()` derives the Buildkite annotation style.                                                                                                                                                                                                                                                                         |

Failure classification (`classifyFailure`):

- `"suite-timeout"` — message matches `Test abandoned because suite timeout was reached.` or `Suite timeout exceeded (>= ...`. Informational; not counted as a real failure.
- `"assertion"` — `AssertionError` and subclasses.
- `"error"` — any other Exception / Error.
- `"other"` — everything else.

This mirrors the failure shapes randomised-runner emits when `@TimeoutSuite` fires.

## Observability

To track pipeline health — and whether improvements (smaller batches, the
never-fail wrapper) actually move the false-positive rate — the pipeline
publishes a structured **outcome** per batch job as a build artifact. An
external metrics pipeline (maintained separately) consumes that artifact and
stores it for dashboards and alerts. This repo only ever uploads a Buildkite
artifact; it never talks to any datastore directly.

### Why publish a structured outcome

The point is to distinguish *why* a job ended the way it did — clean pass,
proven flaky, timeout, hang, or infrastructure failure — which raw Buildkite job
state cannot express. A `state=passed` job could be a genuine clean pass or a
flaky run that we deliberately let pass; a `failed`/`timed_out` job says nothing
about whether a test actually failed versus the agent running out of memory.
Recovering those categories needs the wrapped command's return code plus the
JUnit XML.

`wrapNeverFail` (in `runners/buildkite.ts`) makes this *necessary* as well as
useful: it forces every batch step to `exit 0` so a flaky test never blocks a
PR, which means the job `state`/`exit_status` carry no signal at all — almost
every job looks like `state=passed, exit_status=0`. But even without the
wrapper, the richer taxonomy below would still be worth deriving.

### How it works

1. Each batch job's wrapper captures the wrapped command's return code `rc` and
   wall-clock duration and writes a tiny `flakiness-status/status-<jobId>.json`
   (batch steps only — the `analyze` step does not). It does **no**
   classification: the JUnit XML cannot tell you `rc`/duration, and that is all
   the wrapper contributes.
2. Both the JUnit XML (`*/build/test-results/*/TEST-*.xml`) and the status files
   are uploaded as build artifacts.
3. The `analyze` step (node) downloads the status files, then downloads each
   job's XML per job (`buildkite-agent artifact download ... --step <jobId>`),
   classifies every job with the shared `analyzer/outcome.ts`, and uploads a
   **single** `flakiness-outcomes.json` build artifact whose body is a JSON array
   of per-job payloads. (It also posts the human-readable report as an annotation.)
4. The external metrics pipeline downloads that artifact on build completion,
   merges each payload with job metadata (branch, PR, `web_url`, duration), and
   stores one record per job.

### Payload contract

The `analyze` step uploads a `flakiness-outcomes.json` artifact. Its body is a
JSON array of objects of this shape:

```
{ jobId, stepKey, kind, rc, durationSec, realFailures, suiteTimeouts,
  totalCases, outcome, timedOut, infraSubtype?, failingClasses[] (capped at 50) }
```

### Outcome taxonomy

Derived in priority order by `analyzer/outcome.ts` (`deriveOutcome`):

| outcome         | how it is decided                                                              |
| --------------- | ------------------------------------------------------------------------------ |
| `flaky_detected`| `realFailures > 0` (failing test cases, excluding suite-timeout markers)        |
| `timeout`       | `rc == 124`, or `rc == 137` with duration at/after the inner timeout            |
| `infra_fail`    | `rc == 137` short run (`oom_killed`), a non-zero `rc` with a heap dump (`oom`), or any other non-zero `rc` with no real failures |
| `hang`          | `rc == 0`, zero recorded test cases, and at least one requested task actually ran |
| `clean_pass`    | `rc == 0` with recorded cases and no real failures                              |
| `not_applicable`| two sources, both meaning "nothing to re-run" and both excluded from the false-failure metric. **Upstream** (not by `deriveOutcome`): the resolver found no enabled `Test` task (`no-runnable-task`) or only destructive packaging tasks (`requires-packaging-host`). A re-homed subclass carries whichever of those its *owning* source set reported, not the base target's. Two more come from the resolver: `not-a-test-class` for something a `Test` task cannot address (a helper sharing a test source set, or an inner/anonymous subclass surfaced by bytecode expansion), and `subclass-outside-target-output`, a fallback for a class directory no project claimed a source set for - reachable in principle since `main` outputs are scanned but carry no disposition, though not in practice because `main` cannot depend on a test source set. **By `deriveOutcome`** (`task-skipped`): `rc == 0`, zero test cases, and gradle-runner's `task-status.json` reports *every* task the batch asked for as `SKIPPED` - Gradle's verdict for a task rejected by `onlyIf` or with no source. This is the only way to catch `onlyIf`, which is an execution-time `Spec` the resolver cannot introspect |
| `build_failed`  | assigned upstream when the `compile` orchestration step fails: the PR did not compile, so `scan` was skipped and `generate` uploaded no batches. `analyze` emits one `build_failed` (keyed under `flakiness-orchestration:compile`, not a test batch), excluded from the false-failure metric (the PR is already red from its main build). |

`timedOut` is reported alongside `outcome` so the two timeout shapes stay
distinguishable: a job that times out **with** a real failure is
`flaky_detected` + `timedOut=true` (flakiness proven, so it is not a false
positive), while a job that times out with **no** failing run is `timeout`
(`timedOut=true`) — the false positive we want to drive down.

`infraSubtype` is `oom_killed` (rc 137 + short run, the kernel OOM-killer) or
`oom` (a JVM-heap `OutOfMemoryError`: rc != 0 with a `*/build/heapdump/*.hprof`
file present, detected by the never-fail wrapper; the analyze step does not read
the job log). Finer infra subtypes (disk-full, etc.) would require the job log,
which we currently choose not to read, so they are left unset. Jobs that fail
*before* the wrapper runs (e.g. a pre-command hook failure) write no status file
and so produce no payload; the external pipeline records those as `infra_fail`
from job state. This is where the `flakiness-orchestration:` key split matters:
when the `compile` step fails, `scan` and `generate` are skipped, but because
they are keyed under `flakiness-orchestration:` (not `flakiness-detection:`) the
external batch-job predicate ignores them, so no skipped-batch `infra_fail` noise
is recorded. `generate` still runs (its `depends_on` are `allow_failure`) and
uploads a single `build_failed` record keyed `flakiness-orchestration:compile` -
also outside the batch predicate. The old in-pipeline compile gate keyed under
`flakiness-detection:precompile` (which did produce that skipped-batch noise) has
been removed.

## File layout

```
flakiness-detection/
  README.md
  JAVA_RESOLVER_NOTES.md the B2 rewrite: design, problems, benefits, honest assessment
  domain.ts              types (FlakinessRef, FlakinessPlan, PlanCommand, ClassifiedTest, ...), KIND_* tables, AGENTS/DEFAULT_AGENT_CONFIG
  detectors/
    unmutes.ts           muted-tests.yml diff → unmute refs (parse/diff kept; locate removed)
    explicit-list.ts     spec strings → explicit refs
  commands.ts            planEntryToClassifiedTest + withGradleBinary + planCommandsToRunnable (__GRADLE__ swap)
  runners/
    buildkite.ts         RunnableCommand[] → BK YAML + upload; toResolvePipeline (orchestration [resolve, compile, scan] + separate generate step)
    local.ts             RunnableCommand[] → sequential execSync
    repeat-rest-test.sh  REST-loop wrapper: repeats a Gradle run restIters times, preserves each iteration's XML
  analyzer/
    analyze.ts           JUnit XML → FlakinessReport
    render.ts            FlakinessReport → markdown + severity
    outcome.ts           rc + JUnit counts → outcome taxonomy (deriveOutcome)
  entrypoints/
    pr.ts                bootstrap: gather changed-file + unmute refs → refs.json → upload resolve pipeline
    manual.ts            bootstrap: FLAKINESS_CLASSES → explicit refs → refs.json → upload resolve pipeline
    local.ts             argv driven: refs → flakinessResolveProject → compile tasks → flakinessScan → planCommandsToRunnable → runLocally
    generate.ts          reads flakiness-plan.json → planCommandsToRunnable + upload batches/analyze; folds skip/buildFailed
    analyze.ts           final BK step — classifies each job, uploads outcomes artifact + report annotation

build-tools-internal/.../gradle/internal/flakiness/   (the Java resolver)
  FlakinessResolvePlugin / FlakinessScanTask           root plugin + the scan task
  FlakinessProjectResolve / FlakinessResolveProjectTask per-project self-selecting resolve
  RefResolver / ClassHierarchyScanner / PlanBuilder    pure core (refs→targets, ASM enrichment, plan)
  FlakinessTargets                                     pure fold of the per-project answers
  FlakinessRef / BaseTarget / FlakinessPlan / Kinds    records + wire constants
  FlakinessJson                                        Jackson (de)serialization of the contracts
```

Per-module test files (`*.test.ts`) sit alongside their source. Run with `cd .buildkite && npx vitest run scripts/flakiness-detection`. The Java resolver's unit tests are in `build-tools-internal` (`FlakinessResolverTests`): `./gradlew :build-tools-internal:test --tests "org.elasticsearch.gradle.internal.flakiness.FlakinessResolverTests"`.
