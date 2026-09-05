# Java/Gradle resolver - design notes

This is the running log for the Java/Gradle flakiness *resolution layer* in `build-tools-internal` (`org.elasticsearch.gradle.internal.flakiness`): it replaces the old TypeScript regex/path detectors with an authoritative, Gradle-model-backed resolver, wired into a three-step Buildkite topology.

It is an honest account of the design, what was verified, and the residual risks.

Status legend: [done] implemented + verified, [partial] implemented with caveats, [unverified] written but not executed end-to-end.

## Architecture

**Three** Buildkite steps.
The three Gradle invocations run sequentially inside **one** orchestration step on **one** agent (so the compiled output is shared - see "Why orchestration is one step" below); `generate` is a **separate** step on the default node-capable agent (the gradle image lacks node); the batch + analyze steps are then uploaded by `generate`:

```
1. bootstrap    (TS)     gather refs (git diff / muted-tests.yml diff / FLAKINESS_CLASSES)
                         -> flakiness-refs.json, then upload the [orchestration, generate] group     (contract 1)
2. orchestration (1 step, 1 gradle agent, key flakiness-orchestration:run) runs, in order:
   a. resolve   (Gradle) `flakinessResolveProject`, UNQUALIFIED: it runs in EVERY project that registered it
                         and each project decides for itself whether it owns a ref
                         -> build/flakiness/project-targets/<project>.json  (its resolved targets + classDirs;
                                                                            EVERY project writes one)
   b. compile   (Gradle) PLAIN, UNQUALIFIED `run-gradle.sh compileTestJava compileInternalClusterTestJava
                         compileJavaRestTestJava compileYamlRestTestJava` - every test source set in the repo,
                         reading NOTHING back from resolve.
                         Its non-zero exit is the SOLE build_failed signal (writes buildFailed plan.json + marker).
   c. scan      (Gradle) read the per-project files DIRECTLY (no merge task), fold them back into ref order,
                         ASM-scan the UNION of every project's classDirs, flatten abstract bases, emit ready
                         batch commands
                         -> flakiness-plan.json                                          (contract 2)
3. generate     (TS, separate step, default node agent, key flakiness-orchestration:generate)
                         download+read flakiness-plan.json -> map plan.commands to BK steps -> upload batches + analyze.
                         buildFailed -> upload only the analyze/build_failed record. no plan -> no-op (upstream failed).
```

Boundary held: **Java owns build-model/bytecode facts AND batch-command generation; TS owns Buildkite orchestration + JUnit analysis.**
The two contracts between the layers are `flakiness-refs.json` (gather -> resolve) and `flakiness-plan.json` (scan -> generate; also carrying the ready `commands`).
The only other hand-off is the per-project `build/flakiness/project-targets/` directory (resolve -> scan), which is consumed by Java only and carries no TS type.

**TS/shell now touches none of the intermediate data.** The compile phase used to be the one exception: it runs between resolve and scan, so something had to turn the resolved targets into a task list before scan existed, which the orchestration shell did with `cat .../*.compile-tasks.txt | sort -u`. Compiling everything removes that hand-off entirely - the compile phase is a fixed, unqualified lifecycle-task list that depends on nothing resolve produced. `<project>.compile-tasks.txt`, `FlakinessTargets.compileTaskPaths`, `BaseTarget.compileTaskPath`, `SourceSetInfo.compileTaskPath`, `BaseTarget.outputDir` and the `flakiness-compile-tasks.txt` artifact are all gone with it.

### Why compile everything

A subset compile cannot answer "is this class abstract, and what are its concrete subclasses?" across project boundaries: `ClassHierarchyScanner` can only report a class abstract if it visited that class's own `.class` file, and it can only find a subclass if that subclass's class file is in the scan set. With only the owning projects' output compiled, an abstract base whose subclasses live elsewhere looks childless, and an abstract base that lives in a `main` source set (`org.elasticsearch.test.AbstractBWCSerializationTestCase`, `ESTestCase`) is not even *known*, so `expand` falls through its pass-through branch and yields the abstract class itself as a single "concrete" run - silently wrong.

Compiling everything was measured, not assumed, on a real CI agent (`n4-custom-32-98304`, `/dev/shm` build dir):

| | wall | tasks |
|---|---|---|
| compile all test source sets, remote cache warm | **65s** | 1676 actionable: 431 executed, 1227 from cache |
| same, `--no-build-cache` | **2m30s** | 1676 actionable: 1658 executed |
| ASM scan of the resulting 872 class dirs / ~59k classes | ~9s single-threaded | - |

For comparison the old targeted subset compile took ~76s locally, so on CI the repo-wide compile is not a regression at all. The scan is left single-threaded on purpose: parallelising it measured ~1.7s, but ~9s is a small share of the phase and the sequential code is simpler.

### What compiling everything buys

Repo-wide compile + repo-wide scan makes expansion complete: `expansions.total` is the true number of concrete subclasses, and each one is **run by the tasks that actually execute it**.

A run entry cannot simply inherit the base target's `runnableTasks` - those were chosen by intersecting each `Test` task's `testClassesDirs` with the base's *own* source-set output, so a subclass compiled elsewhere is not in them. Emitting `:app:test --tests com.downstream.DownstreamTests` would run zero tests and look exactly like a hang. So expansion has two cases:

| where the concrete subclass was compiled | outcome |
|---|---|
| the base target's own source-set output | `run` under the base's tasks |
| any other output dir (another project, or another source set of the same project) | **re-homed**: `run` under *that* source set's real tasks |

Re-homing works because every project reports a `SourceSetDisposition` per test source set - its output dir, kind, and the `Test` task paths that run it - and the scan indexes those by output dir (`FlakinessTargets.dispositionsByClassDir`). The scanner records which dir each class came from, so the join is exact. Comparison is on the **directory**, not the project path: a base in `:p/test` with a subclass in `:p/internalClusterTest` shares a project but no `Test` task.

If the owning source set has nothing runnable (bwc-only, packaging host), its own `skipReason` is carried through rather than a new one invented.

### Not every class in a test source set is a test

A test source set holds helpers, fixtures, mock plugins and abstract bases alongside the tests. Two paths turn one of those into a run entry, and both end as a silent zero-test run that `deriveOutcome` scores as `hang` - the failure mode this feature exists to remove:

- a changed non-test file (`TestUtils.java` under `src/test/java`) resolves to a target and emits `--tests TestUtils`;
- expanding an abstract *helper* yields its inner/anonymous subclasses, which are concrete in bytecode, and emits `--tests Foo$1`.

The TypeScript this replaced had the filter in its path regexes (`/^(.+)\/src\/test\/java\/(.+Tests)\.java$/`, `IT` for the rest), so `TestClassNames` restores a dropped behaviour rather than adding a rule. Suffixes come from `TestingConventionsPrecommitPlugin`, taken as the **union** (`Tests`, `IT`) plus `TestCase`: a handful of concrete runnable tests use that suffix, `RollingUpgradeLuceneIndexCompatibilityTestCase` among them, and abstract classes never reach the check because they are expanded rather than run.

The filter is applied where the plan decides what to **run**, not at ref resolution. Filtering at resolution would drop abstract-base refs whose names do not match a suffix, including `AbstractSnapshotBasedRecoveryRestTestCase` - i.e. it would disable cross-project expansion, the feature above.

A rejection is reported as `skip` / `not-a-test-class`, never dropped, so a mis-named real test stays visible instead of turning into a missing check.

Audited over the whole repo's compiled output (897 class dirs): of the concrete descendants of the 450 top-level abstract bases, the filter rejects 7 classes and all 7 are helpers (`WebProxyServer`, `Otlp*Parser`, `*PauseFieldPlugin`, `LocalStateSecurity`). Without it, 5 top-level bases would expand into `$`-bearing names taking 1 to 5 of the 5 capped slots. Bases that are themselves inner classes (87 of 532) cannot be ref targets at all, since a ref's FQCN comes from a source file path or a `muted-tests.yml` entry.

### Blast radius of the repo-wide phases

Two deliberate widenings worth knowing when triaging, neither of which the cost tables above capture:

- **resolve**: `Test`-task realization now happens in every project with a candidate test source set, so a single project whose realization throws breaks every flakiness run, not just runs whose refs touch it. Previously only ref-owning projects realized anything.
- **compile**: a PR that breaks compilation of *any* test source set anywhere is reported `build_failed` by this pipeline even when the refs do not touch that project. Regular CI would fail on it too, so this is desirable, but it is a wider trigger than before. The no-targets guard narrows it: a PR that resolves nothing never compiles at all.

### Why the cheap exit was removed, and what it cost

Re-homing needs the owning source set's `Test`-task model, and the owning project is by definition one no ref pointed at. The old design short-circuited exactly there: a project owning no ref skipped realizing its `Test` tasks. So the shortcut had to go - "owns no ref" does not mean "irrelevant".

That shortcut was justified on the grounds that realizing `tasks.withType(Test)` is expensive (636 tasks in `:qa:packaging` alone) across hundreds of projects. **Measured, it is not.** Unqualified `flakinessResolveProject` over 450 projects, local 12-core host:

| | with cheap exit | full capture everywhere |
|---|---|---|
| resolve, no configuration cache (how CI runs it) | 4.71s | **3.7 - 7.7s, median ~4.3s** |
| resolve, CC store | 5.03s | 4.51s |
| CC entry size | 3.5M | 5.2M |
| `Test` tasks realized | a handful | **3,201 across 342 projects** |

The difference is inside run-to-run variance. Task *realization* is cheap; it is task *execution* that is expensive, and the old note conflated the two. Projects with no candidate test source set still skip realization, since they have nothing a flakiness run could execute.

One real side effect: realizing `Test` tasks everywhere pulls the ES build's per-invocation random test seed (`TestSeedValueSource`) into the configuration-cache key, so a CC entry is never reused across invocations. This does not matter today - **CC is not enabled for this build** (nothing in `.ci/` or `gradle.properties` turns it on, so the production resolve runs without it) - and the mitigation is verified if that changes: pinning `-Dtests.seed` restores reuse (measured 2.04s, "entry reused"). The func tests still run with CC explicitly enabled and pass, so CC compatibility itself is intact.

The scan set must include **`main`**, not just the test source sets - that is where the abstract bases live, and it is also where 36,441 of the 58,712 scanned classes are, i.e. essentially all of the added cost.

### Why orchestration is one step (fixes a latent cross-agent bug)

resolve/compile/scan were originally separate Buildkite steps.
That is **broken on real CI**: Buildkite steps run on fresh agents with no shared workspace, and nothing ships the compile phase's `build/classes` output to a separate scan step - so on real agents `flakinessScan` would find **zero** compiled classes and every enrichment would silently no-op.
(The split only appeared to work in local verification because a single workspace was reused.)
Running resolve/compile/scan in one step on one agent keeps the compiled output on local disk for scan and warms the gradle daemon across the three invocations.
`generate`, by contrast, ships no build output and needs node, so it is its OWN step on the default agent - it downloads `flakiness-plan.json` (uploaded as an artifact by the orchestration step) and reads it.

**Failure attribution (P2) is preserved entirely in-shell**, phase by phase:
- resolve non-zero -> resolver/infra defect, **not** build_failed: no marker, exit rc (the red orchestration step is the signal for pipeline owners; the separate `generate` step then finds no plan and no-ops).
- compile non-zero -> the **sole** build_failed signal: write the buildFailed `flakiness-plan.json` + the `flakiness-precompile.json` marker, then exit rc.
  The separate `generate` step (wired `depends_on` orchestration with `allow_failure: true`) still runs, reads the buildFailed plan, and uploads the analyze-only pipeline that records the single build_failed.
- scan non-zero -> resolver/infra defect, **not** build_failed: no marker, exit rc; `generate` no-ops.
- happy path -> orchestration exits 0; `generate` reads the plan and uploads the batch + analyze steps.

Both orchestration steps are keyed `flakiness-orchestration:*` (not `flakiness-detection:`), so a red/failed orchestration or generate run is never fallback-recorded as a test batch by the external metric predicate (P2a).

## The per-project resolve task (the core of the design)

`flakinessResolveProject` is registered in **every** project with test sources, from `ElasticsearchTestBasePlugin` behind the `-Pflakiness.resolve` gate (a normal build pays nothing), and invoked **unqualified**.
It reads only its own project - no `getRootProject()`/`getAllprojects()`/`getSubprojects()`/`getByPath()`; the repo root comes from `ProjectLayout.getSettingsDirectory()` (`Project.getRootDir()` has an empty ArchUnit baseline) - so the shape is isolated-projects-clean.

Two properties make it work, and they are independent:

### 1. Self-selection (who owns which ref)

Nothing outside Gradle computes which project owns a ref.
Each project asks the question itself: **does a ref's file lie under one of *my* source sets' `srcDirs`?**
That is precisely what `RefResolver` already answers, so the ownership probe *is* the real resolver, run against this project's source-set model with an **empty `Test`-task lookup**:

```java
new RefResolver(repoRoot, List.of(thisProject), path -> List.of(), 0).resolve(refs).targets().isEmpty() == false
```

Two consequences:

- **It is authoritative, and it disambiguates nested projects.** `:x-pack:plugin:logsdb` and `:x-pack:plugin:logsdb:qa:rolling-upgrade` have *nested project directories* but *disjoint `srcDirs`*, so exactly one of them claims a given test file. A directory-prefix or nearest-ancestor-build-script heuristic cannot get this right in general; `srcDirs` can, and needs no settings parsing, no `git ls-files`, and no cross-project access.
- **It used to double as a cheap exit.** `RefResolver` consults the `Test` tasks only *after* it has decided a ref belongs to one of the project's source sets, so a project owning nothing could skip realizing `tasks.withType(Test)` entirely. That shortcut has been **removed** - re-homing a cross-project subclass needs the owning project's `Test` tasks, and the owning project is by definition one no ref pointed at. Measurement showed the shortcut was not buying anything anyway (see "Why the cheap exit was removed").

The probe itself costs a handful of path comparisons per ref, plus (for class refs) one `Files.isRegularFile` probe per java source dir - roughly 2-3k `stat` calls across the whole build, which is noise.

This is what made a caller-side ref->project mapping unnecessary. The alternative considered first was a bootstrap-time node script (`owning-projects.mjs`) that would map refs to projects with a nearest-ancestor-build-script heuristic and then name `:proj:flakinessResolveProject` on the command line. It was prototyped and abandoned before ever being committed: it was wrong in the corners (a directory with a non-`include`d build script, or a project without one), it duplicated resolution logic in a second language, and it would have had to be kept in sync with the Java resolver by hand.

### 2. The model crosses the configuration/execution boundary as a task INPUT

The project's whole model is captured into a single `Provider<String>` set on the task's `@Input`:

```java
Provider<String> modelJson = project.provider(() -> FlakinessJson.writeProjectModel(snapshot(project, refsJson.getOrNull())));
...
t.getProjectModelJson().set(modelJson);   // @Input Property<String>
```

Gradle asks a task-input provider for its execution-time value while **storing** the configuration cache entry, and stores the **computed value** in place of the provider. Two things fall out:

1. **Timing.** The lambda runs *after the entire configuration phase*. Iterating `tasks.withType(Test)` there **realizes** the tasks, which runs every pending `configureEach`/`named` action on them - so `elasticsearch.bwc-test`'s `tasks.named("javaRestTest") { enabled = false }` and its `testClassesDirs = sourceSets.javaRestTest.output.classesDirs` reassignment are both applied, and the whole `v<version>#bwcTest` family exists. There is no "later" left in which the values could change.
2. **Serializability.** The lambda closes over the live `Project`, but by store time it has been replaced by a `String`. The task action touches no Gradle model at all.

`snapshot(project, ...)` reuses the *existing* readers verbatim - `FlakinessProjectModel.sourceSetInfo` and `FlakinessProjectModel.testTaskSnapshot` - and `RefResolver` / `TestTaskSelector` / `PlanBuilder` / `FlakinessJson` are untouched by the topology.

Two `Provider` kinds behave differently here, and both behaviours are what this feature wants:

| provider | encoded as | consequence |
|---|---|---|
| `project.provider { ... }` (the model) | **fixed value** computed at store time | post-mutation model frozen into the entry; not recomputed on reuse |
| `providers.fileContents(refs).getAsText()` **queried inside** that lambda | **value source obtained at configuration time** | a changed `flakiness-refs.json` **invalidates** the entry, so ownership is recomputed |

That second row is deliberate and is a change from the earlier spike. The ownership decision depends on the refs, so the refs *must* be a configuration input: otherwise a reused entry would serve a frozen "I own nothing" verdict for a project the new refs actually touch, and the pipeline would silently resolve nothing for it. Verified below.

### Output layout, and why it is one shared directory

Each project writes **one** file into `<repoRoot>/build/flakiness/project-targets/`, named after its project path (`:x-pack:plugin:logsdb:qa:rolling-upgrade` -> `x-pack.plugin.logsdb.qa.rolling-upgrade.json`): a `ProjectTargetsFile` carrying

- each resolved target plus the **index of the ref that produced it**;
- that project's `classDirs` - its test source sets plus `main` - written **whether or not it resolved anything**, because the scan step unions them into the repo-wide set it ASM-scans.

It deliberately does *not* live in each project's own `build/` directory. The consumer, `flakinessScan`, must find the files without knowing the project set, and a `**/build/flakiness/*.json` glob would mean walking every build output directory in the repo (a `@InputFiles` fingerprint over hundreds of thousands of files). One flat directory makes discovery O(#projects). Each project writes only its own uniquely named file, so no two tasks overlap, and the orchestration `rm -rf`s the directory first so a reused CI workspace cannot leak a previous run's answer.

### No merge task

`flakinessScan` consumes the per-project files directly (`@InputFiles` over `project-targets/*.json`) and does the fold itself via the pure `FlakinessTargets.merge`. Two things genuinely need the global view, and both belong on the *scan* side of the compile step:

- **ref ordering** - each per-project entry carries its ref index, so the merged list reproduces the refs file's order;
- **the `unresolved` verdict** - a class ref is unresolved only if *no* project claimed it. This is why the per-project task deliberately runs the resolver one ref at a time and throws away its own per-ref "unresolved" verdicts: "not in *this* project" is not "not anywhere".

Nothing between resolve and compile needs the merged view any more: the compile phase reads nothing from resolve at all. So there is no phase left for a merge task to occupy. `flakiness-base-targets.json` and `flakiness-compile-tasks.txt` no longer exist.

## Which task actually runs this test? (the disposition layer)

The pipeline used to *assume* the target's task was `:<project>:<kind>` (`:proj:test`, `:proj:javaRestTest`).
Several ES conventions **disable that bare task** and point differently-named `Test` tasks at the *same* source-set output:

| convention | disables | real tasks | pointed at |
|---|---|---|---|
| `elasticsearch.bwc-test` | `javaRestTest` (`enabled = false`) and `test` (`matching {}.configureEach`) | `v<version>#bwcTest` (`StandaloneRestIntegTestTask extends Test`), plus `bcUpgradeTest` | `sourceSets.javaRestTest.output.classesDirs` |
| `elasticsearch.distro-test` (`qa/packaging`) | `test` (`enabled = false`) | `destructiveDistroTest.<distroId>`, `destructiveDistroUpgradeTest.<v>.<distroId>` | `test` source-set output |

Emitting the disabled bare task made Gradle report `SKIPPED`, run 0 tests, exit 0 - and the analyzer record a bogus **`hang`**.
The old code papered over the bwc half with a marker (`hasPlugin("elasticsearch.bwc-test")` -> `skip`, reason `bwc`), which mislabelled real work as not-applicable and did nothing at all for packaging.

That marker is **gone**, replaced by one uniform, convention-free query (`TestTaskSelector`):

> a target is run by the **enabled** `Test` tasks whose `testClassesDirs` overlap the compiled-output
> directory of the source set that owns the class.

Dispositions, in order:
1. the **bare conventional task** (task name == source-set name) is among the enabled candidates -> `run` with just it.
   Today's behaviour, now *derived* rather than assumed.
2. bare disabled/absent, other candidates exist -> `run` with them.
   **This is the new capability: bwc tests are re-runnable for the first time**, as `v<version>#bwcTest` rather than a no-op.
3. only `destructive*` candidates -> `skip`, reason `requires-packaging-host` (policy, see below).
4. no enabled candidate at all -> `skip`, reason `no-runnable-task`.

`BaseTarget`/`PlanEntry` therefore lost `bwc` and gained `runnableTasks` (+ `candidateTasks` / `skipReason` on the base target).
`CommandBuilder` builds the gradle invocation from `runnableTasks`, so no layer of the pipeline constructs a task path from a convention any more.
Compile-task and ASM-scan-dir derivation likewise now key off "is runnable", not "is bwc".

### The lifecycle crux: a LATE read, at configuration-cache store time

`enabled` and `testClassesDirs` are **mutated after** the resolve task is registered (the bwc/distro plugins may be applied later in the build script), and the `v<version>#bwcTest` / `destructiveDistroTest.*` families are **registered** later still.
Snapshotting them eagerly - in a `configureEach` callback, or anywhere during plain configuration - reads pre-mutation values and silently produces the wrong answer.

The store-time provider (above) is what makes the read late enough, and it is late by *construction*, not by convention: Gradle only asks for the value once the configuration phase is over and the task graph is computed. Direct proof, by line number in an `--info` log of a storing run of the real build:

```
line   27 .. 1906 : "Evaluating project ..."   (532 lines - the whole build configures)
line 1907        : "All projects evaluated."
line 1910        : "Tasks to be executed: [task ':benchmarks:flakinessResolveProject', ...]"
line 1939        : "flakiness: capturing model for :libs:dissect (realizing its Test tasks)"
line 2021        : "flakiness: capturing model for :qa:packaging (realizing its Test tasks)"
line 2392        : "flakiness: capturing model for :x-pack:plugin:logsdb:qa:rolling-upgrade (realizing its Test tasks)"
line 3249        : "> Task :libs:dissect:flakinessResolveProject"
line 7597        : "Configuration cache entry stored."
```

Strictly after every project is evaluated, strictly before any task action.

### Fan-out cap

A bwc project registers one task per wire-compatible version - **68 candidates** for `:x-pack:plugin:logsdb:qa:rolling-upgrade` (67 `v<version>#bwcTest` + `bcUpgradeTest`), each booting a real multi-node cluster - so the fan-out is capped at **2** by default (`-Pflakiness.taskCap`).
Candidates are ordered **newest-first**: a numeric-aware ("natural") comparison of the task name, descending.
That prefers the newest versions, orders `v8.19.10` *above* `v8.19.9` (plain lexicographic would not), and is a total order over unique task names - so the selection is reproducible regardless of registration order (asserted in `TestTaskSelectorTests`).
What was dropped is recorded in the plan's `taskSelections[]`, mirroring `expansions[]`: `selected 2 of 68 candidate tasks (cap 2)`.

### Packaging policy (an AGENT-CAPABILITY decision, not a model fact)

`destructiveDistroTest.*` / `destructiveDistroUpgradeTest.*` are classified `skip` / `requires-packaging-host`.
The **model says they are runnable** - they are enabled and they do run those classes.
They are excluded because they install/remove packages and mutate the host, so they need a dedicated ephemeral packaging host (AGENTS.md: "do not run packaging suites on your workstation"), which the standard flakiness agent is not.
The `destructive` task-name prefix is the ES-wide marker for exactly that property (the `destructive*` tasks run against the local host; their non-destructive wrappers delegate to a throw-away VM).
This is stated in `TestTaskSelector`'s javadoc as a policy, and a non-destructive candidate always wins over destructive ones.
If a packaging-capable agent is ever wired up, the policy - not the model - is what changes.

### Multi-task command shape

The batching unit is an **(entry, task path) pair**.
An entry with N runnable tasks contributes N units, which are then batched per kind exactly as before.
Consequences:
- `javaRestTest` has batch cap 1, so the capped bwc set becomes **one Buildkite step per bwc version** - clean per-task attribution for the analyzer (a genuine incompatibility with one old version does not get mixed into another version's runs);
- unit/integ kinds keep their existing multi-task-per-invocation batching (they already put several *projects'* tasks in one gradle invocation), so nothing regresses for the common case.

## PROBLEMS

### P0 (RESOLVED, and the claim that motivated it was WRONG) - the configuration cache

The earlier root-task topology had a single root `flakinessResolve` reading a cross-project model that every test project pushed into a `FlakinessModelService` (a `BuildService`) during its own configuration. Under the configuration cache that task saw an **empty** model, so the step had to run `--no-configuration-cache`.

The explanation recorded at the time was **wrong**, and it survived in code comments and an ArchUnit baseline comment until this migration. It said CC "only configures the projects reachable from the requested root task". It does not: **CC does not imply configuration-on-demand.** Measured on this repo, a CC storing run emits **532 `Evaluating project` lines** - the whole build configures, exactly as without CC. The data was there; it just could not cross the boundary.

The actual mechanism is a lifecycle mismatch: under CC a `BuildService` is **re-instantiated at execution time from its serializable `Parameters`**, so any state accumulated into it during configuration is discarded. Without CC, configuration and execution share one build session and therefore one service instance, which is the only reason the fan-in worked at all. Confirmed independently of *which* projects configure: with `--configuration-cache --rerun-tasks`, the old `flakinessResolve` failed with "FlakinessModelService is empty" even when `:libs:dissect:help` **and** `:libs:dissect:compileTestJava` were requested alongside it.

The whole fan-in is now **deleted** (`FlakinessModelService`, `FlakinessProjectModel.contribute`, `FlakinessResolveTask`, the `flakinessResolve` registration). `FlakinessResolveTask`'s empty-model fail-fast guard moved rather than vanished: `flakinessScan` now refuses to write a plan when it finds no per-project output but does have refs to resolve, which is the same "prove the hand-off happened" check one stage later. Task inputs, not shared service state, carry the model. `--no-configuration-cache` is gone from `runners/buildkite.ts`, `entrypoints/local.ts`, the func test, and the docs.

### P1 - Config-pass cost x3
The pipeline runs three Gradle invocations (resolve, compile, scan) on one agent, so the gradle daemon stays warm across them. resolve and scan both apply `-Pflakiness.resolve`, so each configures the whole build (scan does not need the model, only its own file inputs). compile is a plain invocation, so it configures nothing extra.
What CC buys is that a *repeat* resolve costs nothing (see the timing table); it does **not** reduce the cost of the first, storing run. Getting to "configure only the owning projects" needs isolated projects or configuration-on-demand, which is separate, much larger work.

### P2 - Failure attribution (in-shell in the orchestration step; generate is a separate step)
Only the **compile** phase's non-zero exit means `build_failed`: the orchestration shell writes `{"buildFailed":true,"reason":"precompile"}` into `flakiness-plan.json` + the `flakiness-precompile.json` marker, then exits non-zero.
A failure in the **resolve** or **scan** phase is a resolver/tool/infra defect and is NOT reported as `build_failed` - the shell exits non-zero without writing a marker, so the orchestration step just goes red and reads downstream as an infra/pipeline problem.
The separate `generate` step is wired `depends_on` orchestration with `allow_failure: true`, so it always runs: on a compile failure it reads the buildFailed plan and uploads the analyze-only pipeline that records the single `build_failed`; on a resolve/scan failure it finds no plan and no-ops cleanly (logs, exit 0, uploads nothing) rather than erroring.

### P2a - Step-key namespacing
The external metric treats a job as a flakiness test-batch job iff `step_key.startsWith("flakiness-detection:") && step_key !== "flakiness-detection:analyze"`.
So **both** orchestration steps are keyed under `flakiness-orchestration:` (`:run`, `:generate`), NOT `flakiness-detection:`.
Otherwise a red/failed/skipped orchestration or generate run would be fallback-recorded as a test batch.
Only the actual test batch steps (`flakiness-detection:unit` etc.) and `analyze` (`flakiness-detection:analyze`) keep the `flakiness-detection:` prefix.
`analyze.ts`'s synthetic `build_failed` payload is likewise keyed under `flakiness-orchestration:`.

### P3 - Class-ref resolution still needs a filesystem probe
Unmute/explicit refs carry only an FQCN; mapping it to a source set means checking where `<pkg>/<Name>.java` exists on disk under the source set's real `javaSrcDirs`.
The model gives authoritative source *dirs*, not the file inventory, so a disk probe per candidate root is unavoidable.
Under self-selection every project runs that probe over its own source dirs (rather than one project running it over all of them). Same total work, now parallel and CC-cached.

### P7 - `Test`-task realization cost
Reading the task facts *requires realizing* the project's `Test` tasks, which runs arbitrary task-configuration code (testClusters wiring, distribution resolution, ...).
Mitigations: it happens only under `-Pflakiness.resolve`, only at configuration-cache store time, and only in projects that have a candidate test source set. It is no longer bounded to ref-owning projects - see "Why the cheap exit was removed" for why, and for the measurement showing the fan-out is free.
Realizing them for all ~450 projects would be indefensible; realizing them for 1-3 is cheap (measured below: the whole unqualified run is no slower than the old single root task).

Realizing `Test` tasks inside a store-time provider is **permitted** - no error, no CC problem, no deprecation, on 708 `Test` tasks across the three probe projects (`:qa:packaging` alone realized 637). This was the main uncertainty going in, and it simply worked. It is still arbitrary user code running late in the build, so a pathological project could in principle throw there; the failure would be a red resolve step, not a wrong answer.

### P8 (solved runner-side) - `onlyIf` predicates are invisible
`enabled` is a plain property we can read; **`onlyIf` is a `Spec<Task>` evaluated by Gradle at execution time** and cannot be introspected (and must not be speculatively evaluated - the predicates close over live task state).
Two live cases:
- `elasticsearch.bwc-test`: `onlyIf("BWC tests enabled") { project.bwc_tests_enabled }` on every `v<version>#*` task;
- `DistroTestPlugin`: `onlyIf { distribution.getArchitecture() == Architecture.current() }` - which is why the spike saw *all* `destructiveDistroTest.*` tasks reporting `enabled = true` on an aarch64 host, including the x86-only ones.

**How it is handled:** not in the resolver, deliberately - and no longer at all necessary there.
A task that passes `enabled` but is skipped by `onlyIf` runs 0 tests, which used to read as a `hang`:
- the packaging `onlyIf` is moot, because the packaging policy skips those tasks anyway;
- `bwc_tests_enabled` is true in normal CI/dev builds (it is only flipped off during release surgery), so the bwc `onlyIf` passes whenever the flakiness pipeline runs.
  Reading `project.bwc_tests_enabled` would fix the known case but would re-introduce exactly the per-convention special-casing this feature deleted, for a predicate that is almost always true.

**The gap is now closed runner-side, which covers every `onlyIf` rather than the ones we can name.**
`.ci/scripts/run-gradle.sh` already routes every CI invocation through `gradle-runner` - a Gradle **Tooling API** client - and its `TaskTracker` records each task's outcome in `build/task-status.json`. The batch wrapper copies that report next to its per-job status file and carries the task paths the batch invoked (`PlanCommand.taskPaths`, so they are the resolver's own authoritative paths rather than a regex over the command string); `analyze.ts` parses both and, when **every** requested task came back `SKIPPED`, records `not_applicable` with reason `task-skipped` instead of `hang`.

Verified against Gradle 9 with a purpose-built fixture:

| fixture | gradle console | `task-status.json` |
| --- | --- | --- |
| `onlyIf { false }` | `SKIPPED` | `SKIPPED` |
| `enabled = false` | `SKIPPED` | `SKIPPED` |
| `Test` + `onlyIf { false }` | `SKIPPED` | `SKIPPED` |
| `Test`, no source | `NO-SOURCE` | `SKIPPED` |
| normal task | executed | `SUCCESS` |

So `onlyIf` rejection is reported as `SKIPPED`, **not** `NOT_RUN` (which `TaskTracker` reserves for tasks that emitted no events at all), and `NO-SOURCE` collapses into `SKIPPED` too because `TaskTracker` discards `TaskSkippedResult.getSkipMessage()`. One check therefore covers the whole family.

Two deliberate constraints:
- **Scoped to the batch's own task paths.** A healthy build contains unrelated `SKIPPED` entries (a `processResources` with no resources), so an unscoped check would mislabel the muted-tests case - task ran, filter matched nothing - as `not_applicable`.
- **Requires *all* requested tasks to be skipped.** If even one really ran, a zero-test result is not explained by `onlyIf` and stays a `hang`.

For REST kinds `repeat-rest-test.sh` loops the gradle invocation and each iteration overwrites `task-status.json`, so the verdict is the **last** iteration's. That is sound because `onlyIf` predicates are static for the life of a job; they do not flip between iterations seconds apart.

Option (b), asking Gradle for a dry-run of the task graph, was not needed.

## Review-driven refinements

- **Clear error when `flakiness-refs.json` is missing.** `refsJson` is `@Optional` on both tasks; when the file is absent the action throws a `GradleException` naming the path and telling the operator that the gather/bootstrap step is expected to have written it (or to pass `-Pflakiness.refs=<path>` for a standalone run), instead of Gradle's "property 'refsJson' doesn't have a configured value".
- **No `afterEvaluate`, no cross-project access.** `GradlePluginConventionsArchUnitSpec` and `IsolatedProjectsArchUnitSpec` both pass.
- **Java owns batch-command generation.** The batching that used to live in the TS `commands.ts` (dedupe, collapse-yaml-suites, dedup-runners, cap-batching, per-kind command strings, the repeat-rest wrapper) lives in `CommandBuilder`, and the scan task attaches the ready batch commands to `flakiness-plan.json`'s `commands` array.
  Each command is **target-neutral**: it carries the literal `__GRADLE__` placeholder where the gradle binary belongs (both plain invocations and inside `repeat-rest-test.sh <iters> __GRADLE__ <tasks>`), which the thin TS runner layer replaces with `.ci/scripts/run-gradle.sh` (CI) or `./gradlew` (local).
  The `FLAKINESS_ITERS` override flows to Java: the plugin reads `-Pflakiness.iters` (set by `local.ts` for `--iters`) or the `FLAKINESS_ITERS` env var (carried in the CI build env, so the manual pipeline's override keeps working with **no yml change**).
- **Quieter annotations.** Abstract expansions are logged to console only (they are already in `plan.json`); an unresolved-refs `warning` annotation is emitted only when the list is non-empty; the always-on `info` "Flakiness resolver" annotation is gone.

## BENEFITS

- **Fully authoritative model.** Project boundaries, source-set shape (real `srcDirs`), compiled output locations (real `outputDir`), the compile task paths (real `compile<Ss>Java`), the **tasks that actually re-run a target** (real `Test` tasks: `enabled` + `testClassesDirs`), and now **which project owns a ref** all come from each project's live configured model - not path conventions, build.gradle regexes, plugin markers, or a node-side heuristic.
- **Abstract detection + subclass expansion via bytecode.** `ClassHierarchyScanner` reads `ACC_ABSTRACT` + the super-class chain off the compiled `.class` files (ASM, already on the classpath), so an unmuted abstract base deterministically expands to its concrete subclasses (sorted FQCN, capped). TS cannot do this.
- **No batch steps until compile succeeds.** The batch steps are only *created* by `generate`, which runs after a successful orchestration. A PR that does not compile uploads only the analyze/build_failed record, so the skipped-batch `waiting_failed` metric noise structurally cannot occur.
- **Task disposition is derived, not assumed.** The bare-task assumption is gone from every layer. bwc targets are genuinely re-run (as `v<version>#bwcTest`); packaging targets are skipped with a reason that is *true* (`requires-packaging-host`); a source set with no enabled `Test` task says exactly that (`no-runnable-task`).
- **The configuration cache is on.** Resolve and scan both store an entry with 0 problems and both reuse it. No `--no-configuration-cache` anywhere in the pipeline.

## PROOF: real-build run of the whole flow, configuration cache ENABLED

`flakiness-refs.json` covering all three cases (bwc, packaging, ordinary unit test):

```json
{ "mergeBase": "hand-written-proof",
  "refs": [
    { "source": "unmute", "className": "org.elasticsearch.xpack.logsdb.LogsdbIndexingRollingUpgradeIT" },
    { "source": "changed-file", "path": "qa/packaging/src/test/java/org/elasticsearch/packaging/test/ArchiveTests.java" },
    { "source": "changed-file", "path": "libs/dissect/src/test/java/org/elasticsearch/dissect/DissectParserTests.java" } ] }
```

### (a) resolve - unqualified, every project, correct results

```
$ rm -rf build/flakiness .gradle/configuration-cache flakiness-plan.json
$ ./gradlew -Pflakiness.resolve --configuration-cache flakinessResolveProject
Calculating task graph as no cached configuration is available for tasks: flakinessResolveProject
flakiness resolve[:libs:dissect]: 3 refs -> 1 targets (model: 1 source sets, 1 Test tasks)
  ref[2] test org.elasticsearch.dissect.DissectParserTests -> [:libs:dissect:test]
flakiness resolve[:qa:packaging]: 3 refs -> 1 targets (model: 1 source sets, 637 Test tasks)
  ref[1] test org.elasticsearch.packaging.test.ArchiveTests -> skip (requires-packaging-host)
flakiness resolve[:x-pack:plugin:logsdb:qa:rolling-upgrade]: 3 refs -> 1 targets (model: 2 source sets, 70 Test tasks)
  ref[0] javaRestTest org.elasticsearch.xpack.logsdb.LogsdbIndexingRollingUpgradeIT -> [:x-pack:plugin:logsdb:qa:rolling-upgrade:v9.6.0#bwcTest, :x-pack:plugin:logsdb:qa:rolling-upgrade:v9.5.1#bwcTest]

BUILD SUCCESSFUL in 3s
468 actionable tasks: 450 executed, 18 up-to-date
Configuration cache entry stored.
```

All three probe cases:
- **bwc** - the disabled bare `:x-pack:plugin:logsdb:qa:rolling-upgrade:javaRestTest` appears nowhere; the two newest `v<version>#bwcTest` tasks do (`candidateTasks: 68`);
- **packaging** - 636 `destructiveDistroTest.*`/`destructiveDistroUpgradeTest.*` candidates found, entry is `skip` / `requires-packaging-host`;
- **ordinary project** - `:libs:dissect` resolves to the plain enabled bare task.

The 447 other projects each wrote `{"projectPath": "...", "resolved": [], "classDirs": [...]}` - an empty share, but still their class dirs, which is what makes the scan repo-wide.

### (b) compile - a fixed task list, and scan -> the plan

```
$ ./gradlew compileTestJava compileInternalClusterTestJava compileJavaRestTestJava compileYamlRestTestJava
BUILD SUCCESSFUL

$ ./gradlew -Pflakiness.resolve --configuration-cache flakinessScan
> Task :flakinessScan
flakiness plan: 3 refs -> 3 targets (from 450 project files), 3 entries, 3 commands, 0 expansions, 0 unresolved -> .../flakiness-plan.json
  javaRestTest :x-pack:plugin:logsdb:qa:rolling-upgrade org.elasticsearch.xpack.logsdb.LogsdbIndexingRollingUpgradeIT -> [:x-pack:plugin:logsdb:qa:rolling-upgrade:v9.6.0#bwcTest, :x-pack:plugin:logsdb:qa:rolling-upgrade:v9.5.1#bwcTest]
  test :qa:packaging org.elasticsearch.packaging.test.ArchiveTests -> skip (requires-packaging-host)
  test :libs:dissect org.elasticsearch.dissect.DissectParserTests -> [:libs:dissect:test]

BUILD SUCCESSFUL in 4s
Configuration cache entry stored.
```

The resulting `flakiness-plan.json` is **byte-identical** to the one the old root-task + merge-task flow produced (`diff` clean), including the one command per capped bwc version:

```json
  "commands" : [ {
    "kind" : "test", "label" : "unit tests", "key" : "flakiness-detection:unit",
    "command" : "__GRADLE__ -Dtests.iters=100 -Dtests.timeoutSuite=3600000! :libs:dissect:test --tests org.elasticsearch.dissect.DissectParserTests"
  }, {
    "kind" : "javaRestTest", "label" : "java rest tests", "key" : "flakiness-detection:java-rest",
    "command" : ".buildkite/.../repeat-rest-test.sh 10 __GRADLE__ :x-pack:plugin:logsdb:qa:rolling-upgrade:v9.6.0#bwcTest --tests org.elasticsearch.xpack.logsdb.LogsdbIndexingRollingUpgradeIT --rerun"
  }, {
    "kind" : "javaRestTest", "label" : "java rest tests", "key" : "flakiness-detection:java-rest",
    "command" : ".buildkite/.../repeat-rest-test.sh 10 __GRADLE__ :x-pack:plugin:logsdb:qa:rolling-upgrade:v9.5.1#bwcTest --tests org.elasticsearch.xpack.logsdb.LogsdbIndexingRollingUpgradeIT --rerun"
  } ]
```

### (c) `Configuration cache entry stored` with 0 problems, and reused on a repeat run

From the machine-readable CC report (`totalProblemCount` in `configuration-cache-report.html`) of every storing run:

```
problems=0 action=storing  tasks=flakinessResolveProject
problems=0 action=storing  tasks=flakinessScan
```

Reuse, with the outputs deleted (not `--rerun-tasks`, which would perturb the measurement):

```
$ rm -rf build/flakiness
$ ./gradlew -Pflakiness.resolve --configuration-cache flakinessResolveProject
Reusing configuration cache.
flakiness resolve[:libs:dissect]: 3 refs -> 1 targets (model: 1 source sets, 1 Test tasks)
  ref[2] test org.elasticsearch.dissect.DissectParserTests -> [:libs:dissect:test]
flakiness resolve[:qa:packaging]: 3 refs -> 1 targets (model: 1 source sets, 637 Test tasks)
  ref[1] test org.elasticsearch.packaging.test.ArchiveTests -> skip (requires-packaging-host)
flakiness resolve[:x-pack:plugin:logsdb:qa:rolling-upgrade]: 3 refs -> 1 targets (model: 2 source sets, 70 Test tasks)
  ref[0] javaRestTest org.elasticsearch.xpack.logsdb.LogsdbIndexingRollingUpgradeIT -> [:x-pack:plugin:logsdb:qa:rolling-upgrade:v9.6.0#bwcTest, :x-pack:plugin:logsdb:qa:rolling-upgrade:v9.5.1#bwcTest]

BUILD SUCCESSFUL in 710ms
450 actionable tasks: 450 executed
Configuration cache entry reused.
```

`450 actionable tasks: 450 executed` - the tasks genuinely **ran** (no `UP-TO-DATE`, no `NO-SOURCE`), with identical results, and the per-project files were rewritten byte-for-byte.

**Refs-change invalidation (the one staleness surface, closed).** With an entry stored for the 3 refs above, appending a 4th ref owned by a *different* project:

```
$ ./gradlew -Pflakiness.resolve --configuration-cache flakinessResolveProject
Calculating task graph as configuration cache cannot be reused because file 'flakiness-refs.json' has changed.
flakiness resolve[:libs:grok]: 4 refs -> 1 targets (model: 1 source sets, 1 Test tasks)
  ref[3] test org.elasticsearch.grok.GrokTests -> [:libs:grok:test]
```

The newly-owning project self-selects **in**, because reading the refs inside the model provider makes them a configuration input. A frozen ownership verdict would have been a silent false negative; it is not possible.

### (d) TIMING - the main risk of self-selection, measured

Warm daemon, same machine, same 3 refs, 3 runs each. "Old" is the deleted root `flakinessResolve` (which *had* to run `--no-configuration-cache`); "new" is the unqualified per-project task across all 450 projects.

| | run 1 | run 2 | run 3 |
|---|---|---|---|
| OLD root `flakinessResolve`, `--no-configuration-cache` | 4.95s | 4.62s | 5.37s |
| NEW unqualified `flakinessResolveProject`, CC **storing** | 5.37s | 4.40s | 4.92s |
| NEW unqualified `flakinessResolveProject`, CC **reused** | 2.52s | 2.53s | 2.61s |

(wall clock including JVM/daemon handshake; `BUILD SUCCESSFUL in` reports 3-4s storing, ~1s reused.)

**Running the task in 450 projects costs nothing measurable.** The whole-build configuration pass dominates completely, and it happened in the old topology too. This remains true now that every project captures its full model: 3,201 `Test` tasks realized across 342 projects lands inside run-to-run variance (see "Why the cheap exit was removed").

### (e) historical: the cheap exit, and why it was dropped

From the `--info` log of a storing run across all 450 projects:

```
$ grep -c "owns no ref; skipping Test-task realization"    -> 447
$ grep    "realized .* Test tasks"
flakiness: :libs:dissect realized 1 Test tasks
flakiness: :x-pack:plugin:logsdb:qa:rolling-upgrade realized 70 Test tasks
flakiness: :qa:packaging realized 637 Test tasks
```

Exactly the three owning projects realize anything; `:qa:packaging` realizes its 637 `Test` tasks **only because it owns a ref**. And, from the same log, `grep -c "Evaluating project"` -> **532**: the whole build still configures under CC, which is the measurement that disproves the old "CC only configures the reachable projects" claim.

## Verification (what was actually run)

- **Java unit tests - PASS.** `:build-tools-internal:test --tests "*ArchUnitSpec*" --tests "*flakiness*"` -> 68 tests, 0 failures:
  `ConfigurationCacheArchUnitSpec` 3, `GradleApiUsageArchUnitSpec` 2, `GradlePluginConventionsArchUnitSpec` 4, `IsolatedProjectsArchUnitSpec` 14, `LoggingArchUnitSpec` 2, `TaskModellingArchUnitSpec` 6, `CommandBuilderTests` 11, `FlakinessPerProjectJsonTests` 2, `FlakinessResolverTests` 7, `FlakinessScanTaskTests` 1, `FlakinessTargetsTests` 5, `TestTaskSelectorTests` 11.
  `FlakinessTargetsTests` replaces `FlakinessResolveTaskTests` and additionally pins the fold: ref-order restoration across projects, the global `unresolved` verdict (class refs only), and dedupe of the same identity claimed twice.
- **`IntegTestCoverageArchUnitSpec` - PASS (5 tests).** Including "no new AbstractGradleInternalPluginFuncTest subclass disables configuration cache" and "the cc-incompatible baseline contains no stale entries" - the latter is what forced the `FlakinessResolvePluginFuncTest` entry (and its now-wrong explanatory comment) out of `KNOWN_CC_INCOMPATIBLE`.
- **Func test - PASS (4 tests), with the configuration cache ENABLED** (`disableConfigurationCache` is gone). `FlakinessResolvePluginFuncTest` builds a **five**-project fixture and runs the unqualified resolve -> repo-wide compile -> scan. It asserts: every project ran the task; the owning projects each wrote their own share; `:untouched` resolved **nothing** yet still reported a full model (non-empty `sourceSets`, a captured `test` task), its `classDirs` and its `dispositions` - the cheap exit is gone; the class-dir union spans all five projects and includes `main`; the adversarial `:bwcish` fixture - which disables the bare task through `matching {}.configureEach {}` and registers three alternatives **after** the resolve task is registered - captures the post-mutation state (`test.enabled == false`, 3 `#altTest` tasks) and resolves to `[v9.6.0#altTest, v9.5.1#altTest]`, never `:bwcish:test`; `Configuration cache entry stored`; and the target-neutral (`__GRADLE__`) batch commands. **The cross-project regression test:** `:downstream` holds a third concrete subclass of `:app`'s abstract base, so `expansions[0].total == 3` (a subset scan would have found 2), and that subclass is **re-homed**: `gradleProject: :downstream`, `disposition: run`, `runnableTasks: [:downstream:test]`, and the emitted command is `:downstream:test --tests com.downstream.DownstreamTests` - never under `:app:test`. A fourth test pins that a class ref no project owns is reported `unresolved` exactly once by the scan step.
- **Real-build end-to-end - PASS.** See PROOF above; `flakiness-plan.json` byte-identical to the old flow's.
- **TS suite - PASS.** `cd .buildkite && npx vitest run scripts/flakiness-detection` -> 11 files / 130 tests. The compile-phase test now asserts the fixed unqualified task list and that **none** of the old glue survives (no `.compile-tasks.txt`, no `$$TASKS`, no empty-list branch), plus: unqualified `flakinessResolveProject`, no `--no-configuration-cache`, and the `rm -rf build/flakiness/project-targets` hygiene step.
- **`:build-tools-internal` compiles (main + test + integTest) - PASS.**

### NOT run (per brief / environmental)
- The full ES build.
  `:build-tools-internal:spotlessJavaCheck` cannot be *created* in this environment ("You need to add a repository containing google-java-format:1.19.2"), but `spotlessApply` runs and was used to format the sources; note it targets `src/main/java` only, so the test sources are outside its scope.
- The Buildkite pipeline on a real agent (the yml + dynamic uploads are written and the pure `toResolvePipeline`/`toBuildkitePipeline` structure is unit-tested).

## Honest assessment

- The design is now **smaller and less subtle** than what it replaced. Deleted: `FlakinessModelService` (a `BuildService` holding live `Project` references from configuration into execution), `FlakinessResolveTask` (the root fan-in), `FlakinessProjectModel.contribute` (the `configureEach` push + late-read `Supplier` registration), the `flakiness-base-targets.json` intermediate, and every `--no-configuration-cache` flag. Never committed, and abandoned during the design: a separate root merge task (its job is now the pure `FlakinessTargets` fold) and `owning-projects.mjs` (a second, heuristic implementation of ref->project mapping in another language). Added: the ownership probe (one reuse of `RefResolver`), `FlakinessTargets` (a pure fold), and a second output file per project.
- The **riskiest** remaining part is P8: `onlyIf` is invisible, so a task that is `enabled` but skipped at execution still yields a 0-test `hang`. The feature shrinks that hole a lot without closing it, and the right fix is runner-side, not resolver-side.
- **Staleness surface.** On CC reuse the model is served from the entry, not recomputed. That is sound because everything that could change it - build scripts, `gradle.properties`, project properties, version files read at configuration time, and now `flakiness-refs.json` itself - is a CC input and invalidates the entry. The refs case is verified above; it is the only one specific to this feature.
- **CC reuse across PRs is near-zero**, because the refs differ. Reuse is near-total for the local/dev loop and for re-runs of the same PR. On a cold CI agent this topology costs the same as the old one (the timing table's "storing" row); it is just no longer broken, and no longer needs a flag to stay unbroken.
- Residual risks: (1) the `rest-api-spec/test/<suitePath>.yml` yaml-suite layout is still encoded as a constant (an `ESClientYamlSuiteTestCase`-wide convention, not a per-project layout assumption, so low risk); (2) the per-project outputs live in one shared `build/flakiness/project-targets/` directory rather than each project's own build dir - a deliberate trade for cheap discovery, documented in `FlakinessProjectResolve.TARGETS_DIR`, with `rm -rf` hygiene in the orchestration shell so a reused workspace cannot leak; (3) `Test`-task realization runs arbitrary user code inside a store-time provider (P7) - a pathological project would make the resolve step red, not wrong; (4) the packaging skip is a policy about *this agent*, so it will read as a wrong answer the day a packaging-capable agent exists - which is why it is a named constant with a documented rationale.
