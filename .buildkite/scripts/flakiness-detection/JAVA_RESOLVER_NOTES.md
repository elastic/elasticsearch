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
                         -> build/flakiness/project-targets/<project>.json             (its resolved targets)
                         -> build/flakiness/project-targets/<project>.compile-tasks.txt (its compile tasks)
   b. compile   (Gradle) PLAIN `run-gradle.sh $(cat .../*.compile-tasks.txt | sort -u)`.
                         Its non-zero exit is the SOLE build_failed signal (writes buildFailed plan.json + marker).
   c. scan      (Gradle) read the per-project files DIRECTLY (no merge task), fold them back into ref order,
                         ASM-scan the LOCAL compiled output, flatten abstract bases, emit ready batch commands
                         -> flakiness-plan.json                                          (contract 2)
3. generate     (TS, separate step, default node agent, key flakiness-orchestration:generate)
                         download+read flakiness-plan.json -> map plan.commands to BK steps -> upload batches + analyze.
                         buildFailed -> upload only the analyze/build_failed record. no plan -> no-op (upstream failed).
```

Boundary held: **Java owns build-model/bytecode facts AND batch-command generation; TS owns Buildkite orchestration + JUnit analysis.**
The two contracts between the layers are `flakiness-refs.json` (gather -> resolve) and `flakiness-plan.json` (scan -> generate; also carrying the ready `commands`).
The only other hand-off is the per-project `build/flakiness/project-targets/` directory (resolve -> compile/scan), which is consumed by shell/Java only and carries no TS type.

**There is exactly one place where TS/shell touches the intermediate data**, and it is a `cat`: the compile phase runs between resolve and scan, so someone has to turn the resolved targets into a task list before scan exists. Each project writes its own newline-terminated `<project>.compile-tasks.txt`, and the orchestration shell concatenates them (`cat .../*.compile-tasks.txt | sort -u`). No JSON parsing in shell, no extra Gradle task, no extra phase.

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
- **It is the cheap exit.** `RefResolver` consults the `Test` tasks only *after* it has decided a ref belongs to one of the project's source sets. So a project that owns nothing never calls the lookup, never realizes `tasks.withType(Test)` - the expensive part - and emits an empty model (`ownsRefs: false`) and an empty result. Measured on the real build: **3 projects realized `Test` tasks (1 + 70 + 637 = 708), 447 projects cheap-exited.**

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

Each project writes two files into `<repoRoot>/build/flakiness/project-targets/`, named after its project path (`:x-pack:plugin:logsdb:qa:rolling-upgrade` -> `x-pack.plugin.logsdb.qa.rolling-upgrade.*`):

- `<project>.json` - a `ProjectTargetsFile`: each resolved target plus the **index of the ref that produced it**;
- `<project>.compile-tasks.txt` - the compile task paths of that project's runnable targets, newline-**terminated**.

They deliberately do *not* live in each project's own `build/` directory. The consumers - `flakinessScan` and the orchestration shell - must find the files without knowing the project set, and a `**/build/flakiness/*.json` glob would mean walking every build output directory in the repo (a `@InputFiles` fingerprint over hundreds of thousands of files). One flat directory makes discovery O(#projects) and the shell glue a `cat`. Each project writes only its own uniquely named files, so no two tasks overlap, and the orchestration `rm -rf`s the directory first so a reused CI workspace cannot leak a previous run's answer.

### No merge task

`flakinessScan` consumes the per-project files directly (`@InputFiles` over `project-targets/*.json`) and does the fold itself via the pure `FlakinessTargets.merge`. Two things genuinely need the global view, and both belong on the *scan* side of the compile step:

- **ref ordering** - each per-project entry carries its ref index, so the merged list reproduces the refs file's order;
- **the `unresolved` verdict** - a class ref is unresolved only if *no* project claimed it. This is why the per-project task deliberately runs the resolver one ref at a time and throws away its own per-ref "unresolved" verdicts: "not in *this* project" is not "not anywhere".

The only consumer that must run *before* compile is the compile task list, and each project can derive its own share of that - so there is no phase left for a merge task to occupy. `flakiness-base-targets.json` and `flakiness-compile-tasks.txt` no longer exist.

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
Mitigations: it happens only under `-Pflakiness.resolve`; only at configuration-cache store time; and only for the projects that actually **own a resolved ref**, which is what the self-selection cheap exit guarantees.
Realizing them for all ~450 projects would be indefensible; realizing them for 1-3 is cheap (measured below: the whole unqualified run is no slower than the old single root task).

Realizing `Test` tasks inside a store-time provider is **permitted** - no error, no CC problem, no deprecation, on 708 `Test` tasks across the three probe projects (`:qa:packaging` alone realized 637). This was the main uncertainty going in, and it simply worked. It is still arbitrary user code running late in the build, so a pathological project could in principle throw there; the failure would be a red resolve step, not a wrong answer.

### P8 (unsolved) - `onlyIf` predicates are invisible
`enabled` is a plain property we can read; **`onlyIf` is a `Spec<Task>` evaluated by Gradle at execution time** and cannot be introspected (and must not be speculatively evaluated - the predicates close over live task state).
Two live cases:
- `elasticsearch.bwc-test`: `onlyIf("BWC tests enabled") { project.bwc_tests_enabled }` on every `v<version>#*` task;
- `DistroTestPlugin`: `onlyIf { distribution.getArchitecture() == Architecture.current() }` - which is why the spike saw *all* `destructiveDistroTest.*` tasks reporting `enabled = true` on an aarch64 host, including the x86-only ones.

**How it is handled:** not at all for bwc, deliberately.
A task that passes `enabled` but is skipped by `onlyIf` still runs 0 tests and still reads as a `hang` - so this is a *residual* instance of the very bug this feature fixes, just a much narrower one:
- the packaging `onlyIf` is moot, because the packaging policy skips those tasks anyway;
- `bwc_tests_enabled` is true in normal CI/dev builds (it is only flipped off during release surgery), so the bwc `onlyIf` passes whenever the flakiness pipeline runs.
  Reading `project.bwc_tests_enabled` would fix the known case but would re-introduce exactly the per-convention special-casing this feature deleted, for a predicate that is almost always true.

If we later want to close the gap properly, the honest options are (a) have the batch runner treat "gradle reported `SKIPPED`/`NO-SOURCE` for the task" as `not_applicable` rather than letting the 0-test XML fall through to `hang` - a *runner-side* fix that covers every `onlyIf`, not just the ones we can name; or (b) ask Gradle for a dry-run of the task graph.
(a) is clearly the better shape and is where this should go next; it is **not** implemented here.

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

The 447 other projects each wrote an empty `{"projectPath": "...", "resolved": []}` and an empty compile-tasks file.

### (b) compile - the shell glue, and scan -> the plan

```
$ TASKS=$(cat build/flakiness/project-targets/*.compile-tasks.txt 2>/dev/null | sort -u)
$ echo "$TASKS"
:libs:dissect:compileTestJava
:x-pack:plugin:logsdb:qa:rolling-upgrade:compileJavaRestTestJava
$ ./gradlew $TASKS
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

**Running the task in 450 projects costs nothing measurable.** The whole-build configuration pass dominates completely, and it happened in the old topology too. On a repeat run CC makes it ~2x faster than the old flow could ever be. The reason the fan-out is free is the cheap exit: 447 projects do a handful of path comparisons and write a 60-byte file.

### (e) the cheap exit really is cheap - non-owning projects do not realize their `Test` tasks

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
- **Func test - PASS (2 tests), with the configuration cache ENABLED** (`disableConfigurationCache` is gone). `FlakinessResolvePluginFuncTest` builds a four-project fixture and runs the unqualified resolve -> plain compile -> scan. It asserts: every project ran the task; the three owning projects each wrote their own share with correct `compileTaskPath`/`outputDir`; `:untouched` wrote an **empty** share with `ownsRefs: false` and an **empty `testTasks` list** (the cheap exit, observed in the dumped model); the adversarial `:bwcish` fixture - which disables the bare task through `matching {}.configureEach {}` and registers three alternatives **after** the resolve task is registered - captures the post-mutation state (`test.enabled == false`, 3 `#altTest` tasks) and resolves to `[v9.6.0#altTest, v9.5.1#altTest]`, never `:bwcish:test`; the concatenated per-project compile lists; `Configuration cache entry stored`; the abstract base flattened to its two concrete subclasses; the capped fan-out in `taskSelections`; and the target-neutral (`__GRADLE__`) batch commands. A second test pins that a class ref no project owns is reported `unresolved` exactly once by the scan step.
- **Real-build end-to-end - PASS.** See PROOF above; `flakiness-plan.json` byte-identical to the old flow's.
- **TS suite - PASS.** `cd .buildkite && npx vitest run scripts/flakiness-detection` -> 11 files / 121 tests. Updated for the new orchestration shell: unqualified `flakinessResolveProject`, an explicit assertion that the command contains **no** `--no-configuration-cache`, the `rm -rf build/flakiness/project-targets` hygiene step, and the `cat .../*.compile-tasks.txt | sort -u` compile glue.
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
