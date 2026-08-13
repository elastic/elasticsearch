# Java/Gradle resolver - design notes

This is the running log for the Java/Gradle flakiness *resolution layer* in `build-tools-internal` (`org.elasticsearch.gradle.internal.flakiness`): it replaces the old TypeScript regex/path detectors with an authoritative, Gradle-model-backed resolver, wired into a five-step Buildkite topology.

It is an honest account of the design, what was verified, and the residual risks.

Status legend: [done] implemented + verified, [partial] implemented with caveats, [unverified] written but not executed end-to-end.

## Architecture

**Three** Buildkite steps.
The three Gradle invocations run sequentially inside **one** orchestration step on **one** agent (so the compiled output is shared - see "Why orchestration is one step" below); `generate` is a **separate** step on the default node-capable agent (the gradle image lacks node); the batch + analyze steps are then uploaded by `generate`:

```
1. bootstrap    (TS)     gather refs (git diff / muted-tests.yml diff / FLAKINESS_CLASSES)
                         -> flakiness-refs.json, then upload the [orchestration, generate] group     (contract 1)
2. orchestration (1 step, 1 gradle agent, key flakiness-orchestration:run) runs, in order:
   a. resolve   (Gradle) read refs (file-contents provider) + read the FlakinessModelService at EXECUTION time
                         -> flakiness-base-targets.json  (rich targets + unresolved)
                         -> flakiness-compile-tasks.txt  (the distinct compile task paths of the RUNNABLE targets)
   b. compile   (Gradle) PLAIN `run-gradle.sh <task paths from flakiness-compile-tasks.txt>`.
                         Its non-zero exit is the SOLE build_failed signal (writes buildFailed plan.json + marker).
   c. scan      (Gradle) ASM-scan the LOCAL compiled output dirs named in flakiness-base-targets.json, flatten
                         abstract bases, and emit ready batch commands -> flakiness-plan.json         (contract 2)
3. generate     (TS, separate step, default node agent, key flakiness-orchestration:generate)
                         download+read flakiness-plan.json -> map plan.commands to BK steps -> upload batches + analyze.
                         buildFailed -> upload only the analyze/build_failed record. no plan -> no-op (upstream failed).
```

Boundary held: **Java owns build-model/bytecode facts AND batch-command generation; TS owns Buildkite orchestration + JUnit analysis.** The two contracts between the layers are `flakiness-refs.json` (gather -> resolve) and `flakiness-plan.json` (scan -> generate; now also carrying the ready `commands`), plus the intermediate `flakiness-base-targets.json` / `flakiness-compile-tasks.txt` (resolve -> compile/scan), which are consumed only by shell/Java and carry no TS type.

### Why orchestration is one step (fixes a latent cross-agent bug)

resolve/compile/scan were originally separate Buildkite steps.
That is **broken on real CI**: Buildkite steps run on fresh agents with no shared workspace, and nothing ships the compile phase's `build/classes` output to a separate scan step - so on real agents `flakinessScan` would find **zero** compiled classes and every enrichment would silently no-op.
(The split only appeared to work in local verification because a single workspace was reused.)
Running resolve/compile/scan in one step on one agent keeps the compiled output on local disk for scan and warms the gradle daemon across the three invocations.
`generate`, by contrast, ships no build output and needs node, so it is its OWN step on the default agent - it downloads `flakiness-plan.json` (uploaded as an artifact by the orchestration step) and reads it.

This is purely a topology decision: it does **not** change the three gradle invocations, and it does **not** change the CC / whole-build-configuration facts (P0 below) - resolve still runs `--no-configuration-cache`.

**Failure attribution (P2) is preserved entirely in-shell**, phase by phase:
- resolve non-zero -> resolver/infra defect, **not** build_failed: no marker, exit rc (the red orchestration step is the signal for pipeline owners; the separate `generate` step then finds no plan and no-ops).
- compile non-zero -> the **sole** build_failed signal: write the buildFailed `flakiness-plan.json` + the `flakiness-precompile.json` marker, then exit rc.
  The separate `generate` step (wired `depends_on` orchestration with `allow_failure: true`) still runs, reads the buildFailed plan, and uploads the analyze-only pipeline that records the single build_failed.
- scan non-zero -> resolver/infra defect, **not** build_failed: no marker, exit rc; `generate` no-ops.
- happy path -> orchestration exits 0; `generate` reads the plan and uploads the batch + analyze steps.

Both orchestration steps are keyed `flakiness-orchestration:*` (not `flakiness-detection:`), so a red/failed orchestration or generate run is never fallback-recorded as a test batch by the external metric predicate (P2a).

## The BuildService (the core of the design)

`FlakinessModelService` is a Gradle `BuildService<None>` holding a `Map<projectPath, ProjectInfo>`.
It is the configuration-cache-blessed, isolated-projects-clean channel that carries the cross-project model.
The idiom mirrors `ProjectSubscribeBuildService`/`ProjectSubscribeServicePlugin`.

- **Populate at configuration, per project, from the project's OWN model - incrementally, no `afterEvaluate`.** In `ElasticsearchTestBasePlugin.apply(project)` (the per-project test hook), guarded behind `-Pflakiness.resolve`, we `registerIfAbsent` the service and call `FlakinessProjectModel.contribute`, which wires lazy reactions (mirroring `MutedTestPlugin`): `sourceSets.configureEach` records each recognised test source set as it is configured (catching `internalClusterTest`/`javaRestTest`/`yamlRestTest`, added by plugins applied later in the build script), and registers the project's late-read `Test`-task supplier.
  The service **accumulates** the per-source-set contributions into that project's `ProjectInfo`.
  This replaced an earlier `afterEvaluate` snapshot, which `GradlePluginConventionsArchUnitSpec` forbids; `configureEach`/`withPlugin` are order-independent and lazy.
  It reads only *this* project - no `getAllprojects()`/`getSubprojects()`/`getRootProject()`, no `afterEvaluate` - so it is isolated-projects-clean and convention-compliant.
  Note this does **not** change P0: the whole-build-config requirement is independent of `afterEvaluate`.
- **Read at execution.** `FlakinessResolveTask` declares the service via `@ServiceReference` + `usesService` and reads `service.get().projects()` in its `@TaskAction`.
  Because Elasticsearch does not use configuration-on-demand, every project is configured before any task executes (**with the crucial caveat about the configuration cache below**), so the assembled map is complete when the task consumes it.
- **Fully authoritative model.** `ProjectInfo` carries `projectPath`, `projectDir`, and per source set a `SourceSetInfo` with the real `javaSrcDirs`/`resourceSrcDirs`, the real compiled `outputDir`, and the real `compile<Ss>Java` task path.
  Resolution (`RefResolver`) works entirely off these real dirs - it no longer assumes the `src/<ss>/java` layout.
- **Test-task facts read LATE (see "Which task actually runs this test?" below).** `enabled` / `testClassesDirs` are mutated after our hook installs, so they are *not* in `ProjectInfo`; each project registers a supplier that the resolve task invokes from its own `@TaskAction`.

This replaces the prototype's root-plugin `getAllprojects()` walk, which was both an `IsolatedProjectsArchUnitSpec` violation and returned an **empty** model (subprojects are not configured at root-config time).
Both problems are gone: the walk is deleted, and the model is populated from each project's own configuration.

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

### The lifecycle crux: a LATE read, in the task action

`enabled` and `testClassesDirs` are **mutated after** `FlakinessProjectModel.contribute` runs (the bwc/distro plugins may be applied later in the build script), and the `v<version>#bwcTest` / `destructiveDistroTest.*` families are **registered** later still.
Snapshotting them eagerly inside a `configureEach` callback reads pre-mutation values and silently produces the wrong answer - the same class of trap as P1a.

So the service stores, per project, a **late-read supplier** (`registerTestTasks`), invoked only when `FlakinessModelService#testTasks(projectPath)` is called from `FlakinessResolveTask`'s `@TaskAction`.
Two properties make that correct by construction:
- by execution time the project has finished configuring, so every `Test` task exists;
- *iterating* `tasks.withType(Test)` **realizes** the tasks, and realization runs all of their pending `configureEach`/`named` configuration actions - including `enabled = false` and the `testClassesDirs` reassignment.
  There is no "later" left in which the values could change.

No `afterEvaluate`, no `getAllprojects()`/`getSubprojects()`/`getRootProject()`; each supplier closes over *its own* project only.
Both ArchUnit specs still pass.

**Config-cache posture (called out explicitly).** Those suppliers hold live `Project` references from configuration into execution.
That is config-cache-hostile *by construction*, not just by workflow: previously the resolve step merely *needed* whole-build configuration (P0) while the task code itself was CC-clean; now the build service also retains Gradle state.
Since resolve already runs `--no-configuration-cache` this changes nothing operationally, but it does mean "make resolve CC-compatible" is no longer a matter of removing a flag.
Everything the suppliers *return* is still a plain Gradle-free record (`TestTaskInfo`), so the resolution logic stays pure and unit-testable.

### Fan-out cap

A bwc project registers one task per wire-compatible version - **68 candidates** for `:x-pack:plugin:logsdb:qa:rolling-upgrade` (67 `v<version>#bwcTest` + `bcUpgradeTest`), each booting a real multi-node cluster - so the fan-out is capped at **2** by default (`-Pflakiness.taskCap`).
Candidates are ordered **newest-first**: a numeric-aware ("natural") comparison of the task name, descending.
That prefers the newest versions, orders `v8.19.10` *above* `v8.19.9` (plain lexicographic would not), and is a total order over unique task names - so the selection is reproducible regardless of registration order (asserted in `TestTaskSelectorTests`).
What was dropped is recorded in the plan's new `taskSelections[]`, mirroring `expansions[]`: `selected 2 of 68 candidate tasks (cap 2)`.

### Packaging policy (an AGENT-CAPABILITY decision, not a model fact)

`destructiveDistroTest.*` / `destructiveDistroUpgradeTest.*` are classified `skip` / `requires-packaging-host`.
The **model says they are runnable** - they are enabled and they do run those classes.
They are excluded because they install/remove packages and mutate the host, so they need a dedicated ephemeral packaging host (AGENTS.md: "do not run packaging suites on your workstation"), which the standard flakiness agent is not.
The `destructive` task-name prefix is the ES-wide marker for exactly that property (the `destructive*` tasks run against the local host; their non-destructive wrappers delegate to a throw-away VM).
This is stated in `TestTaskSelector`'s javadoc as a policy, and a non-destructive candidate always wins over destructive ones.
If a packaging-capable agent is ever wired up, the policy - not the model - is what changes.

### Multi-task command shape

The batching unit changed from a plan entry to an **(entry, task path) pair**.
An entry with N runnable tasks contributes N units, which are then batched per kind exactly as before.
Consequences:
- `javaRestTest` has batch cap 1, so the capped bwc set becomes **one Buildkite step per bwc version** - clean per-task attribution for the analyzer (a genuine incompatibility with one old version does not get mixed into another version's runs);
- unit/integ kinds keep their existing multi-task-per-invocation batching (they already put several *projects'* tasks in one gradle invocation), so nothing regresses for the common case.

## PROBLEMS

### P0 (NEW, critical) - the configuration cache defeats whole-build population
This is the sharpest edge and the most important finding of the rework.
The design needs *every* test project to be configured so it can contribute its model.
Under the **configuration cache**, Gradle only configures the projects reachable from the requested task graph - and `flakinessResolve` is a single root-project task, so the subprojects that own the refs never configure, their `configureEach`/`withPlugin` reactions never fire, and the service is **empty**.

Verified empirically on the real build:
- `./gradlew flakinessResolve -Pflakiness.resolve` (no CC) -> `2 refs -> 2 base targets across 450 projects`.
- `./gradlew flakinessResolve -Pflakiness.resolve --configuration-cache` -> **fails**, "FlakinessModelService is empty but there are 2 refs to resolve".
- `./gradlew flakinessResolve -Pflakiness.resolve --no-configuration-cache` -> `2 -> 2 across 450`.

Resolution: the resolve invocation **must** run with `--no-configuration-cache` (it is wired explicitly in `runners/buildkite.ts` `resolveCommand` and `entrypoints/local.ts`).
This costs nothing - the refs change every run, so CC would miss every time anyway.
The `FlakinessResolveTask` fails fast (throws) when the model is empty while there are refs to resolve, so this failure mode is **loud**, never the prototype's silent 0-targets trap.
The `scan` step, by contrast, is CC-safe: it only reads `flakiness-base-targets.json` and the output dirs (no cross-project model), so it needs no such flag.

The tasks *themselves* are configuration-cache-clean (no `getProject()`, no `Project`/`Gradle` fields, managed properties + injected services only, refs/base-targets read via a file-contents provider).
It is the *whole-build-configuration requirement* of the resolve step that is intrinsically incompatible with CC - a workflow property, not a bug in the task code.

### P1 - Config-pass cost x3
The pipeline runs three Gradle invocations (resolve, compile, scan) where the prototype ran one - now all on one agent, so the gradle daemon stays warm across them. resolve and scan both apply `-Pflakiness.resolve`, so each configures the whole build and repopulates the service (scan does not even use it).
This is more configuration work, mitigated by resolve/scan being cheap config-only / file-read tasks. compile is a plain invocation (no `-Pflakiness.resolve`), so it does not populate the service.

### P2 - Failure attribution (in-shell in the orchestration step; generate is a separate step)
Only the **compile** phase's non-zero exit means `build_failed`: the orchestration shell writes `{"buildFailed":true,"reason":"precompile"}` into `flakiness-plan.json` + the `flakiness-precompile.json` marker, then exits non-zero.
A failure in the **resolve** or **scan** phase is a resolver/tool/infra defect and is NOT reported as `build_failed` - the shell exits non-zero without writing a marker, so the orchestration step just goes red and reads downstream as an infra/pipeline problem.
The separate `generate` step is wired `depends_on` orchestration with `allow_failure: true`, so it always runs: on a compile failure it reads the buildFailed plan and uploads the analyze-only pipeline that records the single `build_failed`; on a resolve/scan failure it finds no plan (the orchestration wrote none) and no-ops cleanly (logs, exit 0, uploads nothing) rather than erroring.

### P2a - Step-key namespacing
The external metric treats a job as a flakiness test-batch job iff `step_key.startsWith("flakiness-detection:") && step_key !== "flakiness-detection:analyze"`.
So **both** orchestration steps are keyed under `flakiness-orchestration:` (`:run`, `:generate`), NOT `flakiness-detection:`.
Otherwise a red/failed/skipped orchestration or generate run would be fallback-recorded as a test batch.
Only the actual test batch steps (`flakiness-detection:unit` etc.) and `analyze` (`flakiness-detection:analyze`) keep the `flakiness-detection:` prefix.
`analyze.ts`'s synthetic `build_failed` payload is likewise keyed under `flakiness-orchestration:`.

### P3 - Class-ref resolution still needs a filesystem probe
Unmute/explicit refs carry only an FQCN; mapping it to a source set means checking where `<pkg>/<Name>.java` exists on disk under the source set's real `javaSrcDirs`.
The model gives authoritative source *dirs*, not the file inventory, so a disk probe per candidate root is unavoidable (the resolver runs it at task-execution time, so it is not a config-cache concern).

### P7 (NEW) - `Test`-task realization cost
Reading the task facts *requires realizing* the project's `Test` tasks, which runs arbitrary task-configuration code (testClusters wiring, distribution resolution, ...).
Mitigations: it happens only under `-Pflakiness.resolve` (a normal build realizes nothing extra), only at execution time, and only for the projects that actually **own a resolved target** - which is why the facts are fetched per project on demand (`FlakinessModelService#testTasks`) instead of being a field of `ProjectInfo`.
Realizing them for all ~450 projects would be indefensible; realizing them for 1-3 is cheap.

Measured on this repo (warm daemon, 3 runs each), realizing the `Test` tasks of the three proof projects - **706 tasks** (`:qa:packaging` alone has 636) including resolving each task's `testClassesDirs` FileCollection:

| | run 1 | run 2 | run 3 |
|---|---|---|---|
| no realization | 4.85s | 3.58s | 3.71s |
| realize 3 projects (706 `Test` tasks) | 3.60s | 3.83s | 3.69s |

i.e. **below run-to-run noise**; the whole-build configuration pass (P0/P1) dominates completely.
Honest caveat: this is one machine, one project set.
A pathological project whose `Test` tasks resolve heavy configurations on realization could be slower, and the failure mode would be a slow (or, if such realization throws, red) resolve step rather than a wrong answer.

### P8 (NEW, unsolved) - `onlyIf` predicates are invisible
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

- **Clear error when `flakiness-refs.json` is missing (was an opaque Gradle message).** `refsJson` is now `@Optional`; when the file is absent the task action throws a `GradleException` naming the path and telling the operator that the gather/bootstrap step is expected to have written it (or to pass `-Pflakiness.refs=<path>` for a standalone run), instead of Gradle's "property 'refsJson' doesn't have a configured value".
- **No `afterEvaluate`.** Model population is now the incremental `configureEach`/`withPlugin` idiom (see the BuildService section), fixing the `GradlePluginConventionsArchUnitSpec` violation.
  Independent of P0/CC.
- **Java owns batch-command generation.** The batching that used to live in the TS `commands.ts` (dedupe, collapse-yaml-suites, dedup-runners, cap-batching, per-kind command strings, the repeat-rest wrapper) is ported to `CommandBuilder`, and the scan task attaches the ready batch commands to `flakiness-plan.json`'s new `commands` array.
  Each command is **target-neutral**: it carries the literal `__GRADLE__` placeholder where the gradle binary belongs (both plain invocations and inside `repeat-rest-test.sh <iters> __GRADLE__ <tasks>`), which the thin TS runner layer replaces with `.ci/scripts/run-gradle.sh` (CI) or `./gradlew` (local).
  This is what lets `generate` be a minimal, node-only step that just maps commands to BK steps.
  The `FLAKINESS_ITERS` override now flows to Java: the plugin reads `-Pflakiness.iters` (set by `local.ts` for `--iters`) or the `FLAKINESS_ITERS` env var (carried in the CI build env, so the manual pipeline's override keeps working with **no yml change** - verified: `FLAKINESS_ITERS=7 flakinessScan` emits `-Dtests.iters=7`).
- **Quieter annotations.** Abstract expansions are logged to console only (they are already in `plan.json`); an unresolved-refs `warning` annotation is emitted only when the list is non-empty (a silently-unresolved unmute is a real false-negative); the always-on `info` "Flakiness resolver" annotation is gone.

## BENEFITS

- **Fully authoritative model, now genuinely delivered.** Project boundaries, source-set shape (real `srcDirs`), compiled output locations (real `outputDir`), the compile task paths (real `compile<Ss>Java`), and the **tasks that actually re-run a target** (real `Test` tasks: `enabled` + `testClassesDirs`) all come from each project's live configured model - not path conventions, build.gradle regexes, or plugin markers.
  The prototype's P1a (source-set shape / output dirs / bwc falling back to convention because the live model was unavailable at root-config time) is resolved: the model is read where it exists, in each project.
- **Abstract detection + subclass expansion via bytecode.** `ClassHierarchyScanner` reads `ACC_ABSTRACT` + the super-class chain off the compiled `.class` files (ASM, already on the classpath), so an unmuted abstract base deterministically expands to its concrete subclasses (sorted FQCN, capped).
  TS cannot do this.
- **No batch steps until compile succeeds.** The batch steps are only *created* by `generate`, which runs after a successful orchestration (resolve -> compile -> scan).
  A PR that does not compile uploads only the analyze/build_failed record, so the skipped-batch `waiting_failed` metric noise structurally cannot occur.

- **Task disposition is derived, not assumed.** The bare-task assumption is gone from every layer. bwc targets are now genuinely re-run (as `v<version>#bwcTest`) instead of being marked not-applicable; packaging targets are skipped with a reason that is *true* (`requires-packaging-host`) rather than a marker that was merely convenient; and a source set with no enabled `Test` task says exactly that (`no-runnable-task`) instead of emitting a task that Gradle reports `SKIPPED` and the analyzer scores as a `hang`.

## PROOF: real-build run of the disposition layer

Hand-written `flakiness-refs.json` covering all three cases, then `./gradlew -Pflakiness.resolve --no-configuration-cache flakinessResolve` on the real build:

```json
{ "mergeBase": "hand-written-proof",
  "refs": [
    { "source": "unmute", "className": "org.elasticsearch.xpack.logsdb.LogsdbIndexingRollingUpgradeIT" },
    { "source": "changed-file", "path": "qa/packaging/src/test/java/org/elasticsearch/packaging/test/ArchiveTests.java" },
    { "source": "changed-file", "path": "libs/dissect/src/test/java/org/elasticsearch/dissect/DissectParserTests.java" } ] }
```

```
> Task :flakinessResolve
flakiness resolve: 3 refs -> 3 base targets, 0 unresolved (across 450 projects), 2 compile tasks
  javaRestTest :x-pack:plugin:logsdb:qa:rolling-upgrade org.elasticsearch.xpack.logsdb.LogsdbIndexingRollingUpgradeIT -> [:x-pack:plugin:logsdb:qa:rolling-upgrade:v9.6.0#bwcTest, :x-pack:plugin:logsdb:qa:rolling-upgrade:v9.5.1#bwcTest]
  test :qa:packaging org.elasticsearch.packaging.test.ArchiveTests -> skip (requires-packaging-host)
  test :libs:dissect org.elasticsearch.dissect.DissectParserTests -> [:libs:dissect:test]

BUILD SUCCESSFUL in 3s
```

`flakiness-base-targets.json` (all three cases; note `candidateTasks` - the model really did see 68 and 636 candidates, and the disabled bare tasks are absent from `runnableTasks`):

```json
{
  "targets" : [ {
    "gradleProject" : ":x-pack:plugin:logsdb:qa:rolling-upgrade",
    "sourceSet" : "javaRestTest",
    "kind" : "javaRestTest",
    "fqcn" : "org.elasticsearch.xpack.logsdb.LogsdbIndexingRollingUpgradeIT",
    "compileTaskPath" : ":x-pack:plugin:logsdb:qa:rolling-upgrade:compileJavaRestTestJava",
    "outputDir" : ".../x-pack/plugin/logsdb/qa/rolling-upgrade/build/classes/java/javaRestTest",
    "runnableTasks" : [ ":x-pack:plugin:logsdb:qa:rolling-upgrade:v9.6.0#bwcTest", ":x-pack:plugin:logsdb:qa:rolling-upgrade:v9.5.1#bwcTest" ],
    "candidateTasks" : 68,
    "skipReason" : null
  }, {
    "gradleProject" : ":qa:packaging",
    "sourceSet" : "test",
    "kind" : "test",
    "fqcn" : "org.elasticsearch.packaging.test.ArchiveTests",
    "compileTaskPath" : ":qa:packaging:compileTestJava",
    "outputDir" : ".../qa/packaging/build/classes/java/test",
    "runnableTasks" : [ ],
    "candidateTasks" : 636,
    "skipReason" : "requires-packaging-host"
  }, {
    "gradleProject" : ":libs:dissect",
    "sourceSet" : "test",
    "kind" : "test",
    "fqcn" : "org.elasticsearch.dissect.DissectParserTests",
    "compileTaskPath" : ":libs:dissect:compileTestJava",
    "outputDir" : ".../libs/dissect/build/classes/java/test",
    "runnableTasks" : [ ":libs:dissect:test" ],
    "candidateTasks" : 1,
    "skipReason" : null
  } ],
  "unresolved" : [ ]
}
```

(a) **bwc** - `runnableTasks` are the two newest `v<version>#bwcTest` task paths; the disabled bare `:x-pack:plugin:logsdb:qa:rolling-upgrade:javaRestTest` appears nowhere.
Its `compileJavaRestTestJava` IS in `flakiness-compile-tasks.txt` (previously a bwc target was excluded from compilation because it was skipped).
(b) **packaging** - the model discovered 636 `destructiveDistroTest.*` / `destructiveDistroUpgradeTest.*` candidates and the entry is `skip` / `requires-packaging-host`.
(c) **ordinary project** - `:libs:dissect` resolves to the plain enabled bare task, unchanged.

Then the plain compile of the two emitted task paths (`BUILD SUCCESSFUL`, 45s) and `flakinessScan` -> `flakiness-plan.json`:

```json
{
  "buildFailed" : false,
  "entries" : [ {
    "gradleProject" : ":x-pack:plugin:logsdb:qa:rolling-upgrade", "sourceSet" : "javaRestTest", "kind" : "javaRestTest",
    "fqcn" : "org.elasticsearch.xpack.logsdb.LogsdbIndexingRollingUpgradeIT", "disposition" : "run",
    "runnableTasks" : [ ":x-pack:plugin:logsdb:qa:rolling-upgrade:v9.6.0#bwcTest", ":x-pack:plugin:logsdb:qa:rolling-upgrade:v9.5.1#bwcTest" ]
  }, {
    "gradleProject" : ":qa:packaging", "sourceSet" : "test", "kind" : "test",
    "fqcn" : "org.elasticsearch.packaging.test.ArchiveTests", "disposition" : "skip",
    "reason" : "requires-packaging-host", "runnableTasks" : [ ]
  }, {
    "gradleProject" : ":libs:dissect", "sourceSet" : "test", "kind" : "test",
    "fqcn" : "org.elasticsearch.dissect.DissectParserTests", "disposition" : "run",
    "runnableTasks" : [ ":libs:dissect:test" ]
  } ],
  "expansions" : [ ],
  "taskSelections" : [ {
    "gradleProject" : ":x-pack:plugin:logsdb:qa:rolling-upgrade", "sourceSet" : "javaRestTest",
    "selected" : [ ":x-pack:plugin:logsdb:qa:rolling-upgrade:v9.6.0#bwcTest", ":x-pack:plugin:logsdb:qa:rolling-upgrade:v9.5.1#bwcTest" ],
    "total" : 68, "cap" : 2
  } ],
  "unresolved" : [ ],
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
}
```

The two bwc versions land in **separate** batch commands (javaRestTest cap 1) and the ordinary unit command is byte-for-byte what the previous convention-based builder produced.
(`#` mid-word is not a shell comment, so the task paths need no quoting.)

### Proof of the lifecycle claim (why the late read is the point)
The values above cannot be obtained any earlier.
An independent init-script spike confirmed the same reads at execution time, and confirmed what a project's tasks look like once configured:

```
SPIKE PROJECT :x-pack:plugin:logsdb:qa:rolling-upgrade
SPIKE   TASK name=bcUpgradeTest    enabled=true  dirs=[.../build/classes/java/javaRestTest]
SPIKE   TASK name=javaRestTest     enabled=false dirs=[.../build/classes/java/javaRestTest]
SPIKE   TASK name=test             enabled=false dirs=[.../build/classes/java/test]
SPIKE   TASK name=v8.19.0#bwcTest  enabled=true  dirs=[.../build/classes/java/javaRestTest]
... (67 v<version>#bwcTest tasks in total)
SPIKE PROJECT :qa:packaging
SPIKE   TASK name=test                                enabled=false dirs=[.../qa/packaging/build/classes/java/test]
SPIKE   TASK name=destructiveDistroTest.default-deb    enabled=true  dirs=[.../qa/packaging/build/classes/java/test]
...
```

`FlakinessResolvePluginFuncTest` covers the same lifecycle hermetically: its `:bwcish` fixture flips the bare task off through `tasks.matching {}.configureEach {}` and registers three alternative `Test` tasks **after** `FlakinessProjectModel.contribute` has run, and asserts the resolver picks `v9.6.0#altTest` + `v9.5.1#altTest` (never `:bwcish:test`) with `candidateTasks == 3`.
An eager snapshot fails that test.

## Verification (what was actually run)

- **Java pure-core + task-helper unit tests - PASS.** `:build-tools-internal:test --tests "...flakiness.*"`: `FlakinessResolverTests` (7), `FlakinessResolveTaskTests` (2), `FlakinessScanTaskTests` (1), `CommandBuilderTests` (11), `TestTaskSelectorTests` (11) = **32 green**.
  Covers ASM abstract detection + deterministic/capped expansion on real generated bytecode; authoritative changed-file + class/explicit-ref resolution over real `srcDirs`; yaml suite/case; plan flattening; abstract-with-no-subclass surfaced as unresolved; Jackson round-trips; the task-specific compile-task-list / scan-dir derivations; the full batch-command generation (per-kind command shapes with the `__GRADLE__` marker, cap-batching, dedupe/collapse/dedup-runners, iters override); and the whole disposition layer - output-dir overlap matching (incl. multi-`classesDirs` membership and non-normalized paths), bare-task-canonical vs fall-back-to-alternatives vs `no-runnable-task` vs `requires-packaging-host`, the newest-first natural ordering (`v8.19.10` above `v8.19.9`) and its order-independence, a zero cap, and the non-destructive-wins-over-destructive rule.
- **ArchUnit specs - PASS (executed this time).** `:build-tools-internal:test --tests "*IsolatedProjectsArchUnitSpec*" --tests "*GradlePluginConventionsArchUnitSpec*"` = 14 + 4 green, i.e. the late-read design introduces no `afterEvaluate` and no cross-project `getAllprojects()`/`getSubprojects()`/`getRootProject()`.
- **Func test - PASS.** `FlakinessResolvePluginFuncTest` (TestKit, extends `AbstractGradleInternalPluginFuncTest`) builds a **three**-project fixture (`:app` with an abstract base + two concrete subclasses; `:other` a second project; `:bwcish` reproducing the disabled-bare-task shape), each contributing its model via the exact `FlakinessProjectModel.contribute` registration snippet (configureEach, no afterEvaluate).
  It runs resolve -> plain compile of the emitted task paths -> scan and asserts: the service populated from per-project config (3 authoritative base targets across ALL projects, with correct `compileTaskPath`/`outputDir`); cross-project boundary resolution; the abstract base flattened to its two concrete subclasses with `expandedFrom`; that `plan.commands` carries a target-neutral (`__GRADLE__`) unit-test batch command covering the ordinary projects; and the disposition layer end-to-end - `:bwcish` resolves to `[v9.6.0#altTest, v9.5.1#altTest]` (never the disabled `:bwcish:test`), is compiled, reports `selected 2 of 3` in `taskSelections`, and its emitted commands name the alternative tasks.
  The `:bwcish` fixture flips `enabled` through `tasks.matching {}.configureEach {}` and registers the alternatives **after** `contribute` has run, so it fails if the read is not late.
  It runs with `--no-configuration-cache` (see P0) and is listed in `IntegTestCoverageArchUnitSpec.KNOWN_CC_INCOMPATIBLE`.
- **FLAKINESS_ITERS override via env - PASS.** `FLAKINESS_ITERS=7 flakinessScan` emitted a command with `-Dtests.iters=7` (no `-Pflakiness.iters` needed), proving the manual CI override works with no yml change.
- **Missing-refs error - PASS.** `flakinessResolve` with no `flakiness-refs.json` throws the clear "flakiness-refs.json not found at ...; pass -Pflakiness.refs=<path>" message, not Gradle's opaque one.
- **Real-build resolve - PASS, and the CC failure mode reproduced.** See P0: `2 refs -> 2 base targets across 450 projects` without CC (both default and explicit `--no-configuration-cache`); empty (loud failure) with `--configuration-cache`.
  This proves populate->read works end-to-end and that the fail-fast guard fires.
- **Full 3-Gradle-invocation flow on the real build, ONE workspace - PASS (this is the point of the merge).** resolve (`:libs:dissect`, one changed-file + one unmute) -> `flakiness-base-targets.json` with authoritative `compileTaskPath`/`outputDir` + `flakiness-compile-tasks.txt` = `:libs:dissect:compileTestJava`; plain `./gradlew $(cat flakiness-compile-tasks.txt)` (BUILD SUCCESSFUL); `flakinessScan` reads the LOCAL compiled output and writes `flakiness-plan.json` with 2 concrete `run` entries.
  Because compile and scan ran in the same workspace, scan saw the compiled classes - which is exactly the cross-agent bug the single-step merge fixes.
- **`:build-tools-internal` compiles (main + test + integTest) - PASS.**
- **TS suite - PASS.** `npx vitest run scripts/flakiness-detection`: 11 files / **121 tests** green (asserts the two orchestration steps `flakiness-orchestration:{run,generate}`; the orchestration resolve/compile/scan-only shell with in-shell attribution; the separate node-agent generate step with `depends_on` orchestration `allow_failure: true`; `generate` reading the plan local-or-download, mapping `plan.commands` with `__GRADLE__` -> `.ci/scripts/run-gradle.sh`, no-op on missing plan; the quieter annotations; `planCommandsToRunnable`/`withGradleBinary` for both targets; and the new disposition plumbing - `planEntryToSkippedTest` carrying the skip `reason` into `flakiness-skipped.json`, `notApplicablePayload` recording that reason (with a `not-runnable` fallback for a legacy artifact), and `generate` logging the capped `taskSelections` to the console rather than as an annotation).

### NOT run (per brief / environmental)
- The full ES build.
  `:build-tools-internal:spotlessJavaCheck` still cannot be *created* in this environment ("You need to add a repository containing google-java-format:1.19.2"), but `spotlessApply` runs and was used to format the sources; note it targets `src/main/java` only, so the test sources are outside its scope.
  `tsc --noEmit` is not available either (typescript is not a dev dependency here); run via `npx -p typescript` it reports only two pre-existing errors in the untouched `runners/buildkite.test.ts` (verified pre-existing by stashing this change), and none in any file touched here.
- The `ConfigurationCacheArchUnitSpec` / `TaskModellingArchUnitSpec` / `IntegTestCoverageArchUnitSpec` specs were read but not executed; the code was written to satisfy them (tasks have no `getProject()`/`Project` fields - the live `Project` is captured by the *build service's* supplier, not by a task; no eager task creation; the func test stays baselined in `KNOWN_CC_INCOMPATIBLE`).
  The two specs the brief names (`IsolatedProjects*`, `GradlePluginConventions*`) WERE executed and pass.
- The Buildkite pipeline in CI (the yml + dynamic uploads are written and the pure `toResolvePipeline`/ `toBuildkitePipeline` structure is unit-tested, but not run on a real agent).

## Honest assessment

- The **authoritative-model** pitch is now fully delivered *including the disposition*: source-set shape, output dirs, compile task paths, and now the set of tasks that genuinely re-run a target are all read from each project's real configured model, with zero cross-project access.
  The last convention assumption in the pipeline (`:project:<kind>`) is gone, and with it both a false negative (bwc tests reported `not_applicable` and never re-run) and a false positive (packaging targets scored as `hang` because a disabled task ran zero tests).
- The **riskiest** part is P0: the resolve step depends on the whole build being configured, which the configuration cache does not do for a root task, so resolve must run `--no-configuration-cache`.
  This is correct and cheap today, but it is a standing constraint - if a future change made the flakiness pipeline rely on CC for resolve, it would silently under-populate.
  The fail-fast guard turns that into a loud failure rather than the prototype's silent-empty, which is the key safety property.
- The **second-riskiest** part is now P8: `onlyIf` is invisible, so a task that is `enabled` but skipped at execution still yields a 0-test `hang`.
  The feature shrinks that hole a lot (it removes the two big `enabled = false` cases) without closing it, and the right fix is runner-side, not resolver-side.
- Residual risks: (1) the `rest-api-spec/test/<suitePath>.yml` yaml-suite layout is still encoded as a constant (it is an ESClientYamlSuiteTestCase-wide convention, not a per-project layout assumption, so low risk); (2) the config-pass cost is 3x, plus `Test`-task realization for the owning projects (P7 - measured as noise); (3) the build service now holds live `Project` references from configuration into execution, deepening the no-configuration-cache dependency from a workflow constraint into a structural one; (4) the packaging skip is a policy about *this agent*, so it will read as a wrong answer the day a packaging-capable agent exists - which is why it is a named constant with a documented rationale rather than an inline condition; (5) the incremental `sourceSets.configureEach` registration relies on each test source set being *realized* during configuration - true under the whole-build (no-CC) config the resolve step runs, since the java/test plugins realize the source sets via their compile/test tasks.
  The earlier node-on-gradle-image risk is **resolved**: `generate` is now its own step on the default node-capable agent (only resolve/compile/scan run on the gradle image), so nothing assumes node is present on the gradle image.
