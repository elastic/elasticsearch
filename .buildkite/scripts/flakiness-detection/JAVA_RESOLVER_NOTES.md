# Java/Gradle resolver - design notes

This is the running log for the Java/Gradle flakiness *resolution layer* in `build-tools-internal`
(`org.elasticsearch.gradle.internal.flakiness`): it replaces the old TypeScript regex/path detectors with an
authoritative, Gradle-model-backed resolver, wired into a five-step Buildkite topology.

It is an honest account of the design, what was verified, and the residual risks.

Status legend: [done] implemented + verified, [partial] implemented with caveats, [unverified] written but not executed end-to-end.

## Architecture

**Three** Buildkite steps. The three Gradle invocations run sequentially inside **one** orchestration step on
**one** agent (so the compiled output is shared - see "Why orchestration is one step" below); `generate` is a
**separate** step on the default node-capable agent (the gradle image lacks node); the batch + analyze steps
are then uploaded by `generate`:

```
1. bootstrap    (TS)     gather refs (git diff / muted-tests.yml diff / FLAKINESS_CLASSES)
                         -> flakiness-refs.json, then upload the [orchestration, generate] group     (contract 1)
2. orchestration (1 step, 1 gradle agent, key flakiness-orchestration:run) runs, in order:
   a. resolve   (Gradle) read refs (file-contents provider) + read the FlakinessModelService at EXECUTION time
                         -> flakiness-base-targets.json  (rich targets + unresolved)
                         -> flakiness-compile-tasks.txt  (the distinct non-bwc compile task paths)
   b. compile   (Gradle) PLAIN `run-gradle.sh <task paths from flakiness-compile-tasks.txt>`.
                         Its non-zero exit is the SOLE build_failed signal (writes buildFailed plan.json + marker).
   c. scan      (Gradle) ASM-scan the LOCAL compiled output dirs named in flakiness-base-targets.json, flatten
                         abstract bases, and emit ready batch commands -> flakiness-plan.json         (contract 2)
3. generate     (TS, separate step, default node agent, key flakiness-orchestration:generate)
                         download+read flakiness-plan.json -> map plan.commands to BK steps -> upload batches + analyze.
                         buildFailed -> upload only the analyze/build_failed record. no plan -> no-op (upstream failed).
```

Boundary held: **Java owns build-model/bytecode facts AND batch-command generation; TS owns Buildkite
orchestration + JUnit analysis.** The two contracts between the layers are `flakiness-refs.json` (gather ->
resolve) and `flakiness-plan.json` (scan -> generate; now also carrying the ready `commands`), plus the
intermediate `flakiness-base-targets.json` / `flakiness-compile-tasks.txt` (resolve -> compile/scan), which
are consumed only by shell/Java and carry no TS type.

### Why orchestration is one step (fixes a latent cross-agent bug)

resolve/compile/scan were originally separate Buildkite steps. That is **broken on real CI**: Buildkite steps
run on fresh agents with no shared workspace, and nothing ships the compile phase's `build/classes` output to
a separate scan step - so on real agents `flakinessScan` would find **zero** compiled classes and every
enrichment would silently no-op. (The split only appeared to work in local verification because a single
workspace was reused.) Running resolve/compile/scan in one step on one agent keeps the compiled output on
local disk for scan and warms the gradle daemon across the three invocations. `generate`, by contrast, ships
no build output and needs node, so it is its OWN step on the default agent - it downloads `flakiness-plan.json`
(uploaded as an artifact by the orchestration step) and reads it.

This is purely a topology decision: it does **not** change the three gradle invocations, and it does **not**
change the CC / whole-build-configuration facts (P0 below) - resolve still runs `--no-configuration-cache`.

**Failure attribution (P2) is preserved entirely in-shell**, phase by phase:
- resolve non-zero -> resolver/infra defect, **not** build_failed: no marker, exit rc (the red orchestration
  step is the signal for pipeline owners; the separate `generate` step then finds no plan and no-ops).
- compile non-zero -> the **sole** build_failed signal: write the buildFailed `flakiness-plan.json` + the
  `flakiness-precompile.json` marker, then exit rc. The separate `generate` step (wired `depends_on`
  orchestration with `allow_failure: true`) still runs, reads the buildFailed plan, and uploads the
  analyze-only pipeline that records the single build_failed.
- scan non-zero -> resolver/infra defect, **not** build_failed: no marker, exit rc; `generate` no-ops.
- happy path -> orchestration exits 0; `generate` reads the plan and uploads the batch + analyze steps.

Both orchestration steps are keyed `flakiness-orchestration:*` (not `flakiness-detection:`), so a red/failed
orchestration or generate run is never fallback-recorded as a test batch by the external metric predicate (P2a).

## The BuildService (the core of the design)

`FlakinessModelService` is a Gradle `BuildService<None>` holding a `Map<projectPath, ProjectInfo>`. It is the
configuration-cache-blessed, isolated-projects-clean channel that carries the cross-project model. The idiom
mirrors `ProjectSubscribeBuildService`/`ProjectSubscribeServicePlugin`.

- **Populate at configuration, per project, from the project's OWN model - incrementally, no
  `afterEvaluate`.** In `ElasticsearchTestBasePlugin.apply(project)` (the per-project test hook), guarded
  behind `-Pflakiness.resolve`, we `registerIfAbsent` the service and call `FlakinessProjectModel.contribute`,
  which wires lazy reactions (mirroring `MutedTestPlugin`): `sourceSets.configureEach` records each recognised
  test source set as it is configured (catching `internalClusterTest`/`javaRestTest`/`yamlRestTest`, added by
  plugins applied later in the build script), and `pluginManager.withPlugin("elasticsearch.bwc-test")` sets the
  bwc flag. The service **accumulates** these per-source-set contributions into that project's `ProjectInfo`.
  This replaced an earlier `afterEvaluate` snapshot, which `GradlePluginConventionsArchUnitSpec` forbids;
  `configureEach`/`withPlugin` are order-independent and lazy. It reads only *this* project - no
  `getAllprojects()`/`getSubprojects()`/`getRootProject()`, no `afterEvaluate` - so it is
  isolated-projects-clean and convention-compliant. Note this does **not** change P0: the whole-build-config
  requirement is independent of `afterEvaluate`.
- **Read at execution.** `FlakinessResolveTask` declares the service via `@ServiceReference` + `usesService`
  and reads `service.get().projects()` in its `@TaskAction`. Because Elasticsearch does not use
  configuration-on-demand, every project is configured before any task executes (**with the crucial caveat about
  the configuration cache below**), so the assembled map is complete when the task consumes it.
- **Fully authoritative model.** `ProjectInfo` carries `projectPath`, `projectDir`, `bwc`
  (`pluginManager.hasPlugin("elasticsearch.bwc-test")` - the authoritative fact, no more build.gradle regex),
  and per source set a `SourceSetInfo` with the real `javaSrcDirs`/`resourceSrcDirs`, the real compiled
  `outputDir`, and the real `compile<Ss>Java` task path. Resolution (`RefResolver`) works entirely off these
  real dirs - it no longer assumes the `src/<ss>/java` layout.

This replaces the prototype's root-plugin `getAllprojects()` walk, which was both an
`IsolatedProjectsArchUnitSpec` violation and returned an **empty** model (subprojects are not configured at
root-config time). Both problems are gone: the walk is deleted, and the model is populated from each project's
own configuration.

## PROBLEMS

### P0 (NEW, critical) - the configuration cache defeats whole-build population
This is the sharpest edge and the most important finding of the rework. The design needs *every* test project
to be configured so it can contribute its model. Under the **configuration cache**, Gradle only configures the
projects reachable from the requested task graph - and `flakinessResolve` is a single root-project task, so the
subprojects that own the refs never configure, their `configureEach`/`withPlugin` reactions never fire, and the
service is **empty**.

Verified empirically on the real build:
- `./gradlew flakinessResolve -Pflakiness.resolve` (no CC) -> `2 refs -> 2 base targets across 450 projects`.
- `./gradlew flakinessResolve -Pflakiness.resolve --configuration-cache` -> **fails**, "FlakinessModelService is
  empty but there are 2 refs to resolve".
- `./gradlew flakinessResolve -Pflakiness.resolve --no-configuration-cache` -> `2 -> 2 across 450`.

Resolution: the resolve invocation **must** run with `--no-configuration-cache` (it is wired explicitly in
`runners/buildkite.ts` `resolveCommand` and `entrypoints/local.ts`). This costs nothing - the refs change every
run, so CC would miss every time anyway. The `FlakinessResolveTask` fails fast (throws) when the model is empty
while there are refs to resolve, so this failure mode is **loud**, never the prototype's silent 0-targets trap.
The `scan` step, by contrast, is CC-safe: it only reads `flakiness-base-targets.json` and the output dirs (no
cross-project model), so it needs no such flag.

The tasks *themselves* are configuration-cache-clean (no `getProject()`, no `Project`/`Gradle` fields, managed
properties + injected services only, refs/base-targets read via a file-contents provider). It is the
*whole-build-configuration requirement* of the resolve step that is intrinsically incompatible with CC - a
workflow property, not a bug in the task code.

### P1 - Config-pass cost x3
The pipeline runs three Gradle invocations (resolve, compile, scan) where the prototype ran one - now all on one
agent, so the gradle daemon stays warm across them. resolve and scan both apply `-Pflakiness.resolve`, so each
configures the whole build and repopulates the service (scan does not even use it). This is more configuration
work, mitigated by resolve/scan being cheap config-only / file-read tasks. compile is a plain invocation (no
`-Pflakiness.resolve`), so it does not populate the service.

### P2 - Failure attribution (in-shell in the orchestration step; generate is a separate step)
Only the **compile** phase's non-zero exit means `build_failed`: the orchestration shell writes
`{"buildFailed":true,"reason":"precompile"}` into `flakiness-plan.json` + the `flakiness-precompile.json`
marker, then exits non-zero. A failure in the **resolve** or **scan** phase is a resolver/tool/infra defect and
is NOT reported as `build_failed` - the shell exits non-zero without writing a marker, so the orchestration step
just goes red and reads downstream as an infra/pipeline problem. The separate `generate` step is wired
`depends_on` orchestration with `allow_failure: true`, so it always runs: on a compile failure it reads the
buildFailed plan and uploads the analyze-only pipeline that records the single `build_failed`; on a
resolve/scan failure it finds no plan (the orchestration wrote none) and no-ops cleanly (logs, exit 0, uploads
nothing) rather than erroring.

### P2a - Step-key namespacing
The external metric treats a job as a flakiness test-batch job iff
`step_key.startsWith("flakiness-detection:") && step_key !== "flakiness-detection:analyze"`. So **both**
orchestration steps are keyed under `flakiness-orchestration:` (`:run`, `:generate`), NOT `flakiness-detection:`.
Otherwise a red/failed/skipped orchestration or generate run would be fallback-recorded as a test batch. Only
the actual test batch steps (`flakiness-detection:unit` etc.) and `analyze` (`flakiness-detection:analyze`) keep
the `flakiness-detection:` prefix. `analyze.ts`'s synthetic `build_failed` payload is likewise keyed under
`flakiness-orchestration:`.

### P3 - Class-ref resolution still needs a filesystem probe
Unmute/explicit refs carry only an FQCN; mapping it to a source set means checking where `<pkg>/<Name>.java`
exists on disk under the source set's real `javaSrcDirs`. The model gives authoritative source *dirs*, not the
file inventory, so a disk probe per candidate root is unavoidable (the resolver runs it at task-execution time,
so it is not a config-cache concern).

## Review-driven refinements

- **Clear error when `flakiness-refs.json` is missing (was an opaque Gradle message).** `refsJson` is now
  `@Optional`; when the file is absent the task action throws a `GradleException` naming the path and telling
  the operator that the gather/bootstrap step is expected to have written it (or to pass `-Pflakiness.refs=<path>`
  for a standalone run), instead of Gradle's "property 'refsJson' doesn't have a configured value".
- **No `afterEvaluate`.** Model population is now the incremental `configureEach`/`withPlugin` idiom (see the
  BuildService section), fixing the `GradlePluginConventionsArchUnitSpec` violation. Independent of P0/CC.
- **Java owns batch-command generation.** The batching that used to live in the TS `commands.ts` (dedupe,
  collapse-yaml-suites, dedup-runners, cap-batching, per-kind command strings, the repeat-rest wrapper) is
  ported to `CommandBuilder`, and the scan task attaches the ready batch commands to `flakiness-plan.json`'s new
  `commands` array. Each command is **target-neutral**: it carries the literal `__GRADLE__` placeholder where
  the gradle binary belongs (both plain invocations and inside `repeat-rest-test.sh <iters> __GRADLE__ <tasks>`),
  which the thin TS runner layer replaces with `.ci/scripts/run-gradle.sh` (CI) or `./gradlew` (local). This is
  what lets `generate` be a minimal, node-only step that just maps commands to BK steps. The `FLAKINESS_ITERS`
  override now flows to Java: the plugin reads `-Pflakiness.iters` (set by `local.ts` for `--iters`) or the
  `FLAKINESS_ITERS` env var (carried in the CI build env, so the manual pipeline's override keeps working with
  **no yml change** - verified: `FLAKINESS_ITERS=7 flakinessScan` emits `-Dtests.iters=7`).
- **Quieter annotations.** Abstract expansions are logged to console only (they are already in `plan.json`); an
  unresolved-refs `warning` annotation is emitted only when the list is non-empty (a silently-unresolved unmute
  is a real false-negative); the always-on `info` "Flakiness resolver" annotation is gone.

## BENEFITS

- **Fully authoritative model, now genuinely delivered.** Project boundaries, source-set shape (real
  `srcDirs`), compiled output locations (real `outputDir`), the compile task paths (real `compile<Ss>Java`), and
  `bwc` (`hasPlugin`) all come from each project's live configured model - not path conventions or build.gradle
  regexes. The prototype's P1a (source-set shape / output dirs / bwc falling back to convention because the live
  model was unavailable at root-config time) is resolved: the model is read where it exists, in each project.
- **Abstract detection + subclass expansion via bytecode.** `ClassHierarchyScanner` reads `ACC_ABSTRACT` + the
  super-class chain off the compiled `.class` files (ASM, already on the classpath), so an unmuted abstract base
  deterministically expands to its concrete subclasses (sorted FQCN, capped). TS cannot do this.
- **No batch steps until compile succeeds.** The batch steps are only *created* by `generate`, which runs after
  a successful orchestration (resolve -> compile -> scan). A PR that does not compile uploads only the
  analyze/build_failed record, so the skipped-batch `waiting_failed` metric noise structurally cannot occur.

## Verification (what was actually run)

- **Java pure-core + task-helper unit tests - PASS.** `:build-tools-internal:test --tests "...flakiness.*"`:
  `FlakinessResolverTests` (7), `FlakinessResolveTaskTests` (2), `FlakinessScanTaskTests` (1),
  `CommandBuilderTests` (9) = 19 green. Covers ASM abstract detection + deterministic/capped expansion on real
  generated bytecode; authoritative changed-file + class/explicit-ref resolution over real `srcDirs`; yaml
  suite/case; bwc; plan flattening; abstract-with-no-subclass surfaced as unresolved; Jackson round-trips; the
  task-specific compile-task-list / scan-dir derivations; and the full batch-command generation (per-kind
  command shapes with the `__GRADLE__` marker, cap-batching, dedupe/collapse/dedup-runners, iters override).
- **Func test - PASS.** `FlakinessResolvePluginFuncTest` (TestKit, extends `AbstractGradleInternalPluginFuncTest`)
  builds a two-project fixture (`:app` with an abstract base + two concrete subclasses; `:other` a second
  project), each contributing its model via the exact `FlakinessProjectModel.contribute` registration snippet
  (configureEach/withPlugin, no afterEvaluate). It runs resolve -> plain compile of the emitted task paths ->
  scan and asserts: the service populated from per-project config (2 authoritative base targets across BOTH
  projects, with correct `compileTaskPath`/`outputDir`); cross-project boundary resolution; the abstract base
  flattened to its two concrete subclasses with `expandedFrom`; and that `plan.commands` carries a
  target-neutral (`__GRADLE__`) unit-test batch command covering both projects. It runs with
  `--no-configuration-cache` (see P0) and is listed in `IntegTestCoverageArchUnitSpec.KNOWN_CC_INCOMPATIBLE`.
- **FLAKINESS_ITERS override via env - PASS.** `FLAKINESS_ITERS=7 flakinessScan` emitted a command with
  `-Dtests.iters=7` (no `-Pflakiness.iters` needed), proving the manual CI override works with no yml change.
- **Missing-refs error - PASS.** `flakinessResolve` with no `flakiness-refs.json` throws the clear
  "flakiness-refs.json not found at ...; pass -Pflakiness.refs=<path>" message, not Gradle's opaque one.
- **Real-build resolve - PASS, and the CC failure mode reproduced.** See P0: `2 refs -> 2 base targets across
  450 projects` without CC (both default and explicit `--no-configuration-cache`); empty (loud failure) with
  `--configuration-cache`. This proves populate->read works end-to-end and that the fail-fast guard fires.
- **Full 3-Gradle-invocation flow on the real build, ONE workspace - PASS (this is the point of the merge).**
  resolve (`:libs:dissect`, one changed-file + one unmute) -> `flakiness-base-targets.json` with authoritative
  `compileTaskPath`/`outputDir` + `flakiness-compile-tasks.txt` = `:libs:dissect:compileTestJava`; plain
  `./gradlew $(cat flakiness-compile-tasks.txt)` (BUILD SUCCESSFUL); `flakinessScan` reads the LOCAL compiled
  output and writes `flakiness-plan.json` with 2 concrete `run` entries. Because compile and scan ran in the same
  workspace, scan saw the compiled classes - which is exactly the cross-agent bug the single-step merge fixes.
- **`:build-tools-internal` compiles (main + test + integTest) - PASS.**
- **TS suite - PASS.** `npx vitest run scripts/flakiness-detection`: 11 files / 119 tests green, `tsc --noEmit`
  clean (asserts the two orchestration steps `flakiness-orchestration:{run,generate}`; the orchestration
  resolve/compile/scan-only shell with in-shell attribution; the separate node-agent generate step with
  `depends_on` orchestration `allow_failure: true`; `generate` reading the plan local-or-download, mapping
  `plan.commands` with `__GRADLE__` -> `.ci/scripts/run-gradle.sh`, no-op on missing plan; the quieter
  annotations; and `planCommandsToRunnable`/`withGradleBinary` for both targets).

### NOT run (per brief / environmental)
- The full ES build; the ArchUnit specs were not executed (`:build-tools-internal:spotlessJavaCheck` could not
  resolve `google-java-format` in this environment - unrelated to the change; `spotlessApply` ran and formatted
  the sources). The isolated-projects/config-cache/integ-coverage specs were read and the code written to satisfy
  them (no `getAllprojects`/`getRootProject`; tasks have no `getProject()`/`Project` fields; the plugin has a
  `*FuncTest`; both tasks have `*Tests`; the func test is baselined in `KNOWN_CC_INCOMPATIBLE`), but they are
  [unverified] by execution here.
- The Buildkite pipeline in CI (the yml + dynamic uploads are written and the pure `toResolvePipeline`/
  `toBuildkitePipeline` structure is unit-tested, but not run on a real agent).

## Honest assessment

- The **authoritative-model** pitch is now fully delivered: source-set shape, output dirs, compile task paths,
  and bwc are all read from each project's real configured model, with zero cross-project access. That is the
  substantive win over both the TS regexes and the prototype's half-realised P1a.
- The **riskiest** part is P0: the resolve step depends on the whole build being configured, which the
  configuration cache does not do for a root task, so resolve must run `--no-configuration-cache`. This is
  correct and cheap today, but it is a standing constraint - if a future change made the flakiness pipeline rely
  on CC for resolve, it would silently under-populate. The fail-fast guard turns that into a loud failure rather
  than the prototype's silent-empty, which is the key safety property.
- Residual risks: (1) the `rest-api-spec/test/<suitePath>.yml` yaml-suite layout is still encoded as a constant
  (it is an ESClientYamlSuiteTestCase-wide convention, not a per-project layout assumption, so low risk); (2)
  the config-pass cost is 3x; (3) the incremental `sourceSets.configureEach` registration relies on each test
  source set being *realized* during configuration - true under the whole-build (no-CC) config the resolve step
  runs, since the java/test plugins realize the source sets via their compile/test tasks. The earlier
  node-on-gradle-image risk is **resolved**: `generate` is now its own step on the default node-capable agent
  (only resolve/compile/scan run on the gradle image), so nothing assumes node is present on the gradle image.
