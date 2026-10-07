> ## ⚠️ Design document — not for merge
>
> This PR exists **only** to review the design of test-coverage support across the team. It contains
> no implementation and is **not intended to be merged**. Once the design is agreed, this PR is closed
> and the work lands as the milestones described in §6, each as its own PR.
>
> **What to review**: the approach, the constraints, the milestone split, and the alternatives in §9.
> **What not to expect**: working code, tests, or a pipeline you can run.

# Design: Test coverage support in the Elasticsearch build

**Status**: proposed — for discussion, not for merge
**Audience**: `@elastic/es-delivery`, `@elastic/es-core-infra`
**Scope**: measuring JVM test coverage for selected Elasticsearch projects, as build logic

---

## 1. Motivation

The esql team has provided a PR for having esql specific test coverage capturing in https://github.com/elastic/elasticsearch/pull/155367 
This design should superseed the proof of concept in the listed PR and provide a maintainable approach that is usable not just by the esql team but
covers the whole elasticsearch codebase in the long run. 

We want coverage measurement that is:

- **repeatable** — the same numbers locally and in CI, from the same command;
- **reportable, never gating** — no thresholds, no build failures on a number;
- **honest** — a broken instrument must be distinguishable from genuinely low coverage;
- **native to the build** — ordinary convention plugins and tasks, not CI-side scripting.

### Non-goals

- Coverage gates or thresholds. A coverage gate is satisfiable by tests that execute without
  asserting, which is the wrong pressure to create.
- Coverage for BWC/upgrade suites. Old-version nodes execute old classfiles; their coverage cannot
  map onto current ones.
- Repo-wide coverage. Scope is opt-in per project and grows deliberately.

---

## 2. Constraints

| # | Constraint | Rationale |
|---|---|---|
| C1 | Convention plugins applied explicitly. **No init scripts.** | Init scripts are invisible to the project model, cannot be tested, and are incompatible with the direction of the build. |
| C2 | **Isolated Projects compliant.** No cross-project model access. | IP forbids reaching into another project's tasks/extensions/configurations. Depending on another project's *artifacts* remains legal. |
| C3 | **No embedded Python** in build or CI scripts. | Untestable, unpinned interpreter, undeclared dependency. |
| C4 | Third-party artifacts resolved through Gradle. | `gradle/verification-metadata.xml` (6,333 lines) pins every dependency; a coverage agent must not bypass it. |
| C5 | Configuration-cache friendly. | `gradle.properties` sets `org.gradle.configuration-cache.parallel=true`. |
| C6 | Do not depend on `TestClustersAware`. | Being removed. Legacy `TestClustersPlugin` projects are deprecated and out of scope. |

---

## 3. Background: why one custom component is unavoidable

Coverage must be collected from **two different kinds of JVM**, and only one is reachable by Gradle's
JaCoCo plugin.

### 3.1 Forked test JVMs — solved by Gradle

`test` and `internalClusterTest` execute product code inside the forked test JVM
(`internalClusterTest`'s cluster is in-process). Gradle's `jacoco` plugin instruments these directly:
`JacocoPluginExtension.applyTo` accepts any `T extends Task & JavaForkOptions`, which `Test` is.

### 3.2 ES node processes — not reachable by Gradle

REST suites (`javaRestTest`, `yamlRestTest`, `csvSpecTests`, `javaRestTestSecure`) execute product
code in **separate `elasticsearch` processes**. Those processes are launched by
`ProcessBuilder` from inside the *test JVM* (`ProcessUtils:125-144`), not by Gradle. They are
therefore neither a `Task` nor `JavaForkOptions`, and **Gradle's JaCoCo plugin can never instrument
them.**

Getting data *out* of a node cannot rely on the process exiting, for two independent reasons:

1. **Teardown is forcible.** `DefaultLocalClusterHandle.close()` calls `stop(true)`, which reaches
   `ProcessUtils.stopHandle` → `destroyForcibly` (`:188`). No shutdown hook runs, so JaCoCo's
   `dumponexit` never writes.
2. **File output is denied inside the node.** The entitlements runtime denies the JaCoCo shutdown
   hook's exec-file write (`NotEntitledException` on the agent's `FileOutput`).

Nor can the agent *listen* on a port: node JVM options are task-wide, so every concurrent node would
receive identical options and all but one would fail to bind.

**Therefore**: nodes run the agent in `output=tcpclient` and dial out to a collector that requests
dumps while nodes are alive and performs all file I/O in its own process. This collector is the one
genuinely custom component in this design; everything else is stock Gradle.

### 3.3 Entitlements: making the channel legitimate

`Socket::new` is instrumented and enforced against `OutboundNetworkEntitlement`
(`NetworkInstrumentation:169-180`). The JaCoCo agent runs in an unnamed module and resolves to
`UNKNOWN` scope, whose `defaultEntitlements(...)` grants nothing — so the tcpclient channel is
currently unsanctioned even where it happens to work.

There is an existing, reviewed precedent for a Java agent inside the node process: **the APM agent**.
`HardcodedEntitlements.agentEntitlements():230-245` **already grants `OutboundNetworkEntitlement`**,
and `EntitlementBootstrap:195` (the production path, which is what a test-cluster node runs) consumes
it. The only gate is a single hardcoded package prefix:

```java
// ScopeResolver:63
if (module.isNamed() == false && clazz.getPackageName().startsWith(apmAgentPackageName))
    return PolicyScope.apmAgent(ALL_UNNAMED);
```

The prefix is already a constructor parameter (threaded from `Elasticsearch.java:90`). Widening it
from one prefix to a collection, with `org.jacoco.agent.rt` added **only under test bootstrap**, makes
the channel declared and enforced. The existing comment above `agentEntitlements()` already
anticipates this generalisation.

- **Net production delta: zero.** The entitlement set is unchanged; only the test-time prefix list grows.
- Reuses the existing test-only signal `es.entitlement.testOnlyPath`
  (`ElasticsearchTestBasePlugin:198` → `TestEntitlementBootstrap:129`).
- **Known coarseness**: `ALL_UNNAMED` grants the full agent set (classloader creation, native
  libraries, outbound network) to any unnamed-module class under the prefix. Acceptable for a
  test-only agent and identical to how APM is treated today, but it should be called out in review.

---

## 4. Design

### 4.1 Layers

Coverage is reported per **layer**, because where product code executes changes what is measured. 

| Layer | Tasks | Product code runs in | Collection |
|---|---|---|---|
| `unit` | `test` | forked test JVM | file output, `dumpOnExit` |
| `internal-cluster` | `internalClusterTest` | forked test JVM (in-process cluster) | file output, `dumpOnExit` |
| `rest` | `javaRestTest`, `yamlRestTest`, `csvSpecTests`, `javaRestTestSecure` | **separate ES node processes** | tcpclient → collector |

The third layer is named `rest`, not `cluster`: every member is an `ESRestTestCase`-derived suite
driven over the REST API (`csvSpecTests` and `javaRestTestSecure` both run `EsqlSpec*IT` from the
`csvSpecTest` source set). "cluster" would also collide confusingly with `internal-cluster`, which
runs a real cluster too — just in-process. `rest` matches the vocabulary of the plugins the tasks come
from (`RestTestBasePlugin`, `RestIntegTestTask`, `InternalJavaRestTestPlugin`).

**Layers are merged, never averaged.** A line covered by two layers is one covered line, so any total
must be computed from the union of execution data. Measured on `esql-datasource-s3`: unit 69.1%,
rest 47.0%, union **78.7%** — adding gives 116%, averaging gives 58%; both are wrong.

Differencing a layer against the union answers *"if we dropped this suite, what goes dark." and allows 
us to see a trend in where the majority of our testing lives as we should aim for testing more on internalCluster layers
vs. the rest layer. 

### 4.2 Layer classification

Layer is derived from **test task name**, referencing plugin-defined constants rather than string
literals where they exist:

- `InternalClusterTestPlugin.SOURCE_SET_NAME` (`"internalClusterTest"`)
- `InternalJavaRestTestPlugin.SOURCE_SET_NAME` (`"javaRestTest"`)
- `InternalYamlRestTestPlugin.SOURCE_SET_NAME` (`"yamlRestTest"`)
- new constants for the ad-hoc names (`csvSpecTests`, `javaRestTestSecure`)

Task names must never be *guessed*: they do not reliably follow their source set — `src/csvSpecTest`
registers a task called `csvSpecTests`, plural.

Excluded, each for a stated reason:

| Excluded | Reason |
|---|---|
| names containing `#` (`v9.5.0#javaRestTest`, …) | BWC-versioned; old-version nodes, coverage cannot map onto current classfiles |
| `bcUpgradeTest` | same |
| `perfSmokeTest` | measures speed, not behaviour |
| legacy `TestClustersPlugin` projects | deprecated (C6) |

> **Note on classification robustness.** Only the *rest-vs-in-JVM* distinction is mechanically
> load-bearing — it decides whether a collector is attached. The `unit`-vs-`internal-cluster`
> distinction is a **label only**: both use identical mechanism, so a misclassification there
> mislabels a report and cannot lose data. Worth stating in the plugin javadoc so future maintainers
> know which half of the map is safety-critical.

### 4.3 Components

```
build-tools-internal/
  elasticsearch.coverage                  convention plugin, applied explicitly per project
  CoverageCollectorService                BuildService — owns collector lifecycle
  CollectorServer                         receives exec data from node JVMs over loopback TCP
  CoverageSummaryTask                     typed task: JacocoReport XML -> summary (text + markdown)
```

Nothing lives in CI scripts. Nothing is applied via `-I`.

#### `elasticsearch.coverage`

Applied **explicitly** in each measured project's `build.gradle` (C1). Responsibilities:

1. Apply Gradle's `jacoco` plugin; pin `toolVersion` from a declared dependency so the agent and CLI
   resolve through `gradle/verification-metadata.xml` (C4). No `curl`, no checksum management.
2. `tasks.withType(Test).configureEach { … }` — configure `JacocoTaskExtension` for in-JVM layers.
3. For rest-layer tasks, acquire a collector and inject the node argline (§4.4).
4. Register a `JacocoReport` task per layer plus a project-level union report.
5. Publish exec data as a **consumable variant** for later aggregation (§6, M3).

**Always applied, never conditional.** Instrumentation is gated by a lazily-evaluated build flag; when
off, `configureEach` realises nothing and the argline `Provider` resolves empty. Conditional *plugin
application* is rejected: it makes the project model depend on invocation, which is hostile to both
IP and the configuration cache (C2, C5). Configuration-time `System.getProperty` reads and
configuration-time directory creation are likewise rejected — the flag is a `Provider` from
`providers.gradleProperty(...)`.

#### `CoverageCollectorService` (BuildService)

A `BuildService` is the IP-sanctioned way to share infrastructure across projects (C2) — the designed
replacement for the cross-project coordination an init script would do. It provides:

- **Deterministic readiness.** Service construction is synchronous, so the listening port is bound
  before any consumer task runs. No sleep-then-probe race.
- **Guaranteed cleanup.** `AutoCloseable.close()` is invoked by Gradle, so a killed or cancelled build
  cannot orphan a collector.
- **Dynamic ports.** Bind port 0 and expose the assigned port. A fixed port would collide between
  concurrent Buildkite jobs *and* between concurrent test tasks in the same build
  (`org.gradle.parallel=true`).

**Granularity: one collector per rest-layer test task.** The service acts as a lifecycle-owning
factory, not a single shared server. Rationale:

- **Per-task exec files give per-project attribution for free**, which the aggregation variant (§6)
  requires. A single build-scoped collector would merge all projects into one file, or need
  session→task demultiplexing (extra argline plumbing for the same result).
- **A build-scoped collector's `close()` runs at build end**, which is *after* any report task would
  need its input. Per-task collectors make the exec file complete when the task completes, so
  `JacocoReport` is an ordinary consumer with no cross-build coordination.

Cost: one collector's threads per concurrent rest-layer task, inside the daemon. Acceptable, and the
reason the collector must be genuinely concurrency-safe and unit-tested.

### 4.4 Reaching node JVMs

`AbstractLocalClusterFactory:910` folds a task-level system property into each node's `ES_JAVA_OPTS`.
Rather than reuse `tests.jvm.argline`, we add a **dedicated, additive** coverage property and append
it in `commonOpts` (~3 lines in `:test:test-clusters`).

Why not reuse `tests.jvm.argline`:

- It is genuinely multi-consumer: `ElasticsearchTestBasePlugin:149` splits it onto the *test* JVM,
  `JvmInfoTests.isG1GCEnabled()` **parses it** for GC flags, and `ReproduceInfoPrinter:185` echoes it
  into reproduce lines. Appending `-javaagent:` to a property another test parses for correctness is a
  latent failure.
- Setting it clobbers any pre-existing value — a developer's `-Dtests.jvm.argline=...` would silently
  reach the test JVM but not the nodes.
- A single property would route coverage to both the test JVM and the nodes via two different code
  paths, creating a double-instrumentation hazard.

The property is injected via **`nonInputProperties`** (`ElasticsearchTestBasePlugin:98-115`), the
build's established mechanism for values that *"contain absolute paths and break cache
relocatability"*. This matters concretely: the agent jar path and the per-build collector port must
not enter the task's input snapshot, or every instrumented test task would be cache-invalidated by a
port change.

### 4.5 Reporting

Gradle's `JacocoReport` produces HTML/XML/CSV natively. It is a `@CacheableTask` taking
`executionData`, `classDirectories` and `sourceDirectories`. Consequences:

- No external CLI jar, no `curl`, no checksum pinning to hand-maintain (C4).
- The JaCoCo version is a normal dependency bump. **This matters**: the toolVersion must be ≥ 0.8.13
  for official Java 23/24 support, and the build currently bundles JDK 26 with a JDK 25 contributor
  toolchain. An agent that cannot parse a class-file version silently stops instrumenting that class,
  which surfaces as *low coverage* — i.e. as a finding — rather than as a tooling failure.
- `CoverageSummaryTask` reads the report XML and emits both a plain-text table and a Markdown table
  (the latter for the CI annotation, so it reflows and carries table semantics; a fixed-width
  `printf` table collapses in GitHub's proportional font).

The report's denominator is restricted to the measured project's own classes. Vendored sources are the
concrete hazard: `esql-datasource-orc` compiles `org.apache.*` shims into its main classes, which
would otherwise sit at a permanent 0% without measuring anything.

### 4.6 Failure behaviour

**Never gates on a number.** No thresholds.

**Publish only on green tests.** A report is produced only when the test tasks that feed it succeeded.
Partial coverage from a failed run is misleading: the denominator is intact but the numerator is
arbitrary, so the percentage moves for reasons unrelated to coverage. This is *all-or-nothing per
scope* — a green-projects-only report would let the denominator shrink silently, which reads as a
coverage change when it is a scope change.

Because a flake now discards a whole shard's measurement, coverage jobs should use the pipeline
generator's **default auto-retry** rather than disabling it.

**Fail loudly when the instrument did not attach.** An exec file that records nothing reports as 0%
and reads as a finding when it actually means the agent never attached — a mistake that has cost hours
on this codebase more than once. The check distinguishes two questions:

| Question | Kind | Action |
|---|---|---|
| Did an agent attach and dump? (≥1 session recorded) | infrastructure | **fail the build** |
| Are there >0 executed probes? | **result** | warn only |

The second must not fail: a project whose tests are all muted in `muted-tests.yml` legitimately
attaches an agent, opens a session, and records nothing. `MutedTestPlugin` drops matching tests before
the runner sees them, so this is a when-not-if in this repo.

Probe counts are read via the JaCoCo API — **never inferred from file size**. Healthy per-module exec
files have been ~220 bytes, while a sessions-only file can be arbitrarily large.

The check runs in the test task's **`doLast`**, alongside collector teardown. This keeps instrument,
collect and verify under one owner and needs no second task or re-declared inputs. It also has a
useful property: `doLast` only runs when the tests passed, so **any failure there is definitionally
infrastructural** and cannot be confused with a test failure. Note that a build-cache *hit* skips the
action along with the task — correct (nothing executed, nothing to verify), but it means the check is
a miss-path assertion, not an always-on invariant.

---

## 5. CI integration

Coverage runs opt-in, triggered by a label or trigger comment, and follows existing pipeline
conventions:

- **Invoke via `.ci/scripts/run-gradle.sh`**, like every other Gradle-invoking pipeline. Calling
  `$GRADLEW` directly silently loses several guarantees: `run-gradle.sh:3-4` installs
  `.ci/init.gradle`, which is **the only thing that enables the remote build cache**
  (`settings.gradle` merely applies the Develocity plugin); it sizes `MAX_WORKERS`; it drops page
  cache; and it wraps the build in `gradle-runner.jar`, which provides the **GCP Spot preemption
  watchdog and graceful build cancellation**.
- **Per-step commit statuses come from Buildkite's own integration.** No custom status publishing is
  needed: the integration already publishes statuses for group-nested and matrix-generated steps,
  including labels containing slashes (e.g. `elasticsearch-ci/detect changed tests`,
  `elasticsearch-ci/9.5.0 / Part 3 / bwc-snapshots`). Publishing statuses from inside a step is also
  actively harmful — any terminal-state call lives inside the step command, so a timeout or
  preemption leaves the status stuck `pending` forever with nothing to clear it.
- **Sharding follows the existing pattern**: named aggregate tasks in root `build.gradle` fanned out
  by a Buildkite `matrix`, exactly as `splitForCI*` handles splitting up check tasks across buildkite jobs today. 
  Shard by **project group, never by layer** — project shards are self-contained and correctly denominated,
  whereas layer shards must be re-merged on another agent, which is what makes denominators diverge.
- **Publishing is Buildkite artifacts + one annotation.** No S3, no repo pushes, no tokens.
  Time-series indexing to the es-delivery-stats cluster is deliberately deferred (§6).
- **Artifacts**: exec data and summary. Per-layer HTML is *not* uploaded from test shards — the report
  is rebuilt from the same exec data, and it is thousands of files (measured: 25 exec files behind
  4,000+ report files).

### 5.1 Periodic coverage pipeline

On-demand coverage answers *"what does this PR cover?"*. It cannot answer *"is coverage improving?"*,
because it only ever runs on branches where somebody asked. A trend needs measurements taken at a
regular cadence, on the same line of development, with the same scope — which is what a periodic
pipeline provides.

**`.buildkite/pipelines/periodic-test-coverage.yml`** mirrors the coverage that `intake.yml` provides
for correctness: the same breadth of test execution, on a schedule, against `main` and the active
release branches.

#### Relationship to `intake.yml`

Intake is the model for *scope*, not for *placement*. Coverage deliberately runs as its own periodic
pipeline rather than as extra steps inside intake:

| | `intake.yml` | `periodic-test-coverage.yml` |
|---|---|---|
| Trigger | every merge to a tracked branch | scheduled (see cadence below) |
| Purpose | gate correctness | observe a trend |
| Failure meaning | the merge broke something | the measurement did not complete |
| Test scope | `checkPart1`–`checkPart6`, BWC, packaging, rest-compat | the coverage-enabled subset of the same suites |
| Instrumentation | none | JaCoCo agent attached |

Three reasons not to add coverage steps to intake:

1. **Instrumentation changes what intake measures.** A JaCoCo agent alters timing, which is exactly
   the wrong thing to introduce into the suite that gates every merge — instrumented runs are slower
   and shift timing-sensitive test behaviour.
2. **Coverage must never gate** (§1). Intake steps are merge gates by construction; steps in intake
   that are not allowed to fail the build are a confusing exception to that contract.
3. **Cadence differs.** Coverage does not need per-merge granularity. Measuring every merge multiplies
   a large instrumented cost for a signal that moves slowly.

#### Shape

A plain (non-templated) pipeline file, following `periodic.weekly.yml` and
`periodic-micro-benchmarks.yml` rather than the generated `intake.yml`/`periodic.yml` family. Coverage
needs no BWC or FWC version expansion — BWC suites are explicitly out of scope (§4.2) — so there is no
reason to add a `*.template.yml` and a `writeBuildkitePipeline(...)` entry in root `build.gradle`.

The matrix mirrors `periodic.weekly.yml`'s `GRADLE_TASK` structure, fanned out over the coverage shard
tasks rather than `checkPart*`:

```yaml
steps:
  - group: test-coverage
    steps:
      - label: "{{matrix.GRADLE_TASK}} / test-coverage"
        command: .ci/scripts/run-gradle.sh --continue -Pcoverage=true
          -Dbwc.checkout.align=true -Dorg.elasticsearch.build.cache.push=true
          -Dignore.tests.seed -Dscan.capture-file-fingerprints {{matrix.GRADLE_TASK}}
        timeout_in_minutes: 300
        matrix:
          setup:
            GRADLE_TASK:
              - coverageCheckEsqlEngine
              # further shards added as scope grows (M3)
        agents:
          provider: gcp
          image: family/elasticsearch-ubuntu-2404
          machineType: n4-custom-32-98304
          diskType: hyperdisk-balanced
          buildDirectory: /dev/shm/bk
        artifact_paths:
          - "**/build/reports/jacoco/**/*.xml"
          - "**/build/reports/coverage-summary.txt"
```

The flags are taken from intake rather than invented:

- **`-Dorg.elasticsearch.build.cache.push=true`** — periodic pipelines on tracked branches populate the
  remote build cache. This is what makes on-demand PR coverage runs cheap: they read what the periodic
  run pushed. Note that this flag is the *only* thing that enables cache **writes**
  (`.ci/init.gradle` reads `push` from it), and it is combined with `--continue` so one failing shard
  does not prevent the others from contributing cache entries.
- **`-Dignore.tests.seed`** — intake passes this explicitly *"for cacheability"*; without it the random
  test seed becomes a task input and every run is a cache miss.
- **`-Dscan.capture-file-fingerprints`** — consistent with every other periodic/intake step, so build
  scans are comparable.
- **`--continue`** — per-shard independence (§4.6): a failure in one shard must not discard the others.

#### Cadence and branches

**Nightly on `main`** to start, extending to active release branches once the shard set stabilises.
Nightly rather than per-merge because coverage is a slowly-moving signal, and rather than weekly
because a week is too coarse to attribute a drop to a change set. Cadence is configured Buildkite-side
(pipeline schedules), not in the repo, matching the other `periodic-*` pipelines.

Branch selection matters for interpretation: a trend is only meaningful if every point measured the
same line of development. Mixing branches into one series produces steps that look like coverage
changes but are branch differences.

#### Failure semantics

The pipeline is **not a gate** and no commit status it publishes should be required. It has exactly one
legitimate failure mode: *the measurement did not complete*. Concretely:

- **Test failures** — the shard produces no report (§4.6). On a periodic run this is expected
  occasionally and is not actionable by whoever last merged, so the shard should retry on the standard
  transient exit statuses (`-1` agent loss, `47` preemption from `gradle-runner`, `agent_stop`) exactly
  as intake's steps do, and otherwise simply report no data for that run.
- **Instrument failure** — the attachment check (§4.6) fails the shard loudly. This *is* actionable and
  is the reason the check exists.
- **A missing run must be visible.** A trend that silently stops accumulating is worse than one that
  shows a gap: the numbers look current when they are stale. Once time-series publishing lands (§6, M3)
  this should be alerted on absence of recent data, not only on failure of the job that would have
  produced it.

#### Interaction with the milestones

The periodic pipeline is introduced in **M1** with a single shard, so the cadence, artifact paths and
cache-push behaviour are exercised early against low-risk in-JVM layers only. It grows by matrix entry
as M2 adds the rest layer and M3 adds shards — no structural change. It is also the natural producer
for M3's time-series documents: a scheduled run on a fixed branch is exactly the input a trend wants,
whereas indexing from on-demand PR runs would pollute the series with unrelated scopes and branches.

---

## 6. Milestones

Each milestone is independently landable and independently useful.

### M1 — per-project, in-JVM layers

`elasticsearch.coverage` applied to a small set of projects that test their own code
(`:x-pack:plugin:esql`, `:esql-core`, `:esql-datasource-s3`). Layers `unit` and `internal-cluster`.
Per-project `JacocoReport` + summary. Buildkite artifacts + annotation.

Includes the **periodic pipeline** (§5.1) with a single shard, so cadence, artifact paths and
cache-push behaviour are exercised from the start against low-risk in-JVM layers.

Deliberately carries **none** of: collector, `BuildService`, entitlements change, `:test:test-clusters`
change. **Fully Isolated-Projects clean** — no cross-project resolution of any kind. Validates the
reporting half at low risk.

### M2 — rest layer

`CollectorServer` as a `BuildService` (§4.3), node argline plumbing (§4.4), entitlements prefix
generalisation (§3.3), `doLast` teardown and attachment check (§4.6).

Requires `@elastic/es-core-infra` review for the `ScopeResolver` change. Proving ground:
`:esql-datasource-s3`, which hosts `src/javaRestTest` alongside `src/main` and is therefore
self-attributable without any cross-project work.

### M3 — cross-project aggregation

Union coverage across project boundaries, plus time-series publishing.

Aggregation uses **variant-aware dependency resolution**, not cross-project model access (C2). The
build already contains this exact pattern: `ElasticsearchJavadocPlugin:124-185` publishes a consumable
variant per project and resolves it from consumers via `ArtifactView`, with a javadoc that states the
intent — *"Registering the resolved artifact files as inputs wires the producing `:upstream:javadoc`
tasks via their `builtBy` metadata. This replaces `dependsOn(":upstream:javadoc")` and
`evaluationDependsOn(upstream)`."* Gradle's own `JacocoReportAggregationPlugin` works the same way.

Mechanism: producers publish `Category.VERIFICATION` + `VerificationType.JACOCO_RESULTS` (with
`MAIN_SOURCES` for sources), tagged by layer via `TestSuiteName`. All are first-class Gradle
attributes; no custom attributes required. Consumers resolve with `withVariantReselection()` and
`setLenient(true)` — lenience handles projects that publish no variant, e.g. qa/grouping projects with
no main sources.

Sharding restricts resolution with `ArtifactView.componentFilter` on `ProjectComponentIdentifier`
(used already in `DependenciesUtils` and `ElasticsearchJavadocPlugin:167`). Filtering by component
**before** artifact resolution is essential: registering resolved artifacts as task inputs is exactly
what pulls in the producing test tasks, so a lenient "list everything, use what appears" configuration
would make every shard build every project. Lenience tolerates *unresolvable* dependencies, not
"resolvable but please don't build it."

M3 also unblocks attribution for **test-host projects with no main sources**. `esql-datasource-orc/qa`
has no `src/main/java` at all — its purpose is owning `csvSpecTests`, with the code under test arriving
as `clusterPlugins project(xpackModule('esql-datasource-orc'))`. Until M3, that REST-derived coverage
cannot be attributed to the ORC classes. Per-project reporting makes this gap *visible* rather than
hiding it inside a union, which aligns with the strategic goal of moving away from qa projects that
exist solely to host test tasks.

Time-series publishing targets the es-delivery-stats cluster (`ES_DELIVERY_STATS_URL` /
`ES_DELIVERY_STATS_API_KEY`), and must ship with an **index template** — dynamic mapping would type
build numbers as `text`+`keyword` rather than numeric, which is painful to correct after the first
document lands. It needs **its own step gate**; it must not inherit `USE_ARCHIMEDES` or share a
credential bundle with unrelated tokens. Indexing failures must be surfaced as an annotation, not
log-only, or the trend data can silently stop accumulating with a green build.

Only the **periodic** pipeline (§5.1) should index. On-demand PR runs measure whatever scope the
requester chose, on an arbitrary branch, so indexing them would pollute the series with points that
are not comparable. This mirrors `periodic-micro-benchmarks.yml`, which guards indexing with an
explicit `if [[ "$BUILDKITE_BRANCH" == "main" ]]` check.

### M4 — drift detection

Scope membership is manually maintained in M1–M3; a new in-scope project is silently unmeasured until
someone adds it. M4 adds a `precommit` check asserting that every project matching a declared pattern
applies `elasticsearch.coverage`, appears in exactly one shard, and that shards are **disjoint** (an
overlapping project would be double-counted in any union).

Until M4 this is an accepted, documented risk. It is narrower than it appears: the layer mapping
(§4.2) still fails loudly on an unrecognised *task name*, so a new suite type cannot be silently
skipped — only a whole new *project* can be silently absent.

---

## 7. Testing

| Component | Test |
|---|---|
| `CollectorServer` | unit tests in `build-tools-internal` — concurrent connections, flush-on-close, no thread leaks across many tasks |
| Layer classification | unit tests over task-name sets, including `csvSpecTests` and `#`-variant exclusion |
| Attachment check | unit test: zero-session ⇒ fail; session-with-zero-probes ⇒ warn (the all-muted case) |
| Plugin wiring | `AbstractGradleFuncTest` — flag off ⇒ no agent; flag on ⇒ agent present; deliberately mis-wired argline ⇒ build fails with an instrumentation message |
| Pipeline generator changes | extend `pipeline.test.ts` with a mock pipeline and a snapshot case, per `.buildkite/scripts/pull-request/README.md`, which states mocks *"should try to cover all of the various features of the generator"* — including the empty-`changedFiles` case, whose semantics differ from neighbouring filters |

---

## 8. Consequences and risks

| Risk | Mitigation |
|---|---|
| Scope drift (new project unmeasured) | Accepted until M4; narrowed by loud failure on unmapped task names |
| Wall-clock cost of instrumented rest suites | Opt-in only; project sharding; per-invocation cost documented in the README |
| `ALL_UNNAMED` entitlement coarseness | Test-only, identical to APM's existing treatment; called out for review |
| Collector concurrency bugs | Per-task isolation, dynamic ports, unit tests, explicit teardown |
| Cache-hit skips the attachment check | Documented as a miss-path assertion; nothing executed means nothing to verify |
| Periodic pipeline silently stops producing data | Alert on **absence of recent data**, not only on job failure (§5.1) — a stale trend reads as a current one |
| Periodic instrumented runs add recurring CI cost | Nightly, not per-merge; sharded; cache-push makes on-demand PR runs cheaper in return (§5.1) |
| **Reported numbers lower than a cross-project union** | State prominently: per-project in-JVM coverage is a different, smaller, correct number. Measured on the ES\|QL surface, unit alone is 70.4% where the cross-project union is 79.0%. |

### Documentation to update

`TESTING.asciidoc:772-781` must be corrected as part of M1. Leaving it asserting that coverage is
impossible while shipping coverage support reproduces the exact failure this work exists to fix:
contributors read the canonical document, believe coverage is unavailable, and never look again.

---

## 9. Alternatives considered

| Alternative | Why rejected |
|---|---|
| Gradle `jacoco` plugin alone | Cannot instrument ES node processes — `applyTo` requires `Task & JavaForkOptions`; nodes are `ProcessBuilder` children of the test JVM (§3.2) |
| Agent writes exec files in the node | Impossible twice over: `destroyForcibly` runs no shutdown hook, and entitlements deny the write (§3.2) |
| `output=tcpserver` on nodes | Node JVM options are task-wide; concurrent nodes would fight over one port |
| Init script with `allprojects {}` | Violates C1 and C2; forces configuration of every project; not testable |
| Glob-matched project scope | Requires cross-project enumeration (C2) and duplicates scope logic across Groovy, shell and filesystem walks, which drift |
| Shard by test layer | Requires re-merging exec data on a separate agent, whose classfile set differs from the shards' — the direct cause of divergent denominators for identical execution data |
| Reuse `tests.jvm.argline` | Multi-consumer property that other tests parse and print; clobbers user values (§4.4) |
| Publish coverage to a git repo | Needs cross-repo write credentials and shrink-the-data preprocessing; a metrics cluster is the right home for a time series (§6, M3) |
| Custom commit statuses from inside steps | Unnecessary — Buildkite's integration already publishes per-step statuses — and leaves statuses stuck `pending` on timeout or preemption (§5) |
| Coverage steps added to `intake.yml` | Instrumentation perturbs the timing of the suite that gates every merge; intake steps are gates by contract whereas coverage must never gate; per-merge cadence is unnecessary for a slow-moving signal (§5.1) |
| Templated periodic pipeline (`*.template.yml`) | Templating exists for BWC/FWC version expansion, which coverage explicitly excludes (§4.2); a plain file matching `periodic.weekly.yml` avoids a root `build.gradle` generator entry (§5.1) |
| Index coverage from on-demand PR runs | Scope and branch vary per request, so points are not comparable; only the periodic pipeline should feed the series (§6, M3) |

---

## 10. References

- Prior art / origin: [PR #155367](https://github.com/elastic/elasticsearch/pull/155367) —
  *"ci: on-demand code coverage, reported per test layer and merged"*. Established the layer model,
  the union-not-average insight, the tcpclient collector approach, and the fail-loudly-on-zero
  principle; measured unit 70.4% / internal-cluster 52.9% / rest 55.5% / union 79.0% across ~263k
  executable ES|QL lines. This design keeps those conclusions and rebuilds the delivery mechanism as
  build logic.
- [#28867](https://github.com/elastic/elasticsearch/issues/28867) — the 2018 limitation behind the
  current `TESTING.asciidoc` text.
- [#109335](https://github.com/elastic/elasticsearch/issues/109335) — removes the need for the
  unnamed-module agent hack that §3.3 extends.
- `ElasticsearchJavadocPlugin` — in-repo template for IP-clean cross-project artifact aggregation.
- `BUILDING.md`, `TESTING.asciidoc`, `docs/internal/` — build and test documentation this design
  should be linked from.
