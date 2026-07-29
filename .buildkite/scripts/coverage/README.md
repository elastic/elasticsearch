# Code coverage

Measures coverage for any slice of the build, per test layer and merged.

`TESTING.asciidoc` says coverage cannot be done through Gradle. That was true in 2018, when the
custom `RandomizedTestingTask` did not extend Gradle's `Test` and the JaCoCo plugin could not see
test tasks (issue #28867, closed by the docs-only PR #29255 that wrote the current text). The build
moved to standard `Test` tasks and the limitation went with it. Nothing here uses the JaCoCo Gradle
plugin: the agent is attached as a plain `-javaagent` and the stock `jacococli` produces the
reports, so there is nothing coupled to test-task internals.

## Running it

Locally:

```bash
COVERAGE_PROJECTS=':x-pack:plugin:esql-datasource-*' \
COVERAGE_LAYERS=unit \
.buildkite/scripts/coverage/run-coverage.sh
```

In CI, on a PR: apply the `test-coverage` label or comment
`@elasticmachine run elasticsearch-ci/test-coverage` — one leg per layer, each annotating its own
numbers. The legs upload their raw exec data as artifacts; `publish.sh` merges them into the
union report (it is not wired into the PR pipeline — run it as a follow-up step or locally).

All parameters are build environment variables.

| Variable | Meaning | Default |
|---|---|---|
| `COVERAGE_PROJECTS` | Gradle project-path pattern | `:x-pack:plugin:esql-datasource-*` |
| `COVERAGE_INCLUDES` | JaCoCo class-include pattern | the ES\|QL packages |
| `COVERAGE_LAYERS` | `unit`, `internal-cluster`, `cluster` | all three |
| `COVERAGE_OUTPUT` | output directory | `build/coverage` |
| `COVERAGE_PORT` | collector port for node coverage | `6300` |
| `COVERAGE_EXCLUDE_PROJECTS` | regex over project paths to leave out; empty string excludes nothing | parquet-rs (being retired) |
| `COVERAGE_SKIP_PUBLISH` | `1` skips S3, keeps reports + annotation | unset |

Both patterns use their own tool's native syntax — Gradle project paths, JaCoCo class patterns.
There is no coverage-specific DSL to learn. The scope is a parameter; the mechanism never changes.

The report's denominator is restricted to the `COVERAGE_INCLUDES` packages: the agent only ever
records classes matching that filter, so anything outside it would sit in the report as a
permanent 0% without measuring anything (vendored `org.apache.*` shims in `esql-datasource-orc`
being the concrete case).

Projects and tasks are enumerated, not hardcoded, so a module that gains a suite is picked up
automatically. Projects are discovered the way `settings.gradle` discovers them (directories
carrying a `build.gradle`); test tasks are asked from the build itself
(`gradle/coverage-tasks.gradle`), because task names do not reliably follow their source set —
`src/csvSpecTest` registers `csvSpecTests`, plural. Each task name is then classified into a
layer by `COVERAGE_TASK_LAYERS` in `lib.sh`; an unmapped task name fails the run loudly rather
than being silently skipped. BWC-versioned suites (`v9.5.0#...`) are out of scope: old-version
node coverage cannot map onto current classfiles.

## The three layers

| Layer | Tasks | Where product code runs |
|---|---|---|
| `unit` | `test` | the forked test JVM |
| `internal-cluster` | `internalClusterTest` | the forked test JVM (cluster is in-process) |
| `cluster` | `javaRestTest`, `javaRestTestSecure`, `yamlRestTest`, `csvSpecTests` | **separate ES node processes** |

Each is reported on its own, and all are merged into a union.

**Layers are merged, never averaged.** A line covered by two layers is one covered line, so the
total has to be computed from the execution data. Measured on `esql-datasource-s3`: unit 69.1%,
cluster 47.0%, merged **78.7%**. Adding gives 116%; averaging gives 58%; both are wrong.

Per-layer reports also answer "if we dropped this suite, what goes dark" — difference a layer
against the merged report and what remains is the coverage only that layer provides.

## Why the cluster layer works the way it does

Product code for REST tests runs in ES node processes that Gradle does not launch, so the agent has
to reach them another way, and the coverage has to get back out.

`AbstractLocalClusterFactory` joins the test JVM's `tests.jvm.argline` into each node's
`ES_JAVA_OPTS`, which is the way in.

Getting data out cannot rely on the process exiting, for two independent reasons:

- Cluster teardown is forcible. `DefaultLocalClusterHandle.close()` stops nodes with
  `stop(true)`, which is `ProcessUtils.stopHandle` → `destroyForcibly`: no shutdown hook runs, so
  `dumponexit` never writes.
- Even when nodes are stopped gracefully, the entitlements runtime denies the JaCoCo shutdown
  hook's exec-file write inside the node process (`NotEntitledException` on the agent's
  `FileOutput`, observed 2026-07-29). File output from the node cannot work at all.

Nor can the agent listen on a port — `tests.jvm.argline` is task-wide, so every concurrent node
would receive the same options and all but one would fail to bind.

So nodes run the agent in `output=tcpclient` mode and dial out to `CollectorServer`, which requests
dumps every ten seconds while nodes are alive and does all file I/O in its own process. Nothing
depends on how a node dies. The node channel is armed only when the runner passes
`-Dcoverage.port` (cluster layer only), so no other layer carries an agent pointing at a dead port.

## Failure behaviour

**Never gates.** No thresholds, no build failure on a number. A coverage gate is satisfiable by
tests that assert nothing, which is the wrong pressure to create.

**Fails loudly on zero.** Every exec file is read back with `jacococli execinfo` and judged by its
executed-probe count — never by file size (healthy per-module exec files have been ~220 bytes;
sessions-only files can be arbitrarily large). An exec file that records nothing reports as 0% and
reads as a finding when it actually means the instrument is broken — a mistake made twice on this
codebase, costing hours each time. For the cluster layer the check distinguishes "no agent
connected" (missing `nodes.exec`) from "connected but recorded nothing" (present, zero probes),
because those have different fixes. Cluster-layer test-runner JVM files are reported but not
gated: they execute test code, not node code, and are legitimately near-empty. Test failures do
not discard a run — legs run with `--continue`, the publish step merges whatever exec data exists,
and the red leg stays red.

## Publishing

The publish step merges exec data across legs, rebuilds all reports against freshly compiled
classfiles (remote build cache makes that cheap), annotates the build with the summary, and syncs
the HTML to S3. S3 needs one-time ops setup: the `esql-coverage-reports` bucket and AWS keys at
Vault path `secret/ci/elastic-elasticsearch/esql-coverage-s3` (fields `access_key`/`secret_key`).
Until that exists, the step says so and the report is available from the step's Buildkite
artifacts instead.

## Files

| File | Purpose |
|---|---|
| `gradle/coverage.gradle` | init script; attaches the agent. Parameterised by target, fixed in mechanism. |
| `gradle/coverage-tasks.gradle` | init script; enumerates the real test tasks from the build |
| `lib.sh` | shared helpers: tool fetch, project/task enumeration, layer mapping, exec-file introspection |
| `CollectorServer.java` | receives coverage from node processes over TCP |
| `run-coverage.sh` | runs a layer's tasks, gates, reports |
| `check-nonzero.sh` | the zero gate |
| `summarise.sh` | headline numbers per layer |
| `publish.sh` | merges the legs' exec artifacts, rebuilds reports, publishes HTML, annotates the build |
| `../../pipelines/pull-request/test-coverage.yml` | the on-demand PR surface (label / trigger comment) |
