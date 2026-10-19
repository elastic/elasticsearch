# Transform plugin (transform)

Guidance for coding agents working in `x-pack/plugin/transform/` (Gradle `:x-pack:plugin:transform`, esplugin artifact `transform`). The repository-root `AGENTS.md` is authoritative for build toolchain, formatting, logging, transport-version, and general testing conventions — this file does not repeat them. Read the root `AGENTS.md` first; the notes below are transform-specific.

## What transforms promise (preserve these in every change)

A transform turns source indices into a summarized destination index — entity-centric (`pivot`) or latest-document-per-key (`latest`) — and, in continuous mode, keeps it eventually consistent with the source without anyone watching it. Changes must keep these guarantees:

1. **The destination is a recomputation of the source.** Exactly one dest document per `group_by` / `unique_key` bucket, with a deterministic `_id` from `IDGenerator`, written as a full-document replace (`DocumentConversionUtils`). An aggregation that yields several values per bucket writes them into that one document; never fan a bucket out into multiple documents (`AggregationResultUtils`).
2. **No silent data loss in continuous mode.** Checkpoint windows tile contiguously and half-open: from the previous checkpoint's stored upper bound to `now - sync.time.delay` (`TimeSyncConfig.getRangeQuery`). The delay is what protects documents that are indexed but not yet searchable; do not shorten, skip, or bypass it as an optimization.
3. **Re-processing is idempotent.** The pagination cursor (composite `after_key`) only advances after a page has been both searched and indexed; a failed page is retried from the same cursor. Retries and overlapping re-reads must remain safe.
4. **Optimizations never trade correctness for speed.** Model them on `align_checkpoints`: on by default, applied only where a precondition proves them safe, and exposed as a setting that can only *disable* them.
5. **Transforms run unattended.** Transient failures retry with backoff (`TransformFailureHandler`); permanent failures fail fast with a descriptive reason. Users learn about problems through `TransformAuditor` messages, `_stats`, and health — not the server log.
6. **Transforms run as their creator.** Source and destination access uses the stored headers/credentials of the user who created or last updated the transform (`ClientHelper.executeWithHeadersAsync`). Never access user data with elevated internal privileges; the transform origin alone is for the `.transform-*` internal indices.
7. **Mixed-version clusters keep working.** Configs persist in `.transform-internal-*` and `TransformTaskParams` live in cluster state. Gate new wire fields on a `TransportVersion`. Never throw from a `PersistentTaskParams.writeTo` (it breaks cluster-state publication to older nodes); reject unsupported requests in `masterOperation` instead. `TransformConfig.getVersion()` is stamped at PUT and can gate version-dependent behavior.

## Public contract: update together

When a change alters a request, response, config field, setting, or user-visible behavior, update:

- **REST spec JSON (this repo):** `rest-api-spec/src/main/resources/rest-api-spec/api/transform.*.json` and `cat.transforms.json`.
- **API specification (`elastic/elasticsearch-specification`):** `specification/transform/<api>/` for requests/responses; `specification/transform/_types/Transform.ts` for the config and per-transform `Settings`. Annotate new fields with `@availability` and `@server_default`. The API reference docs are generated from this repo.
- **Node/cluster settings docs (this repo):** `docs/reference/elasticsearch/configuration-reference/transforms-settings.md`.
- **Conceptual docs (`elastic/docs-content`):** `explore-analyze/transforms/` — limitations, checkpoints, scaling guidance.
- **Contract tests:** YAML REST specs in `src/yamlRestTest/resources/rest-api-spec/test/transform/`, and the xpack-core serialization tests (`AbstractChunkedSerializingTestCase` subclasses). A new transport-version-gated field must be nulled in `mutateForVersion` for every type that embeds it, or BWC serialization tests fail.

## Build & Test Commands

The Gradle project path is `:x-pack:plugin:transform`. Run from the repository root. The top-level project declares three test source sets: `test` (unit), `internalClusterTest`, and `yamlRestTest`. `javaRestTest`/`integTest` tasks belong to the `qa/` subprojects, not here.

```bash
# Unit tests (src/test/java) — extend ESTestCase / AbstractChunkedSerializingTestCase
./gradlew :x-pack:plugin:transform:test

# Single unit test class / method
./gradlew :x-pack:plugin:transform:test --tests org.elasticsearch.xpack.transform.TransformTests
./gradlew :x-pack:plugin:transform:test --tests org.elasticsearch.xpack.transform.TransformTests.methodName

# Internal cluster tests (src/internalClusterTest/java) — classes named *IT, extend TransformSingleNodeTestCase
./gradlew :x-pack:plugin:transform:internalClusterTest --tests "org.elasticsearch.xpack.transform.integration.*"

# YAML REST tests (src/yamlRestTest) — runner TransformYamlRestIT, uses DEFAULT distribution
./gradlew :x-pack:plugin:transform:yamlRestTest

# Format / forbidden-API / style gate (run before claiming Java work done)
./gradlew :x-pack:plugin:transform:spotlessApply
./gradlew :x-pack:plugin:transform:precommit
```

### QA suites (`qa/`) — REST/IT against provisioned clusters

Each is its own Gradle subproject; run only the relevant one (these are expensive).

| Suite | Command | Cluster |
|---|---|---|
| `common` | (library, not run directly) | provides `TransformCommonRestTestCase` base class for the others |
| `single-node-tests` | `./gradlew :x-pack:plugin:transform:qa:single-node-tests:javaRestTest` | 1 node |
| `multi-node-tests` | `./gradlew :x-pack:plugin:transform:qa:multi-node-tests:javaRestTest` | multi-node; includes continuous-transform tests |
YAML REST specs live in `src/yamlRestTest/resources/rest-api-spec/test/transform/` (CRUD, stats, continuous, latest, preview, reset, update, upgrade, unattended, CAT).

Multi-cluster (cross-cluster search) tests live in `src/yamlRestTest/resources/rest-api-spec/test/multi_cluster/` and `remote_cluster/`, and are run by `MultiClusterWithSecurityYamlTestSuiteIT` (via `./gradlew :x-pack:plugin:transform:yamlRestTest`).

## Plugin entry point

`Transform.java` is the single registration hub. It implements `Plugin`, `SystemIndexPlugin` (owns the `.transform-internal-*` config/state and `.transform-notifications-*` audit indices), and `PersistentTaskPlugin`. When adding a feature you typically register it here:

- `getActions()` — transport actions; `getRestHandlers()` — REST endpoints.
- `createComponents()` — builds `TransformServices` (config manager, checkpoint service, auditor, scheduler, transform-node tracker, cloud-credential manager) and returns it for DI.
- `getPersistentTasksExecutor()` — wires `TransformPersistentTasksExecutor`.
- Settings of note: `NUM_FAILURE_RETRIES_SETTING` (per-transform failure-retry cap) and `SCHEDULER_FREQUENCY` (how often the scheduler ticks) — see `Transform.java` for current defaults.

## xpack-core vs plugin split

This plugin `extendedPlugins = ['x-pack-core']`. The split mirrors other x-pack plugins:

- **`x-pack/plugin/core`** holds the serializable contracts: `*Action` `ActionType`s + request/response classes, `TransformConfig`, `TransformState`, `TransformCheckpoint`, `TransformStoredDoc`, `SourceConfig`/`PivotConfig`/`LatestConfig`/`SettingsConfig`, `TransformMessages`, `TransformMetadata`, and the `CheckpointProvider` interface.
- **This plugin** holds the `Transport*Action` handlers (server-side execution), the `Rest*Action` handlers, and the service implementations.

A new API is usually: config/POJO + `ActionType` in xpack-core → `Transport*Action` here → `Rest*Action` here.

## Core execution model (the multi-file big picture)

Each transform runs as a **persistent task** distributed across nodes by the master:

1. `TransformPersistentTasksExecutor.nodeOperation()` allocates a `TransformTask` (extends `AllocatedPersistentTask`) on the assigned node.
2. `TransformTask` wraps a `ClientTransformIndexer` (extends `TransformIndexer` → `AsyncTwoPhaseIndexer`). The indexer does **two-phase indexing**: search the source (paginated with a point-in-time for stable pagination) → apply the transform function → bulk-index into the destination. seqNo/primaryTerm give optimistic concurrency; failures retry up to `NUM_FAILURE_RETRIES_SETTING`.
3. **Batch vs continuous.** Batch runs one checkpoint to completion then stops. Continuous mode uses `TransformCheckpointService` + a `CheckpointProvider` to detect what changed in the source between checkpoints — `DefaultCheckpointProvider` (sequence numbers) or `TimeBasedCheckpointProvider` (timestamp sync). Checkpoints are persisted to `.transform-internal-*`.
4. **Scheduling.** `TransformScheduler` runs per-node on the generic thread pool, and fires `TransformTask.triggered(Event)` at the configured frequency to drive the next indexer iteration.
5. **Shared state.** `TransformContext` is the mutable runtime state shared between the task and indexer (task state, checkpoints, failure count, auth/credential state, "stop at next checkpoint").

State and config persistence go through `IndexBasedTransformConfigManager` (implements `TransformConfigManager`); audit messages go through `TransformAuditor` (queued, non-blocking, silenced during reset/upgrade mode), not just the logger.

## Transform function types

Two function implementations under `src/main/java/org/elasticsearch/xpack/transform/transforms/`:

- **Pivot** (`pivot/`) — groups source docs via a composite aggregation and computes aggregates per group.
- **Latest** (`latest/`) — keeps the newest document per configured key.

Both support source query filtering, runtime fields, and retention policies.

## Gotchas

- **Persistent-task allocation is subject to node churn.** Like other `PersistentTaskPlugin`s in this codebase, a `TransformTask` can be reassigned when its node is drained or relocated (routine in stateless/serverless autoscaling). Treat allocation failures and reassignments during drain as expected steady state — log at DEBUG/INFO — and reserve WARN/ERROR for retries actually exhausting `NUM_FAILURE_RETRIES_SETTING` or a checkpoint that cannot make progress at all.
- **`TransformContext` is the single source of mutable runtime state shared between the task and indexer.** Reaching around it (e.g. caching failure counts or "stop at next checkpoint" elsewhere) reintroduces the kind of bookkeeping split that has caused hard-to-reproduce races in sibling persistent-task systems in this codebase (see `x-pack/plugin/ml/AGENTS.md` Gotchas for the ML analogue). Keep task/indexer coordination state in one place.

## Testing conventions specific to transform

- Unit-test base classes: `ESTestCase`, `AbstractChunkedSerializingTestCase` (for x-content round-trip of serializable types).
- Internal cluster tests extend `TransformSingleNodeTestCase` (extends `ESSingleNodeTestCase`), are named `*IT`, and live under `...transform.integration` (also `checkpoint`, `auditor` subpackages).
- `yamlRestTest` requires the DEFAULT distribution.
- QA REST suites share `TransformCommonRestTestCase` from `qa/common`.
