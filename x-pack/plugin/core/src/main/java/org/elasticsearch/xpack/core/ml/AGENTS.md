# ML contracts library (xpack-core/ml)

Guidance for coding agents working in the `org.elasticsearch.xpack.core.ml` package inside xpack-core. It holds the *contracts* (serializable config/state POJOs, transport action definitions, registries, cluster-state metadata, versioning), **not** runtime logic. The execution side (transport handlers, REST handlers, persistent-task executors, native processes) lives in `x-pack/plugin/ml` (see its own `AGENTS.md`). The repository-root `AGENTS.md` is authoritative for toolchain, formatting, logging, and transport-version rules — this file does not repeat them.

## Build & Test

This code is **not its own Gradle project** — it compiles and tests under `:x-pack:plugin:core` (esplugin `x-pack-core`). Run from the repository root.

```bash
# Unit tests for the whole core project (ML tests live under .../core/ml/)
./gradlew :x-pack:plugin:core:test

# Single test class / method
./gradlew :x-pack:plugin:core:test --tests org.elasticsearch.xpack.core.ml.job.config.JobTests
./gradlew :x-pack:plugin:core:test --tests org.elasticsearch.xpack.core.ml.job.config.JobTests.testMethodName

# Format / forbidden-API / style gate (run before claiming Java work done)
./gradlew :x-pack:plugin:core:spotlessApply
./gradlew :x-pack:plugin:core:precommit
```

`x-pack-core` applies `internal-test-artifact`, so the ML plugin and other consumers reuse these test base classes and helpers via `testArtifact(xpackModule('core'))`. Changing a shared test base class here affects downstream test compilation.

## What lives here vs. the ML plugin

| Here (`core/ml`) | In `x-pack/plugin/ml` |
|---|---|
| `ActionType` definitions + nested `Request`/`Response` | `Transport*Action` handlers, `Rest*Action` handlers |
| Config/state POJOs (`Job`, `DatafeedConfig`, `DataFrameAnalyticsConfig`, `TrainedModelConfig`, state enums) | Managers, indexers, persistent-task executors, native process I/O |
| Registries, `MlMetadata`, `MlTasks`, system-index defs | Business logic that reads/writes them |

Adding an API typically means: define the `ActionType` + `Request`/`Response` (and any config POJO) **here**, then implement the `Transport*Action` and `Rest*Action` in the plugin.

## Serialization & registry mechanics

Every contract type implements both `Writeable` (wire) and `ToXContent` (JSON), and ships a matching round-trip test (see Testing).

**Action class pattern** (e.g. `action/PutJobAction.java`, `action/GetJobsStatsAction.java`):

```java
public class FooAction extends ActionType<FooAction.Response> {
    public static final FooAction INSTANCE = new FooAction();
    public static final String NAME = "cluster:.../foo";   // scope-encoded name
    private FooAction() { super(NAME); }
    public static class Request  extends ... { /* StreamInput ctor, writeTo, toXContent, validate, equals/hashCode */ }
    public static class Response extends ... { /* same contract */ }
}
```

**Polymorphic types are registered in NamedXContent providers**, not ad hoc. When you add a new inference config, tokenization, data-frame analysis, evaluation metric, or inference result subtype, register it (NamedWriteable + NamedXContent) in the matching provider, or it will not deserialize:

- `inference/MlInferenceNamedXContentProvider` — inference configs, preprocessors, trained-model/tokenization types, results
- `dataframe/analyses/MlDataFrameAnalysisNamedXContentProvider` — Classification / Regression / OutlierDetection
- `dataframe/evaluation/MlEvaluationNamedXContentProvider` — evaluation metrics
- `ltr/MlLTRNamedXContentProvider` — learning-to-rank configs

## Parser duality (strict vs. lenient)

Most configs expose two `ObjectParser`s, and picking the wrong one is a recurring bug source:

- **`LENIENT_PARSER`** — used when reading **persisted documents or cluster state**. Ignores unknown fields (forward-compat) and parses internal/generated fields (`create_time`, version).
- **`STRICT_PARSER` / `REST_REQUEST_PARSER`** — used for **REST API input**. Rejects unknown fields (catches client typos) and excludes internal fields (clients cannot set them).

Configs are immutable with a nested `Builder`; validation runs at `build()` time and throws `ActionRequestValidationException` / `ElasticsearchException`.

## Versioning & backwards compatibility

`MlConfigVersion` is a **config-format version, distinct from `TransportVersion`**: it is human-readable and persisted inside documents and cluster state, so it tracks the schema of stored ML configs rather than the wire protocol. Post-8.10 it uses detached incrementing integer ids (the latest is `MlConfigVersion.CURRENT`), and each constant carries a unique id string to avoid duplicate-id git merge collisions; `MlConfigVersionComponent` surfaces it in node info.

For wire-format changes across mixed-version clusters, follow the root `AGENTS.md` "Backwards compatibility" section — do not restate that workflow here. Any change to a `Writeable`'s `writeTo`/`StreamInput` constructor needs a new named `TransportVersion` gate and a BWC round-trip test. Referable transport-version ids are global (`server/src/main/resources/transport/definitions/referable/*.csv`); after merging `main`, re-check the highest allocated id before keeping a pre-merge id on your branch (`./gradlew :server:generateClusterFeaturesMetadata` fails fast on a duplicate id at class-init).

## Cluster-state, tasks, system indices

- `MlMetadata` — `Metadata.ProjectCustom` cluster-state custom; holds `upgradeMode` and `resetMode` flags (parsed leniently for forward-compat).
- `MlTasks` — persistent-task name constants (`xpack/ml/job`, `xpack/ml/datafeed`, `xpack/ml/data_frame/analytics`, …), task-id prefix helpers, and task matchers/assignment constants.
- System indices: `MlConfigIndex` (`.ml-config`), `MlMetaIndex` (`.ml-meta`), `MlStatsIndex` (`.ml-stats-*` with the `.ml-stats-write` rollover alias). Bumping mappings means bumping the index mappings version.

## Domain-model map (`core/ml/<subpackage>/`)

- `job/` — anomaly-detection `Job`, `AnalysisConfig`, `Detector`, `DataDescription`, model-snapshot & state types.
- `datafeed/` — `DatafeedConfig`/`DatafeedUpdate`, chunking, delayed-data, cross-project routing, state.
- `dataframe/` — `DataFrameAnalyticsConfig`, source/dest; `analyses/` (Classification/Regression/OutlierDetection), `evaluation/` (metrics), `stats/`.
- `inference/` — `TrainedModelConfig`, model definitions; `trainedmodel/` (per-task InferenceConfig + tokenizations + ensemble/tree), `results/` (inference result types), `preprocessing/`.
- `calendars/` — `Calendar` + `ScheduledEvent` (maintenance windows that suppress anomalies).
- `annotations/` — `Annotation` + its system index definition.
- `autoscaling/` — ML node autoscaling policy/decision contracts.
- `ltr/` — learning-to-rank config contracts.
- `process/` — native-process result contracts (`DataCounts`, `ModelSizeStats`, `TimingStats`).
- `stats/` — aggregated usage stats (e.g. `ForecastStats`).
- `notifications/` — audit-message contracts.
- `packageloader/` — packaged-model (e.g. ELSER) loader contracts.
- `vectors/` — dense-vector/embedding metadata.
- `utils/` — shared validation/serialization helpers (`ExceptionsHelper`, `ToXContentParams`, `NamedXContentObjectHelper`, `RuntimeMappingsValidator`).

Top-level: `MachineLearningField` (ML settings/constants, `machine-learning` feature family, PLATINUM license), `MachineLearningFeatureSetUsage`.

## Gotchas

- **Pick the right parser.** Use `STRICT_PARSER`/`REST_REQUEST_PARSER` for API input and `LENIENT_PARSER` for persisted docs / cluster state. The wrong one either rejects valid stored data or lets clients set internal fields.
- **Register new polymorphic subtypes.** A new inference config, tokenization, data-frame analysis, evaluation metric, or result type must be added to the matching `Ml*NamedXContentProvider` (NamedWriteable + NamedXContent) or it will not deserialize.
- **Gate wire changes.** Any change to a `Writeable`'s `writeTo`/`StreamInput` constructor needs a new named `TransportVersion` gate **and** a BWC round-trip test (`AbstractBWC*SerializationTestCase`). Follow the root `AGENTS.md` "Backwards compatibility" workflow.
- **`MlConfigVersion` ≠ `TransportVersion`.** They version different things (stored config schema vs. wire protocol); don't substitute one for the other, and don't reuse a version constant's unique id string.

## Testing conventions

ML contracts are tested almost entirely with serialization round-trip bases — pick by what you're verifying:

- `AbstractXContentSerializingTestCase` — JSON/XContent round-trip (most common).
- `AbstractWireSerializingTestCase` — wire (`Writeable`) round-trip.
- `AbstractBWCSerializationTestCase` / `AbstractBWCWireSerializationTestCase` — backwards-compat across versions; use these when a type's wire/XContent format changed under a version gate.
- ML-local helpers: `org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase`, `AbstractChunkedBWCSerializationTestCase`.

Every new serialized contract type (or new field on one) needs a round-trip test; format changes need the BWC variant. Tests live under `x-pack/plugin/core/src/test/java/org/elasticsearch/xpack/core/ml/`, mirroring the main-source subpackages.
