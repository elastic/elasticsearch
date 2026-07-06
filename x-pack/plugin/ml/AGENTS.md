# Machine Learning plugin (x-pack-ml)

Guidance for coding agents working in `x-pack/plugin/ml/` (Gradle `:x-pack:plugin:ml`, artifact `x-pack-ml`). The repository-root `AGENTS.md` is authoritative for build toolchain, formatting, logging, transport-version, and general testing conventions — this file does not repeat them. Read the root `AGENTS.md` first; the notes below are ML-specific.

## Build & Test Commands

The Gradle project path is `:x-pack:plugin:ml`. Run from the repository root.

```bash
# Unit tests (src/test/java) — extend ESTestCase / MlSingleNodeTestCase
./gradlew :x-pack:plugin:ml:test

# Single unit test class / method
./gradlew :x-pack:plugin:ml:test --tests org.elasticsearch.xpack.ml.job.JobManagerTests
./gradlew :x-pack:plugin:ml:test --tests org.elasticsearch.xpack.ml.job.JobManagerTests.methodName

# Internal cluster tests (src/internalClusterTest/java) — start real clusters, classes named *IT
./gradlew :x-pack:plugin:ml:internalClusterTest --tests "org.elasticsearch.xpack.ml.integration.*"

# Format / forbidden-API / style gate (run before claiming Java work done)
./gradlew :x-pack:plugin:ml:spotlessApply
./gradlew :x-pack:plugin:ml:precommit
```

### QA suites (`qa/`) — REST/IT against provisioned clusters

Each is its own Gradle subproject. These are expensive; run only the relevant one.

| Suite | Command | Cluster |
|---|---|---|
| `single-node-tests` | `./gradlew :x-pack:plugin:ml:qa:single-node-tests:javaRestTest` (and `:yamlRestTest`) | 1 node, no security |
| `basic-multi-node` | `./gradlew :x-pack:plugin:ml:qa:basic-multi-node:javaRestTest` | 3 nodes, no security |
| `native-multi-node-tests` | `./gradlew :x-pack:plugin:ml:qa:native-multi-node-tests:javaRestTest` | 3 nodes, security + TLS; exercises native processes |
| `ml-with-security` | `./gradlew :x-pack:plugin:ml:qa:ml-with-security:yamlRestTest` | security + RBAC (`ml_admin`/`ml_user`/`no_ml` users) |
| `ml-inference-service-tests` | `./gradlew :x-pack:plugin:ml:qa:ml-inference-service-tests:javaRestTest` | inference/ingest coordination |
| `cps-datafeed-tests` | `./gradlew :x-pack:plugin:ml:qa:cps-datafeed-tests:javaRestTest` | cross-project/CCS datafeeds |
| `multi-cluster-tests-with-security` | `./gradlew :x-pack:plugin:ml:qa:multi-cluster-tests-with-security:integTest` | CCS across clusters |

YAML REST tests for ML live in `src/test/resources/rest-api-spec/test/ml/`. Use the root `AGENTS.md` YAML/CSV single-test invocation syntax.

## Native code dependency

This plugin sets `hasNativeController = true` and ships the C++ backend (`ml-cpp`) as a downloaded artifact (see `build.gradle`). The Java side **launches and communicates with native processes** rather than computing in-JVM. The Java↔C++ boundary is the most important architectural fact in this plugin:

- Anomaly detection, normalization, data-frame-analytics, and PyTorch inference each run in a separate native process.
- Each process type is created behind a `*ProcessFactory` abstraction (`AutodetectProcessFactory`, `NormalizerProcessFactory`, `AnalyticsProcessFactory`, `PyTorchProcessFactory`). **Tests use `BlackHole*` process implementations** (e.g. `BlackHoleAutodetectProcess`, `BlackHolePyTorchProcess`) to avoid spawning native code — follow this pattern; do not require `ml-cpp` in unit tests.
- The C++ backend lives in the separate `elastic/ml-cpp` repository and is consumed as a downloaded build artifact (see `build.gradle`). ML behavior is split across the Java plugin and the C++ backend; a change in process I/O framing usually requires coordinated edits on both sides.

## Plugin entry point

`MachineLearning.java` is the single, large registration hub. It implements many Elasticsearch plugin SPIs (`SystemIndexPlugin`, `AnalysisPlugin`, `CircuitBreakerPlugin`, `IngestPlugin`, `PersistentTaskPlugin`, `SearchPlugin`, `ShutdownAwarePlugin`, `ExtensiblePlugin`). When adding a feature you typically register it here:

- `getActions()` — transport actions
- `getRestHandlers()` — REST endpoints
- `createComponents()` — managers, services, factories (the wiring point for new services)
- `getExecutorBuilders()` — ML thread pools (`ml_datafeed`, `ml_job_comms`, `ml_native_inference_comms`, `ml_utility`)
- `getNamedXContent`/`getNamedWriteables`, `getSettings`, `getAggregations`

`xpack-core` holds the request/response/config classes and `ActionType` constants; this plugin holds the `Transport*Action` handlers and `Rest*Action` wrappers. A new API is usually: config + ActionType in xpack-core → Transport handler here → Rest handler here.

## Subsystem map

Four feature families, each orchestrated by a `*Manager` and backed by persistent tasks distributed across ML nodes:

| Feature | Package | Manager | Persistent task / process |
|---|---|---|---|
| Anomaly detection (jobs) | `job/` | `JobManager` | `OpenJobPersistentTasksExecutor`, autodetect process |
| Datafeeds (input pipelines for jobs) | `datafeed/` | `DatafeedManager` | `DatafeedRunner`; `DataExtractor`/`DataExtractorFactory` pull from indices (incl. CCS/cross-project — see Gotchas) |
| Data frame analytics | `dataframe/` | `DataFrameAnalyticsManager` | step pipeline: reindex → analysis → inference → final |
| Trained model inference | `inference/` | `DeploymentManager` | `TrainedModelDeploymentTask`, PyTorch process; `assignment/` allocates models to nodes |

Cross-cutting infrastructure: `process/` (native process management), `notifications/` (per-feature `*Auditor` classes writing audit docs), `autoscaling/` (ML node autoscaling), `aggs/` (ML-specific aggregations: categorization, change-point, correlation, frequent-itemsets, k-S test), `task/` (persistent-task executor base classes).

### Recurring patterns

- **Manager → Provider/Persister.** `*ConfigProvider` reads config from system indices; `*ResultsPersister`/`*ResultsProvider` write/read results. All use an origin-setting client (`ML_ORIGIN`) for system-level index access.
- **Persistent tasks.** Jobs/datafeeds/analytics/deployments are persistent tasks assigned to nodes by the master; see `AbstractJobPersistentTasksExecutor` and the `assignment/` rebalancer for trained models. Assignment can fail and be retried — assume node churn (especially in stateless/serverless, where nodes are drained and relocated routinely). This is expected steady state, not an incident: log assignment/allocation failures caused by routine node drain at DEBUG/INFO, and reserve WARN/ERROR for a cluster-wide inability to place work.
- **Auditing.** User-facing lifecycle events go through `AnomalyDetectionAuditor` / `DataFrameAnalyticsAuditor` / `InferenceAuditor`, not just the logger.

## Gotchas

- **Trained-model assignment bookkeeping must stay synchronous.** `TrainedModelAssignmentNodeService` keeps two structures in sync: the `deploymentIdToTask` map (fast "do we have this deployment?" guard) and the `TaskManager` registry (authoritative). If a stop path removes from one synchronously and the other asynchronously (e.g. on a utility thread), a cluster-state change in the gap can create a *second* native process for the same deployment while the first is orphaned and never told to stop. Keep `deploymentIdToTask.remove()` and `taskManager.unregister()` on the same thread/step in every stop path; only the expensive native-process shutdown itself may go async.
- **Zero-allocation deployments (`min_number_of_allocations: 0`) are not inert in cluster state.** On ML-node shutdown, the rebalance builder in `TrainedModelAssignmentClusterService` transitions existing routes to STOPPING — but for a deployment with no live routes on the draining node it can find nothing to transition and silently drop the assignment from cluster state instead of keeping it at zero allocations. Any shutdown/rebalance code path touching trained-model assignments must handle the "no routes to transition, but the assignment itself must survive" case explicitly.
- **Model id ≠ deployment id in assignment lookups.** `TrainedModelAssignmentMetadata` is keyed by deployment id (e.g. `.elser-2-elasticsearch`), not the public model id (e.g. `.elser_model_2_linux-x86_64`) surfaced by the `_inference`/trained-model APIs. Looking an assignment up by the wrong id returns `null` and fails callers with a transient (but alerting-noise-generating) 409 during scale-from-zero, even though the deployment exists and self-heals once the node joins.

## Testing conventions specific to ML

- Unit-test base classes: `ESTestCase`, `MlSingleNodeTestCase`, `org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase` (multi-node), `CategorizationTestCase`.
- Internal cluster tests are in `src/internalClusterTest/java/.../integration/` and named `*IT`.
- Do not spawn native processes in unit tests — use the `BlackHole*` process implementations.
- This plugin requires a Platinum/Trial license; test clusters set `xpack.license.self_generated.type = trial`.
