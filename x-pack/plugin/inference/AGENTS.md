# Inference API plugin (x-pack-inference)

Guidance for coding agents working in `x-pack/plugin/inference/` (Gradle `:x-pack:plugin:inference`, esplugin artifact `x-pack-inference`, class `InferencePlugin`). It implements the `_inference` API, integrates many external/internal inference services (see the provider subdirs under `services/`), and provides `semantic_text` and inference-based reranking. The repository-root `AGENTS.md` is authoritative for toolchain, formatting, logging, transport-version, and general testing conventions — this file does not repeat them. Read it first.

## This plugin vs. the ML plugin — don't confuse them

These two plugins both say "inference" but do different things:

| | `x-pack/plugin/inference` (this plugin) | `x-pack/plugin/ml` |
|---|---|---|
| Scope | The `_inference` endpoint: register an *inference endpoint*, call out to a service, get embeddings/completions/reranks | Anomaly detection, datafeeds, data-frame analytics, **local trained-model deployment** (PyTorch via `ml-cpp`) |
| Compute | Mostly **calls external HTTP APIs** (OpenAI, Cohere, Bedrock, …); the `elasticsearch` service runs models locally | Runs native processes on ML nodes |
| Key feature | `semantic_text` field, semantic query, reranking | jobs, datafeeds, DFA, trained-model assignment |

**The seam:** the internal `elasticsearch` service (`services/elasticsearch/`, e.g. ELSER and locally-hosted models) delegates to **ML-deployed trained models** — so a request through this plugin can end up running on the ML plugin's deployment infrastructure. The shared SPI lives in **server** (`org.elasticsearch.inference`); trained-model POJOs live in `xpack.core.ml.inference`.

## Where the SPI lives (important)

The core inference contracts are **not** in this plugin and **not** in xpack-core — they are in **server** at `server/src/main/java/org/elasticsearch/inference/`: `InferenceService`, `InferenceServiceRegistry`, `Model`, `ModelConfigurations`, `ServiceSettings`/`TaskSettings`/`SecretSettings`, `TaskType`, `InputType`, `InferenceServiceResults`, `ChunkingSettings`, `InferenceServiceExtension`. This plugin provides the *implementations*; other plugins integrate via `InferenceServiceExtension` (loaded through `ExtensiblePlugin`).

## Build & Test Commands

Gradle project `:x-pack:plugin:inference`. Run from repo root. `extendedPlugins = ['x-pack-core']`. Heavy third-party deps (AWS SDK v2, Google auth/api-client, Azure identity/msal4j, Nimbus OAuth/JOSE, Jackson, Netty, Reactor) — check `build.gradle` and the thirdPartyAudit allowances before adding more.

```bash
# Unit tests
./gradlew :x-pack:plugin:inference:test
./gradlew :x-pack:plugin:inference:test --tests "org.elasticsearch.xpack.inference.ModelConfigurationsTests"
./gradlew :x-pack:plugin:inference:test --tests "org.elasticsearch.xpack.inference.ModelConfigurationsTests.testSerialization"

# Internal cluster tests (*IT under ...inference.integration)
./gradlew :x-pack:plugin:inference:internalClusterTest

# YAML REST tests (specs in src/yamlRestTest/resources/rest-api-spec/test/inference/)
./gradlew :x-pack:plugin:inference:yamlRestTest

# Format / forbidden-API / style gate
./gradlew :x-pack:plugin:inference:spotlessApply
./gradlew :x-pack:plugin:inference:precommit
```

Unit-test base classes: `ESTestCase`, `AbstractWireSerializingTestCase`, `AbstractBWCWireSerializationTestCase`, and `MapperTestCase`/`MapperServiceTestCase` (for `semantic_text`).

### QA suites (`qa/`)

| Suite | Command | Purpose |
|---|---|---|
| `test-service-plugin` | (cluster plugin, consumed by others) | **Mock inference service** (`TestInferenceServicePlugin`) — the way to write service-level REST tests without real API keys |
| `inference-service-tests` | `:qa:inference-service-tests:javaRestTest` | provides `InferenceBaseRestTest` (`putModel`/`infer`/`deleteModel` helpers) |
| `inference-with-security` | `:qa:inference-with-security:yamlRestTest` | security enabled |
| `multi-node` | `:qa:multi-node:yamlRestTest` | multi-node cluster |
| `oauth2` | `:qa:oauth2:javaRestTest` | in-process mock OAuth2 server (skipped on FIPS) |
| `mixed-cluster`, `rolling-upgrade` | `:qa:<name>:vX.Y.Z#javaRestTest` | BWC / rolling-upgrade |

## Request lifecycle

REST handler (`rest/`) → Transport action (`action/`, base `BaseTransportInferenceAction`, with license check) → `ModelRegistry` lookup of the endpoint config → `InferenceServiceRegistry` dispatch to the concrete service → service runs inference (external HTTP or local) → `InferenceServiceResults` back to the listener. Streaming uses the chunked/streaming variants.

## Adding a new external service integration

1. Create `services/<name>/`. Implement the `Model` plus its `<Name>ServiceSettings` (non-secret config), `<Name>TaskSettings` (per-request knobs), and `<Name>SecretSettings` (API keys) — subtypes of the server SPI settings interfaces (base helpers in `services/settings/`).
2. Extend `SenderService` (`services/SenderService.java`, the base `InferenceService` impl) with a concrete `<Name>Service`; implement the `doInfer`/`doChunkedInfer`/`doUnifiedCompletionInfer` abstract methods.
3. Provide a `RequestManager` (builds the outbound HTTP request) and a service-specific `ResponseHandler` (parses the response into `InferenceServiceResults`).
4. Register the service factory in `InferencePlugin` and any `NamedWriteable`s in `InferenceNamedWriteablesProvider`.
5. Respect `ConfigurationParseContext` — `REQUEST` (strict, user API input) vs `PERSISTENT` (lenient, loaded from the index). Use `ServiceUtils`/`ServiceFields` helpers for map extraction and validation.

## External HTTP layer (`external/`) — traced flow

For services that call out over HTTP, a request flows:

1. `SenderService` calls `Sender.send(requestManager, inferenceInputs, timeout, listener)`.
2. `HttpRequestSender` lazily starts the async client/executor on first use, then enqueues to `RequestExecutorService`.
3. The executor applies **per-service rate limiting** and dequeues onto the `inference_utility` thread pool.
4. `RequestManager.buildRequest(...)` produces the outbound HTTP request.
5. `RetryingHttpSender` executes it with backoff on 429/5xx/timeouts.
6. `HttpClientManager` (Apache `HttpAsyncClient` connection pool, idle-eviction, XPack SSL) does the actual call.
7. The service's `ResponseHandler` parses the response into `InferenceServiceResults`, returned via the listener (responses delivered on the `inference_response` pool).

## Registry & system indices

`registry/ModelRegistry` persists endpoint configurations and syncs them from cluster state for fast `getModel(inferenceId)` lookups. Three system indices:

- `InferenceIndex` → `.inference` — endpoint configs (service, task_type, service_settings, task_settings, chunking_settings). Root mappings are strict; the settings sub-objects are `dynamic:false` to allow service-specific fields.
- `InferenceSecretsIndex` → `.inference-secrets` — credentials, kept in a **separate index** from config so secrets can be secured/backed-up independently.
- An Elastic-Inference-Service cloud-connected-mode index for EIS auth config.

## semantic_text & the search path

`semantic_text` makes embedding generation automatic across ingest and search:

- **Mapping:** `mapper/SemanticTextFieldMapper` defines the field and auto-creates dense- or sparse-vector subfields (plus chunk/text metadata) based on the referenced endpoint's task type.
- **Ingest:** `action/filter/ShardBulkInferenceActionFilter` intercepts bulk shard requests, batches inference over the configured `inference_fields` (chunking long text, respecting `INDICES_INFERENCE_BATCH_SIZE`), and injects the embeddings into the document source before normal indexing.
- **Search:** `queries/SemanticQueryBuilder` and the `Semantic{Knn,Match,SparseVector}QueryRewriteInterceptor`s auto-embed the query text at rewrite time and rewrite to native vector/sparse/match queries. `highlight/SemanticTextHighlighter` highlights matched chunks.
- **Reranking:** `rank/textsimilarity/TextSimilarityRankBuilder` reranks first-pass hits through a rerank endpoint.

## Gotchas

- **Register on both sides.** A new service/model/results type must be registered as a service factory in `InferencePlugin` **and** as `NamedWriteable`(s) in `InferenceNamedWriteablesProvider`, or it silently fails to deserialize from cluster state / the index.
- **Right parse context.** Honor `ConfigurationParseContext`: `REQUEST` (strict, rejects unknown fields — user API input) vs `PERSISTENT` (lenient — loaded from `.inference`). Using the wrong one breaks either client validation or forward-compat reads.
- **Secrets stay separate.** Credentials are written to `.inference-secrets`, never `.inference`. Don't fold secret settings into the config document.
- **SPI is in server.** The `InferenceService`/`Model`/settings/`TaskType` contracts live in server's `org.elasticsearch.inference`, not this plugin and not xpack-core — extend those, don't fork them.
- **Don't block.** External calls go through the async `Sender`/`HttpRequestSender` path on the inference thread pools; don't call services synchronously from a transport or cluster-state thread.

## Testing conventions specific to inference

- Serialization round-trips use the wire/BWC bases; `semantic_text` mapper tests use `MapperTestCase`/`MapperServiceTestCase`.
- To test service behavior end-to-end without real credentials, depend on `qa/test-service-plugin` (the mock `TestInferenceServicePlugin`) and extend `InferenceBaseRestTest` from `qa/inference-service-tests`.
- Internal cluster tests are named `*IT` under `...inference.integration`; YAML specs live in `src/yamlRestTest/resources/rest-api-spec/test/inference/`.
