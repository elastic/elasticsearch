# OTel audit log delivery — POC writeup for ES-14356

**Author:** Patrick Doyle
**Status:** Draft for review

The point of this writeup is twofold: (1) record what's been validated by this PoC and how to verify each claim, and (2) make the gap between PoC scope and a production-ready feature explicit, so estimates of remaining work are grounded.

## 1. Goal

Make Elasticsearch audit logs deliverable to serverless customers via OTLP. Today, audit logs are written to a per-cluster JSON file by `LoggingAuditTrail`; in self-hosted deployments, customers ship that file with Filebeat. Serverless has no Filebeat sidecar and no shared disk, so the file path doesn't apply.

The cross-team plan: emitting services (ES, Kibana, Control-Plane services) send OTLP log records over the network to an `otel-delivery-gateway` running in the same ECP K8s cluster. That gateway routes records to per-project MOTel endpoints, which write to the customer's destination Elasticsearch project. Architecture is documented in the gateway TDD (see §2 references).

This PoC validates that ES can emit OTLP audit logs end-to-end and surfaces the work that remains.

## 2. Requirements

Each requirement below points to a source document or comment so reviewers can verify the requirement, not just our reading of it. Italicized requirements have evidence in §3 that the PoC satisfies them; the rest are gaps tracked in §4.

### 2.1 Source documents

- **Audit TDD** — *TDD - Audit Logging in Elasticsearch* (Ankit Sethi). [Google Doc](https://docs.google.com/document/d/1ox4Hc-Ga_HMQtpFNzvBivHbn0khBOzzL4hN6WsMH8ko/edit). Frames the audit-logic-side concerns (filtering, redaction, namespacing) as the broader audit-in-serverless effort. ES-14356 is the *delivery* slice of this; most of the audit TDD's eight requirements are owned outside this PoC (see §2.4).
- **Gateway TDD** — *Shipping Logs to Elastic Cloud customers (TDD)*. [Google Doc](https://docs.google.com/document/d/12IZxcX5uoFWvIhfff1I3DwRVMt8ab6tNkaIKs-Fx8Y8/edit). Frames the cross-team architecture from emitting service to MOTel.
- **ES-14356** — parent epic for the ES-side delivery work. [Jira](https://elasticco.atlassian.net/browse/ES-14356).
- **ES-13255** — broader "ES logs over OTel". [Jira](https://elasticco.atlassian.net/browse/ES-13255). Tracks any non-audit ES log streams; out of scope here.
- **elasticsearch-team#2170** — Tim Vernum's brain-dump on audit-in-serverless work areas. [GitHub](https://github.com/elastic/elasticsearch-team/issues/2170).

### 2.2 Functional requirements

What the system must *do* — observable behaviors with pass/fail criteria.

| ID | Requirement | Source |
|---|---|---|
| **R1** | *Audit events from `LoggingAuditTrail` must be emitted via OTLP towards the local `otel-delivery-gateway`.* | Audit TDD §"Technical Dependencies" ("ES Core/Infra will take on adding support for a new Log4J appender that ships logs over the wire"); Gateway TDD §"How services emit (audit) log records" ("Must emit logs through the otlp protocol, on the network, towards the local otel-delivery-gateway service"). |
| **R2** | *Records must use OTel semconv field names (semconv → ECS → custom fallback) and the OTel JSON top-level structure (`@timestamp`, `attributes`, `body`, `resource.attributes`).* | Gateway TDD §"Standardized Audit Log schema" + §"How services emit"; Audit TDD §"Requirement 8". |
| **R3** | *Audit events must be enriched with `project.id` so per-project routing can work.* | Audit TDD §"Requirement 7"; Gateway TDD example schema in §"Standardized Audit Log schema" / Appendix 5. |
| R4 | Authenticate to the gateway via mTLS (client cert proves source identity). | Gateway TDD §"OTel log delivery pipeline on ECP" — Authentication & authorization. |
| **R5** | *Transport over OTLP.* (PoC already uses OTLP/HTTP-protobuf, the planned production target. The gateway TDD parenthetically says "(grpc)", but the project's stance is to push back on gRPC and stay on HTTP unless a concrete reason for gRPC surfaces.) | Gateway TDD §"How services emit (audit) log records" — *"Must emit logs through the otlp protocol, on the network, towards the local otel-delivery-gateway service"*; §"OTel log delivery pipeline on ECP" — *"via the otlp protocol (grpc)"* (parenthetical, not load-bearing). |
| R6 | On serverless, strip cluster identity (`cluster.name`, `cluster.uuid`, `node.name`, `node.id`) at the source. | Audit TDD §"Requirement 2" — strip the four fields from `log4j2.serverless.properties`. |
| R7 | Fall back to stdout when retries to the gateway are exhausted, so the internal observability pipeline can replay records. | Gateway TDD §"Unavailability issues mitigation" point 1. |
| R8 | Filter audit events per project ID (drop events from projects whose customers have opted out). | **Placement** (must be ES-side; downstream components can't filter per project today): [#hosted-otel-collector thread, 2026-04-29](https://elastic.slack.com/archives/C076MUD9BK8/p1777470066015769) — Vignesh Shanmugam: *"the processing/redact bits should live as part of the SDK layer inside ES for now till we have support for managed processing layer"*; Julien Lind concurred filtering is not MOTel's responsibility; Andrew Wilkins flagged the `otel-delivery-gateway` as the eventual external home for transformations. **Durable reason** (ES-side even if downstream filtering becomes possible): cost — emitting for N projects only to drop most of it downstream still pays the ES → gateway network hop. Patrick Doyle's framing; pending formal capture. |
| **R9** | *The emit path must not block transport or network threads.* (Bounded latency on the calling thread, regardless of gateway availability.) | Henning Andersen comment on Audit TDD: *"I remember you had thoughts on how shipping audit logs could work in a way that did not require a new shipper, still allowed spill to disk and would not block transport threads. This seems to be a significant part of the work unless this is considered separately."* Tim Vernum reply: *"Will the Core/Infra work guarantee that no blocking work is done on transport threads? I can't see where it's explicitly mentioned in this doc or the ticket referenced above."* Underlying rationale: Audit TDD §"Why is audit logging not already enabled?" point 2 — *"Writing to disk occurs on transport threads, adding a source of instability."* |
| **R10** | *Best-effort delivery: a failed log send must not fail the originating operation.* | Gateway TDD §"Reliability & throughput" — *"From the application/service perspective, we still move forward with executing an operation if writing the audit log fails."* + §"How services emit" — *"Should consider audit log delivery as 'best effort'..."* |
| R11 | Delivery semantics are at-least-once: records are not silently dropped during normal operation; occasional duplicates are acceptable. | Gateway TDD §"Reliability & throughput". |
| R12 | Configuration changes (in particular: R8 per-project state, plus cluster-level on/off owned separately) take effect without cluster restart. | For per-project (R8): the same cost rationale that makes R8 ES-side applies here too — toggling a project off must not require a restart, since both ops cadence and cost reasoning depend on it being cheap. For cluster-level: Audit TDD §"Requirement 1" — *"... enable or disable audit logging with a simple setting on the Cloud console without any downtime (cluster restart)."* In flight via [PR 147333](https://github.com/elastic/elasticsearch/pull/147333) for `xpack.security.audit.enabled`. |

### 2.3 Non-functional requirements

Quality and resource constraints — *how well* the system performs the behaviors above.

| ID | Requirement | Source |
|---|---|---|
| NF1 | Retry policy parameters and in-memory buffer size targets (e.g. ~3 retries up to ~2 minutes; buffer 30–50 MB). | Gateway TDD §"How services emit"; §"OTel log delivery pipeline on ECP"; §"Unavailability issues mitigation". The 4 MB/s per-container source-rate forecast in the gateway TDD's Throughput section is an input for sizing this buffer (~10 s of headroom at the spec'd rate), not an ES requirement on its own. |

### 2.4 Non-requirements (explicitly out of scope for ES-14356)

These are real concerns, but they belong to other tracks. Listed here so the reader can see what *isn't* on us.

- **The existing `audit_rolling` file appender is unchanged.** Customers consuming `<cluster>_audit.json` with Filebeat keep doing so. Making `audit_rolling` non-blocking is also out of scope.
- **Non-audit ES log streams** (server, deprecation, slowlog, ESQL). Tracked under ES-13255.
- **Cluster-level dynamic enable/disable of audit logging** (Audit TDD Req 1; in flight via [PR 147333](https://github.com/elastic/elasticsearch/pull/147333)). Per-project granularity (NF10) is on us; cluster-level is not.
- **Origin-type / realm filtering of internal traffic.** Audit TDD Reqs 3, 4. Owned by Ankit Sethi; tracked under elasticsearch-team#2170.
- **Operator-privilege placeholder for Elastic-employee access.** Audit TDD Req 5.
- **UIAM / CPS audit instrumentation.** Audit TDD Req 6.
- **Field namespacing currently done by Filebeat** (e.g. `user.realm` → `elasticsearch.audit.user.realm`). Audit TDD Req 8; lives in `log4j2.serverless.properties`, not here.
- **Internal infosec cluster delivery.** Gateway TDD §"Out of scope".
- **Production integration test against a real `otel-delivery-gateway`.** PoC's IT uses an in-process recording server.
- **The 99.9% delivery SLO** is a *system-level* property the ES side cannot uphold on its own — it depends on gateway, MOTel, and destination cluster availability. ES contributes to it via R7 (stdout fallback), R9 (non-blocking emit), R10 (best-effort), R11 (at-least-once), and NF1 (retry/buffer sizing); it is not a standalone ES requirement.

## 3. What's implemented (with evidence)

The headline result: an audit event emitted by `LoggingAuditTrail` reaches a recording OTLP server end-to-end, in a security-enabled multi-node test, with `project.id` attached and the OTel logs SDK on the wire path. Each claim below is anchored to a test, file, or log line so reviewers don't need to take any of it on faith.

### 3.1 End-to-end OTLP delivery — *satisfies R1*

**Evidence:** `OtelAuditLogsIT.testAuditEventArrivesAsOtlpLogRecord` in `test/external-modules/apm-integration/src/javaRestTest/java/org/elasticsearch/test/apmintegration/OtelAuditLogsIT.java:77`. The test boots a security-enabled `ElasticsearchCluster` with `xpack.security.audit.enabled=true`, `telemetry.otel.logs.enabled=true`, and the gateway endpoint pointed at an in-process `RecordingApmServer`. It hits `/_security/_authenticate` (which produces an `authentication_success` audit event), forces a flush via `/_flush_telemetry`, and asserts a `ReceivedTelemetry.ReceivedLog` arrives within `TELEMETRY_TIMEOUT`.

**Pipeline construction:**

- `OtelSdkExportLogsSupplier.install()` (`modules/apm/src/main/java/org/elasticsearch/telemetry/apm/internal/export/otelsdk/OtelSdkExportLogsSupplier.java:65`) builds the OTel SDK (`SdkLoggerProvider` + `OtlpHttpLogRecordExporter` + `BatchLogRecordProcessor`), constructs an `OpenTelemetryAppender` programmatically, and attaches it to the audit logger's `LoggerConfig` (line 111). This must happen programmatically — see Appendix A.2 for why declaring the appender in `log4j2.properties` doesn't work.
- `APM.createComponents()` (`modules/apm/src/main/java/org/elasticsearch/telemetry/apm/APM.java:69`) wires the supplier into the plugin lifecycle and exposes it via `APMTelemetryProvider`.
- Settings `telemetry.otel.logs.enabled` and `telemetry.otel.logs.endpoint` registered in `OtelSdkSettings` and added to the plugin's setting list.
- `attemptFlushLogs()` plumbed through `TelemetryProvider` and `APMTelemetryProvider` so tests and graceful shutdown can force-flush the `BatchLogRecordProcessor`.

### 3.2 Non-blocking on the calling thread — *satisfies R9; partially satisfies R10, R11*

**Evidence:** the OTel SDK is used unmodified, and its publication path provably does not block. Trace:

1. `LoggingAuditTrail` calls `logger.info(AUDIT_MARKER, logEntry)` on a transport/REST thread. log4j2 dispatches via `LoggerConfig.log(...)` → `callAppenders(...)` → `OpenTelemetryAppender.append(LogEvent)` on the *same calling thread*.
2. `OpenTelemetryAppender.append` does only the log4j → OTel record translation (CPU only) and calls `LogRecordBuilder.emit()`.
3. `SdkLogRecordBuilder.emit()` invokes `getLogRecordProcessor().onEmit(...)` — i.e. `BatchLogRecordProcessor.onEmit`.
4. `BatchLogRecordProcessor.onEmit` calls `worker.addLogRecord(...)`, which `offer`s the record onto a bounded `ArrayBlockingQueue` (default capacity 2048). On full, `offer` returns false and the record is **dropped**, not held.
5. The actual OTLP HTTP POST runs on a worker thread the `BatchLogRecordProcessor` owns, never on the caller.

Consequences:

- **No back-pressure to the caller.** A slow or unreachable gateway produces drops, not blocked transport threads.
- **No I/O on the caller.** Network and serialization happen on the worker thread.

Per the [OTel logs SDK spec](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/logs/sdk.md#onemit), `LogRecordProcessor.OnEmit` *SHOULD NOT block* and overflow drops rather than back-pressures. We are in spec.

This is a property we *inherit* from the OTel SDK, not one we engineered into ES. There is no ES-side code to point at; the evidence is the call chain above plus the spec.

### 3.3 `project.id` enrichment — *satisfies R3*

**Evidence:**

- `LoggingAuditTrail.PROJECT_ID_FIELD_NAME` constant at `x-pack/plugin/security/src/main/java/org/elasticsearch/xpack/security/audit/logfile/LoggingAuditTrail.java:214`.
- One line in `LogEntryBuilder.withThreadContext(...)` at line 1662: `setThreadContextField(threadContext, Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, PROJECT_ID_FIELD_NAME);` — reads the project header from `ThreadContext` and adds it to the audit `StringMapMessage`.
- Audit JSON pattern updated in `x-pack/plugin/core/src/main/config/log4j2.properties:47` so the field appears in the file appender too.
- `LoggingAuditTrailTests` covers it at lines 363 and 3333–3334 (sets the header on `ThreadContext`, asserts the event carries `project.id`). The full class (35 tests) passed against this change at last run.

### 3.4 Semconv field names arrive on the wire — *partially satisfies R2*

**Evidence:** `OtelAuditLogsIT` lines 109–110 assert that `event.action` and `event.type` are present on the received `LogRecord` attributes. So the *fields* survive the log4j → OTel hop end-to-end.

**Caveat:** they arrive prefixed — `log4j.map_message.event.action` — because the upstream `OpenTelemetryAppender` library hardcodes that prefix when capturing `StringMapMessage` entries with `setCaptureMapMessageAttributes(true)`. Bare-key shape is not yet satisfied; see §4.1.1 below for options.

### 3.5 Negative evidence (no regressions)

- `:modules:apm:thirdPartyAudit` clean — no jar-hell or banned-API regression from the new `opentelemetry-sdk-logs` and `opentelemetry-log4j-appender-2.17` deps. (See Appendix A.3 for the jar-hell episode that drove the module placement.)
- **OTLP/HTTP-protobuf wire encoding** is what the PoC actually uses, matching the planned production target. Exporter: `OtlpHttpLogRecordExporter.builder().setEndpoint(endpoint)` (`OtelSdkExportLogsSupplier.java:77`) — the default encoding for that class is protobuf, not JSON. Confirmed on the receiving side by `OtlpLogsParser.parse(...)` (`OtlpLogsParser.java:35–36`) which deserializes via `ExportLogsServiceRequest.parseFrom(InputStream)` (protobuf).
- `OtelSdkExportLogsSupplierTests` 7/7 — covers install/uninstall lifecycle of the supplier (idempotent install, detach on close, behavior when audit logger config is absent).
- Boots cleanly under `./gradlew run` with audit + OTel logs enabled. Supplier emits `OTel SDK logs export installed; endpoint=...` (`OtelSdkExportLogsSupplier.java:117`); node reaches `started`.
- `manage_threads` entitlement registered in `modules/apm/src/main/plugin-metadata/entitlement-policy.yaml` for `io.opentelemetry.sdk.logs` — without this, `BatchLogRecordProcessor`'s worker thread fails to start with `NotEntitledException`. (See Appendix B.)

### 3.6 Files changed (summary)

| Where | Purpose |
|---|---|
| `modules/apm/build.gradle` | Promote `opentelemetry-sdk-logs` to `implementation`; add `opentelemetry-log4j-appender-2.17`. |
| `modules/apm/.../OtelSdkSettings.java` | New settings: `telemetry.otel.logs.enabled`, `telemetry.otel.logs.endpoint`. |
| `modules/apm/.../OtelSdkExportLogsSupplier.java` (new) | SDK construction, programmatic appender attach, idempotent `install()`, `forceFlush()`. |
| `modules/apm/.../APM.java` | `createComponents` wires the supplier and registers it in `APMTelemetryProvider`. |
| `modules/apm/src/main/plugin-metadata/entitlement-policy.yaml` | `manage_threads` entitlement for `io.opentelemetry.sdk.logs`. |
| `server/.../telemetry/TelemetryProvider.java` | `attemptFlushLogs()` (default no-op). |
| `modules/apm/.../APMTelemetryProvider.java` | Implements `attemptFlushLogs()`. |
| `x-pack/plugin/security/.../LoggingAuditTrail.java` | `PROJECT_ID_FIELD_NAME` constant + one `setThreadContextField` line. |
| `x-pack/plugin/core/.../log4j2.properties` | `project.id` added to audit JSON pattern. |
| `build-tools/.../testclusters/MockApmServer.java` | New `/v1/logs` handler. |
| `test/external-modules/apm-integration/.../{ReceivedTelemetry,OtlpLogsParser,RecordingApmServer}.java` | `ReceivedLog` record, OTLP parser, route. |
| `test/external-modules/apm-integration/.../OtelAuditLogsIT.java` (new) | End-to-end IT. |
| `test/external-modules/apm-integration/.../FlushTelemetryRestHandler.java` | `/_flush_telemetry` flushes logs as well as metrics/traces. |
| `gradle/verification-metadata.xml` | Auto-regenerated for the new artifact. |

## 4. What's not implemented (gap analysis)

For each requirement we don't yet satisfy, what's needed to make it real.

| # | Requirement | Gap |
|---|---|---|
| R2 (full) | Bare semconv keys, no prefix | `log4j.map_message.` prefix from upstream appender. See §4.1.1 for options (custom appender ~50 LOC vs. v3-preview wrapper vs. AutoConfigured SDK refactor). Decision needed before further audit-event mapping work. |
| R2 (mapping) | Field-by-field semconv translation | See §4.1.2 for per-field decisions (`origin.address` split, `apikey.*` namespace, `indices` array, `request.body`, nested `security_config_change` blobs). Mechanical for ~half the fields; non-trivial for the rest. |
| R4 | mTLS to the gateway | PoC uses HTTP without TLS. Production: client cert distributed by Control-Plane (Vault → cert-manager → secret mount). Reuse pattern: `SSLService` + `PemKeyConfig` + `SSLConfigurationReloader` (used by monitoring HTTP exporter and Watcher's HTTP client). Need to verify whether `OtlpHttpLogRecordExporterBuilder` exposes client TLS config in our OTel-Java version, or whether we wrap the underlying HTTP client. |
| R6 | Strip cluster/node fields on serverless | Not an ES-14356 deliverable per se — lives in `log4j2.serverless.properties` (Audit TDD Req 2). PoC does not block this; field stripping happens at the source by config. |
| R7 | Stdout fallback on exhausted retries | Not implemented. Today the `BatchLogRecordProcessor` drops on queue overflow with no fallback. Production needs: catch exporter failure after retries, write the record to stdout in a recognizable format, ensure internal observability pipeline collects it. Coupled to a "replay" service on the platform side (gateway TDD §"Unavailability mitigation" point 3) which is outside ES. |
| R8 | Per-project ID filter | Not implemented. The PoC has no per-project gating; the only per-project field on the record today is `project.id` itself (R3). Production needs: (a) an appender-path filter that drops events whose `project.id` is not in the enabled set; (b) a config source for the enabled set, likely from Control-Plane via the existing project-state propagation mechanism. |
| R12 | Configuration without cluster restart | The PoC reads `telemetry.otel.logs.enabled` once in `OtelSdkExportLogsSupplier.install()` (line 69) at startup; toggling it requires a restart. Per-project state changes (R8) would have the same problem. Production needs a settings/state listener that re-installs or reconfigures the appender path on change. Cluster-level dynamic on/off for `xpack.security.audit.enabled` is being addressed separately by Audit TDD Req 1 / [PR 147333](https://github.com/elastic/elasticsearch/pull/147333), but that's a different setting and doesn't cover the OTel-logs path. |
| NF1 | Tuned retry policy + buffer size | Default `BatchLogRecordProcessor` settings in place. Explicit "retry up to 2 minutes / buffer 30–50 MB" not configured. Cheap once the right knobs are picked. |
| — | `request.body` PII story | Default-off / redact / sample. Needs security review before this field can leave the cluster. |
| — | Production-side IT | PoC uses an in-process recording server. Real `otel-delivery-gateway` (or sufficiently faithful mock) test would catch wire-level surprises. |

### 4.1 Decision points

These are tradeoffs the team should weigh before the implementation work in §4 starts.

#### 4.1.1 Attribute-key shape

The upstream `OpenTelemetryAppender` library hardcodes a `log4j.map_message.` prefix on every attribute that comes from a `StringMapMessage`. Today's PoC records carry `log4j.map_message.event.action` etc. instead of bare `event.action`. The integration test asserts on the prefixed names with a comment noting it's PoC-only; production needs bare keys.

| Option | Pros | Cons |
|---|---|---|
| **Custom log4j appender (~50 LOC)** | Narrow scope. No incubator/extension deps. Aligned with where v3 of the upstream library is going (prefix off by default). Easy to extend with semconv translation later. | We own a small log4j component; reviewers may ask "why not use upstream?" |
| **`v3_preview` wrapper (~15 LOC)** | Uses upstream library as designed. Smallest delta. | Pulls in `opentelemetry-api-incubator` (marked unstable) and `opentelemetry-sdk-extension-declarative-config`. Enables *all* v3-preview-gated behavior in the OTel logs path, not just the prefix. |
| **Switch the apm module to `AutoConfiguredOpenTelemetrySdk`** | Aligns ES with OTel's standard config story; system properties / YAML drive SDK behavior. Worth doing for reasons beyond this feature. | Substantial refactor; affects metrics and traces too. Bigger conversation than this epic. |
| **`LogRecordProcessor` post-hoc rename (~30 LOC)** | Keeps upstream appender. Localized change. | Smell: prefix is added by one stage and stripped by the next. Doesn't solve semconv translation, only the prefix. |

#### 4.1.2 Field mapping shape: semconv vs ECS vs custom

The gateway TDD asks emitting services to use OTel semconv where it exists, ECS where it doesn't, custom names as last resort. Most ES audit fields have a clean answer; some are genuinely ambiguous. The decision affects how downstream consumers query and join audit data.

| Field group | Status |
|---|---|
| Direct semconv: `event.action`, `user.name`, `http.request.method`, timestamp | Clean. Mechanical translation. |
| `origin.address` | Split into `client.address` (REST) vs `server.address` (transport). Easy once the convention is agreed. |
| `authentication.type`, `user.roles` | ECS-only. Custom attributes. |
| `apikey.id`, `apikey.name` | No `apikey.*` namespace in semconv. Choose ECS-style or invent OTel-flavored nesting. |
| `indices` (array) | OTel has no multi-database convention. Custom array attribute. |
| `trace.id` | Native: should go in `LogRecord.traceId` for span correlation, with parsing/validation. |
| `request.body` | Hard. Potentially large + PII. Default-off today; needs a redaction or sampling story before it can leave the cluster. |
| `put` / `delete` / `change` / `create` / `invalidate` blobs in `security_config_change` | Hard. Nested arbitrary objects. Choose between flatten-to-attributes vs opaque-JSON-string; each has querying implications. |

#### 4.1.3 Where the SDK setup lives

`OtelSdkExportLogsSupplier` lives in `modules/apm/`, alongside the meter/tracer suppliers. The apm module's stated purpose is "ES's own observability" (metrics + traces back to Elastic). Customer-facing audit-log delivery is conceptually different — it ships data the customer cares about, not telemetry about ES itself.

Options:
- Keep it in `modules/apm/`. Cheap, consistent with the metrics/tracer pattern, tolerable in the short term.
- Carve out a new sibling module (e.g. `modules/customer-telemetry/`). Cleaner conceptual boundary; adds a module.

Worth deciding before the audit-log path grows further.

#### 4.1.4 `LoggingAuditTrail` constructor signature for `project.id`

The PoC reads `X-Elastic-Project-Id` from `ThreadContext` directly inside `withThreadContext()`, which works because the header is always populated by the time audit events fire. A more orthodox approach injects `ProjectResolver` into the `LoggingAuditTrail` constructor — `ProjectResolver` is the canonical accessor and handles edge cases (cross-cluster, internal actions with no project) more gracefully.

Not urgent — the header path is correct for the common case — but it's the kind of thing a code reviewer will ask about.

## Appendix A — Options considered and rejected

Paths the PoC explored where evidence pushed us elsewhere. Captured here so we don't relitigate.

### A.1 Architecture B: write to file, sidecar tails it

Keep the existing rolling-file audit appender, deploy a sidecar OTel collector with a `filelog` receiver to tail the file and forward via OTLP. Operationally appealing — survives ES-process death since the launcher can drain remaining file content like a heap dump. Discussed with Ryan Ernst, who agreed this is the safer strategy in principle.

Rejected because: the team that owns the gateway has scoped their pipeline assuming OTLP ingress from the application (see Gateway TDD §"OTel log delivery pipeline on ECP"). Architecture A is what the Jira description and the gateway TDD assume. If the gateway team changes their mind, B becomes attractive again, but that's a cross-team conversation.

### A.2 Declaring `OpenTelemetryAppender` in `log4j2.properties`

Initial implementation declared the OTel appender in `x-pack/plugin/core/src/main/config/log4j2.properties` like any other appender. ES boot failed:

```
main ERROR Unable to locate plugin type for OpenTelemetry
main ERROR Unable to locate plugin for OpenTelemetry
main ERROR Unable to invoke factory method ... NullPointerException
main ERROR Unable to locate appender "audit_rolling" for logger config "..."  ← cascade
main ERROR Unable to locate appender "console" for logger config "root"        ← cascade
```

The properties file is parsed at JVM startup, before plugin/module classloaders are set up; the appender plugin class is in the apm module's classloader and isn't reachable yet. The NPE that follows cascades and breaks **all** appender registration, not just the OpenTelemetry one — meaning audit logging would be silently broken.

Working solution (implemented): attach the appender programmatically in `OtelSdkExportLogsSupplier.install(...)`, after the apm module is loaded. Build the appender via `OpenTelemetryAppender.builder().setOpenTelemetry(sdk).build()` (passing the SDK directly on the builder rather than via the static `install(...)` method, which has its own ordering subtleties), attach to the audit logger's `LoggerConfig`.

### A.3 Putting the OTel deps in `:server`

Tried as a fix for A.2 — if the appender JAR is on `:server`'s classpath, log4j can resolve it at boot. ES then failed with **jar hell**: `opentelemetry-context` is also pulled transitively by `:x-pack:plugin:esql-datasource-gcs` at a different version, and ES rejects cross-classpath duplicate classes at plugin-load time. Unwound the `:server` change and kept everything in `modules/apm/`, going with the programmatic-attach path instead.

### A.4 Direct OTel SDK call from `LoggingAuditTrail`

Rather than going through log4j at all, have `LoggingAuditTrail.LogEntryBuilder` emit OTLP records directly via `OpenTelemetry.getLogsBridge()`. Considered — it's the most controlled shape and bypasses the whole appender library. Not pursued because the goal is for an appender (the natural log4j extension point) to handle the conversion. The custom-appender option in §4.1.1 captures the right shape without modifying `LoggingAuditTrail`.

## Appendix B — Findings worth knowing

Things that surprised us; worth knowing for the implementation.

- **`manage_threads` entitlement** is required for `io.opentelemetry.sdk.logs` because `BatchLogRecordProcessor` spawns a worker thread. First test run failed with `NotEntitledException` until added to `entitlement-policy.yaml`. The kind of finding that's a 30-minute mystery in a real PR cycle if you don't expect it.
- **`gradle/verification-metadata.xml` regen** is required for any new third-party artifact. `./gradlew --write-verification-metadata sha256 ...` does it; takes ~2 minutes and produces a sizeable diff.
- **Hand-wired OTel SDK and declarative config don't compose easily.** ES uses `OpenTelemetrySdk.builder()` (the bare SDK builder), which has no public way to set a `ConfigProvider`. As a result, instrumentation-config flags like `otel.instrumentation.common.v3-preview` — the upstream library's preview knob for dropping the `log4j.map_message.` prefix — can't be set through the public SDK API. They're settable via `AutoConfiguredOpenTelemetrySdk.builder().addPropertiesSupplier(...)` (auto-configure module) or by implementing `ExtendedOpenTelemetry` ourselves with a `YamlDeclarativeConfigProperties.create(Map, ComponentLoader)` (incubator + declarative-config extension modules). This is a real OTel-Java friction point that affects more than just this feature. The OTel configuration spec itself requires SDKs to expose programmatic config; the Java SDK does, but only outside the bare builder.
- **The `OpenTelemetryAppender.install(sdk)` static method is order-dependent.** It pushes the SDK to *already-registered* `OpenTelemetryAppender` instances. Calling it before the appender is in the log4j config is a silent no-op for that appender. Using `Builder.setOpenTelemetry(sdk)` directly is more robust.
- **Audit events use `StringMapMessage`, which is structured but bodyless.** The OTel appender library treats this as "no `LogRecord.body`, capture entries as attributes." With `setCaptureMapMessageAttributes(true)` this works but adds the prefix discussed in §4.1.1.
- **Non-blocking is inherited, not engineered.** The OTel SDK's `BatchLogRecordProcessor` is non-blocking by spec (§3.2). We didn't need to add ES-side code to satisfy R9 — using the SDK *as the SDK is meant to be used* gives us the property for free. Worth knowing because reviewers will reasonably ask "where's the code that makes this non-blocking?" — there isn't any; it's a property of the dependency.
