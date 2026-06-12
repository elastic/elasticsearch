# OTel audit log delivery: POC writeup for ES-14356

This PoC validates the ES side of the OTel audit-log pipeline: an audit event emitted by `LoggingAuditTrail` reaches an in-process recording OTLP server over OTLP/gRPC (okhttp-based sender) through log4j and the OTel SDK, in a security-enabled multi-node test, with `project.id` attached and the SDK's `BatchLogRecordProcessor` keeping I/O off the calling thread. The real `otel-delivery-gateway`, MOTel, and destination project aren't exercised here; closing that gap is [§4.12](#sec-4-12). Around half of the cross-team requirements are satisfied or close to it; the meaningful gaps are mTLS to the gateway, a stdout fallback for exhausted retries, a cluster-level audit-enabled toggle without restart, dynamic configuration, and a few field-shape problems caused by the upstream `OpenTelemetryAppender`. R3 specifically has four open gaps that are larger than the PoC suggests: `security_config_change` events bypass `withThreadContext` in the existing audit-emit code ([§4.3](#sec-4-3)); cross-cluster CPS strips the project header at the boundary, so linked-side events have no `project.id` ([§4.4](#sec-4-4)); Cloud API key audit (the dominant serverless path) is structurally incomplete and gets `project.id` only because the platform ingress sets it ([§4.13](#sec-4-13)); and audit reads `project.id` via a different mechanism than authorization, so the two can disagree on any deployment ([§4.14](#sec-4-14)).

I'm not expecting anyone to read this doc end-to-end;
I've arranged it so you can skip to the parts you care about.
[§2](#sec-2) lists each requirement with a one-line status pointing into [§3](#sec-3) (validated behavior) or [§4](#sec-4) (gaps). [§5](#sec-5) collects the open design tradeoffs that need a decision before further work. [§6](#sec-6) distills [§4](#sec-4) and [§5](#sec-5) into a punch list: decisions needed ([§6.1](#sec-6-1)), implementation work ([§6.2](#sec-6-2)), and questions by audience ([§6.3](#sec-6-3)). [Appendix A](#sec-appendix-a) records the alternatives the PoC considered and rejected; [Appendix B](#sec-appendix-b) captures incidental findings worth knowing for follow-up work.

## <a id="sec-1"></a>1. Goal

Make Elasticsearch audit logs deliverable to serverless customers via OTLP. Today, audit logs are written to a per-cluster JSON file by `LoggingAuditTrail`; in self-hosted deployments, customers ship that file with Filebeat. Serverless has no Filebeat sidecar and no shared disk, so the file path doesn't apply.

The cross-team plan: emitting services (ES, Kibana, Control-Plane services) send OTLP log records over the network to an `otel-delivery-gateway` running in the same ECP K8s cluster. That gateway routes records to per-project MOTel endpoints, which write to the customer's destination Elasticsearch project. Architecture is documented in the gateway TDD (see [§2](#sec-2) references).

This PoC validates that ES can emit OTLP audit logs end-to-end and surfaces the work that remains.

## <a id="sec-2"></a>2. Requirements

References for the requirements below:

- **Audit TDD**: *TDD - Audit Logging in Elasticsearch* (Ankit Sethi). [Google Doc](https://docs.google.com/document/d/1ox4Hc-Ga_HMQtpFNzvBivHbn0khBOzzL4hN6WsMH8ko/edit). Frames the audit-logic-side concerns (filtering, redaction, namespacing) as the broader audit-in-serverless effort. ES-14356 is the *delivery* slice of this; most of the audit TDD's eight requirements are owned outside this PoC (see [§2.14](#sec-2-14) below).
- **Gateway TDD**: *Shipping Logs to Elastic Cloud customers (TDD)*. [Google Doc](https://docs.google.com/document/d/12IZxcX5uoFWvIhfff1I3DwRVMt8ab6tNkaIKs-Fx8Y8/edit). Frames the cross-team architecture from emitting service to MOTel.
- **ES-14356**: parent epic for the ES-side delivery work. [Jira](https://elasticco.atlassian.net/browse/ES-14356).
- **ES-13255**: broader "ES logs over OTel". [Jira](https://elasticco.atlassian.net/browse/ES-13255). Tracks any non-audit ES log streams; out of scope here.
- **elasticsearch-team#2170**: tracking issue for audit-in-serverless work; the issue body itself is a one-line pointer to the `#log-delivery-project-team` Slack channel where the work is being discussed. [GitHub](https://github.com/elastic/elasticsearch-team/issues/2170).
- **Customer-facing audit log configuration TDD**: *TDD — Customer-facing audit log configuration in Serverless* (Julio Camarero). [Google Doc](https://docs.google.com/document/d/1M9Uzq6M8s3R6cfKMdJcoIkgswbADQbbANnS40anbb3E/edit). Covers how customers configure audit logging (and other log streams) in serverless: Project API shape, cross-app consistency between ES and Kibana, and the structural relationship between per-setting plumbing and a framework-level solution. See also [§4.15](#sec-4-15) and [§5.5](#sec-5-5).
- **TRACING.md**: *Tracing in Elasticsearch* (inline ES doc). [GitHub](https://github.com/elastic/elasticsearch/blob/b037081bb93f8e9466438980c4855fd740a38a79/TRACING.md). Documents the existing OTel tracing infrastructure in ES — the `tracing` package abstraction, the APM agent, and telemetry configuration (including the optional auth credentials `telemetry.secret_token` / `telemetry.api_key`). Background context for the `modules/apm/` module this work extends.

Each requirement below has a status of *Satisfied*, *Partially satisfied*, or *Gap*. The status line points to [§3](#sec-3) evidence or [§4](#sec-4) gap analysis.

### <a id="sec-2-1"></a>2.1 R1: Emit audit events via OTLP to the OTel gateway

*Satisfied ([§3.1](#sec-3-1)).*

The headline behavior of ES-14356: each `LoggingAuditTrail` event must reach the `otel-delivery-gateway` (running in the same ECP K8s cluster as ES) over OTLP. The gateway then routes the record to a per-project MOTel endpoint, which writes to the customer's destination project.

Sources: Audit TDD §"Technical Dependencies": *"ES Core/Infra will take on adding support for a new Log4J appender that ships logs over the wire."* Gateway TDD §"How services emit (audit) log records": *"Must emit logs through the otlp protocol, on the network, towards the local otel-delivery-gateway service."*

### <a id="sec-2-2"></a>2.2 R2: Use OTel semconv field names and JSON structure

*Partially satisfied ([§3.4](#sec-3-4)); both the bare-key shape and per-field translation are gaps ([§4.1](#sec-4-1), [§4.2](#sec-4-2)).*

Records must use the OTel JSON top-level structure (`@timestamp`, `attributes`, `body`, `resource.attributes`) and OTel semconv field names where they exist, falling back to ECS, then to custom names only when neither has one.

Two pieces are open:
1. the upstream `OpenTelemetryAppender` library prepends `log4j.map_message.` to every attribute that comes from a `StringMapMessage`, so today's records carry `log4j.map_message.event.action` instead of `event.action`. [§5.1](#sec-5-1) lists options for dropping the prefix.
2. the per-field translation from current ES audit names to semconv/ECS/custom hasn't been done. [§5.2](#sec-5-2) has the per-field table.

Sources: Gateway TDD §"Standardized Audit Log schema" + §"How services emit"; Audit TDD §"Requirement 8".

### <a id="sec-2-3"></a>2.3 R3: Enrich audit events with `project.id`

*Partially satisfied ([§3.3](#sec-3-3)). Four open gaps: `security_config_change` events bypass the chokepoint ([§4.3](#sec-4-3)); cross-cluster CPS leaves linked-side events without `project.id` ([§4.4](#sec-4-4)); Cloud API key audit shape is incomplete and the chokepoint is fragile under UIAM ([§4.13](#sec-4-13)); and audit reads `project.id` via a different mechanism than authorization, so the two can disagree on any deployment ([§4.14](#sec-4-14)).*

Audit events must carry `project.id` so the gateway can route per-project. The PoC plumbs the field through `LoggingAuditTrail.LogEntryBuilder.withThreadContext()`. The 12 public audit-emit methods all reach this chokepoint; however, `accessGranted` also emits a parallel `security_config_change` log entry through a private builder factory that bypasses `withThreadContext()`, so events of `event.type=security_config_change` (put_user, put_role, change_password, create_api_key, etc.) do not carry `project.id`. End-to-end OTLP delivery has been verified for one event type only.

Cross-Project Search audits today's `withThreadContext`-based plumbing in two directions: the cross-cluster boundary strips `X-Elastic-Project-Id`, leaving linked-side events without a `project.id` at all (we initially believed the linked side carried project B's ID; a local IT showed otherwise — see [§4.4](#sec-4-4)); and UIAM-authenticated requests don't populate the header themselves, so the chokepoint depends on the platform ingress doing it (see [§4.13](#sec-4-13)). (Cluster-internal traffic, by contrast, is explicitly out of scope per [§2.14](#sec-2-14).)

Sources: Audit TDD §"Requirement 7"; Gateway TDD §"Standardized Audit Log schema" / Appendix 5.

### <a id="sec-2-4"></a>2.4 R4: Authenticate to the gateway via mTLS

*Gap ([§4](#sec-4)).*

The application identifies itself to the `otel-delivery-gateway` with a client certificate; the gateway uses the cert's identity to validate that a record's claimed source matches its TLS origin. The PoC uses plain HTTP without TLS.

Source: Gateway TDD §"OTel log delivery pipeline on ECP", subsection "Authentication & authorization".

### <a id="sec-2-5"></a>2.5 R5: Transport over OTLP/gRPC

*Satisfied ([§3.5](#sec-3-5)).*

Wire transport is OTLP/gRPC. The PoC uses `OtlpGrpcLogRecordExporter` backed by the okhttp-based gRPC sender. Production requires **gRPC**, per Julio Camarero ([#log-delivery-project-team, 2026-05-13](https://elastic.slack.com/archives/C09PANY7FFS/p1778657565867949)):

> Hey Patrick, it was HTTP initially, but we had to change it to gRPC. When testing autoscaling of the gateway, we realized that HTTP clients often reused long-lived connections, which lead to uneven distribution of the load behind kubernetes services, and even after adding more replicas to the gateway, all the HTTP Clients kept making requests to the first one. (This is a [known issue](https://github.com/open-telemetry/opentelemetry-collector/issues/9211) of the opentelemetry-collector). There are more details [here](https://github.com/elastic/otel-delivery-gateway/pull/133).

So gRPC is a load-balancing-correctness requirement for the gateway behind k8s services, not a protocol-feature preference. The switch is implemented; see [§3.5](#sec-3-5).

Sources: Gateway TDD §"How services emit (audit) log records": *"Must emit logs through the otlp protocol, on the network, towards the local otel-delivery-gateway service"*; §"OTel log delivery pipeline on ECP": *"via the otlp protocol (grpc)"*; Julio Camarero's 2026-05-13 message above.

### <a id="sec-2-6"></a>2.6 R6: Strip cluster identity fields on serverless

*Gap ([§4.6](#sec-4-6)); implementation is a serverless-config change with no ES code change.*

On serverless, audit records must not carry `cluster.name`, `cluster.uuid`, `node.name`, or `node.id`. These expose platform internals customers shouldn't see. See [§4.6](#sec-4-6).

Source: Audit TDD §"Requirement 2".

### <a id="sec-2-7"></a>2.7 R7: Stdout fallback when retries are exhausted

*Gap ([§4](#sec-4)).*

If the application can't reach the gateway after its configured retries, it falls back to writing the audit record to stdout in a recognizable form. The internal observability pipeline picks that up, and a separate "otel-delivery-replay" service queries it back into the audit pipeline. Not implemented.

Source: Gateway TDD §"Unavailability issues mitigation" point 1: *"In case of continuous unavailability exhausting all retries, the service should fallback to logging the audit record on stdout, to be collected through the internal observability pipeline as regular application logs."*

### <a id="sec-2-8"></a>2.8 R8: Filter audit events per project ID

*Gap ([§4](#sec-4)).*

ES must not emit OTel audit records when audit logging is disabled for the cluster. Filtering must happen in ES before the OTel emit, not downstream in the gateway or MOTel. For project-per-cluster, this reduces to a cluster-level `telemetry.otel.logs.enabled` toggle; see [§4.8](#sec-4-8). Multi-project per-project filtering rationale and design are in [Appendix C](#sec-appendix-c).

**Customer toggle**: `_cluster/settings` is permanently off the table in serverless (Ryan Ernst, Mark Vieira; see [§4.15](#sec-4-15)). Customers turn audit on/off per project via the Control Plane's Project API (`PATCH /api/v1/serverless/projects/<type>/{id}`), which is already wired up for audit-logging enablement today. The signal reaches ES via file-based settings. In a project-per-cluster deployment, R8 reduces to a cluster-level dynamic delivery toggle; see [§4.9](#sec-4-9).

### <a id="sec-2-9"></a>2.9 R9: Non-blocking emit path

*Satisfied ([§3.2](#sec-3-2)).*

The audit-emit path must not block transport or network threads. The OTel SDK's `BatchLogRecordProcessor` queues records and ships them on its own worker thread, so the property is inherited from the SDK rather than engineered into ES ([§3.2](#sec-3-2) walks the call chain).

Henning Andersen raised the requirement in a comment on the Audit TDD: *"I remember you had thoughts on how shipping audit logs could work in a way that did not require a new shipper, still allowed spill to disk and would not block transport threads. This seems to be a significant part of the work unless this is considered separately."* Tim Vernum followed up: *"Will the Core/Infra work guarantee that no blocking work is done on transport threads? I can't see where it's explicitly mentioned in this doc or the ticket referenced above."* The underlying rationale appears in the Audit TDD §"Why is audit logging not already enabled?" point 2: *"Writing to disk occurs on transport threads, adding a source of instability."*

### <a id="sec-2-10"></a>2.10 R10: Best-effort emit

*Partially satisfied ([§3.2](#sec-3-2)); becomes fully satisfied once R7 lands.*

A failed log send must not fail the originating operation. The OTel SDK's bounded queue drops on full rather than blocking, so a slow gateway doesn't fail the originating operation. Partial because the PoC doesn't yet implement R7: dropped records aren't preserved anywhere, so customers may lose audit records during gateway outages.

Sources: Gateway TDD §"Reliability & throughput": *"From the application/service perspective, we still move forward with executing an operation if writing the audit log fails. We favor availability of the service over consistency of the audit logs"*; §"How services emit": *"Should consider audit log delivery as 'best effort'..."*

### <a id="sec-2-11"></a>2.11 R11: End-to-end at-least-once delivery

*Partially satisfied; full at-least-once requires R7.*

Records are not silently dropped during normal operation; occasional duplicates are acceptable. The PoC inherits the SDK's retry-and-buffer behavior, which gives at-least-once *while the gateway is reachable*. Persistent gateway unavailability still causes drops on queue overflow. Closing that gap is R7 (stdout fallback); once dropped records are written to stdout and replayed by the platform-side replay service, true at-least-once is reached.

Source: Gateway TDD §"Reliability & throughput" + §"Unavailability issues mitigation".

### <a id="sec-2-12"></a>2.12 R12: Configuration changes without cluster restart

*Gap ([§4](#sec-4)).*

Toggling audit logging must take effect without a restart. The PoC reads `telemetry.otel.logs.enabled` once in `OtelSdkExportLogsSupplier.install()` at startup, so toggling it requires a restart today.

Audit TDD §"Requirement 1" calls for *"... enable or disable audit logging with a simple setting on the Cloud console without any downtime (cluster restart)"*; this is in flight under [PR 147333](https://github.com/elastic/elasticsearch/pull/147333) for `xpack.security.audit.enabled`. That setting governs the audit logger, not the OTel-logs path; we still need our own dynamic listener for `telemetry.otel.logs.enabled`. (The R6 `emit_*` settings are already `Property.Dynamic`; toggling them on serverless requires no extra work.)

### <a id="sec-2-13"></a>2.13 R13: Retry-policy and buffer-size targets

*Gap ([§4](#sec-4)).*

The OTel SDK should be configured to retry up to ~2 minutes (e.g. ~3 retries) and to bound the in-memory buffer at ~30–50 MB. The 4 MB/s per-container source-rate forecast in the Gateway TDD's Throughput section is an input for sizing this buffer (~10 s of headroom at the spec'd rate); it's not an ES-side requirement on its own, but it informs the buffer choice.

The PoC uses the SDK's defaults; specific tuning is straightforward once the values are decided.

Sources: Gateway TDD §"How services emit"; §"OTel log delivery pipeline on ECP"; §"Unavailability issues mitigation".

### <a id="sec-2-14"></a>2.14 Non-requirements (explicitly out of scope for ES-14356)

These concerns belong to other tracks.

- **The existing `audit_rolling` file appender is unchanged.** Customers consuming `<cluster>_audit.json` with Filebeat keep doing so. Making `audit_rolling` non-blocking is also out of scope.
- **Non-audit ES log streams** (server, deprecation, slowlog, ESQL). Tracked under ES-13255.
- **Cluster-level dynamic enable/disable of `xpack.security.audit.enabled`** (Audit TDD Req 1; in flight via [PR 147333](https://github.com/elastic/elasticsearch/pull/147333)). The corresponding work for `telemetry.otel.logs.enabled` is ours; see [§4.9](#sec-4-9). Multi-project per-project granularity is deferred; see [Appendix C](#sec-appendix-c).
- **Origin-type / realm filtering of internal traffic.** Audit TDD Reqs 3, 4. Owned by Ankit Sethi; tracked under elasticsearch-team#2170.
- **Operator-privilege placeholder for Elastic-employee access.** Audit TDD Req 5.
- **UIAM / CPS audit instrumentation.** Audit TDD Req 6.
- **Field namespacing currently done by Filebeat** (e.g. `user.realm` → `elasticsearch.audit.user.realm`). Audit TDD Req 8; lives in `log4j2.serverless.properties`, not here.
- **Internal infosec cluster delivery.** Gateway TDD §"Out of scope".
- **Production integration test against a real `otel-delivery-gateway`.** PoC's IT uses an in-process recording server.
- **The 99.9% delivery SLO** is a *system-level* property the ES side cannot uphold on its own; it depends on gateway, MOTel, and destination cluster availability. ES contributes to it via R7 (stdout fallback), R9 (non-blocking emit), R10 (best-effort), R11 (at-least-once), and R13 (retry/buffer sizing); it is not a standalone ES requirement.

## <a id="sec-3"></a>3. What's implemented

The headline result: an audit event emitted by `LoggingAuditTrail` reaches a recording OTLP server end-to-end, in a security-enabled multi-node test, with `project.id` attached and the OTel logs SDK on the wire path.

### <a id="sec-3-1"></a>3.1 End-to-end OTLP delivery: *satisfies R1*

The test is `OtelAuditLogsIT.testAuditEventArrivesAsOtlpLogRecord`.
The test boots a security-enabled `ElasticsearchCluster` with `xpack.security.audit.enabled=true`, `telemetry.otel.logs.enabled=true`, and the gateway endpoint pointed at an in-process `RecordingApmServer`. It hits `/_security/_authenticate` (which produces an `authentication_success` audit event), forces a flush via `/_flush_telemetry`, and asserts a `ReceivedTelemetry.ReceivedLog` arrives within `TELEMETRY_TIMEOUT`.

**Pipeline construction:**

- `OtelSdkExportLogsSupplier.install()` builds the OTel SDK (`SdkLoggerProvider` + `OtlpGrpcLogRecordExporter` + `BatchLogRecordProcessor`), constructs an `OpenTelemetryAppender` programmatically, and attaches it to the audit logger's `LoggerConfig`. This must happen programmatically; see [Appendix A.2](#sec-a-2) for why declaring the appender in `log4j2.properties` doesn't work.
- `APM.createComponents()` wires the supplier into the plugin lifecycle and exposes it via `APMTelemetryProvider`.
- Settings `telemetry.otel.logs.enabled` and `telemetry.otel.logs.endpoint` registered in `OtelSdkSettings` and added to the plugin's setting list.
- `attemptFlushLogs()` plumbed through `TelemetryProvider` and `APMTelemetryProvider` so tests and graceful shutdown can force-flush the `BatchLogRecordProcessor`.

### <a id="sec-3-2"></a>3.2 Non-blocking on the calling thread: *satisfies R9; partially satisfies R10, R11*

The OTel SDK is used unmodified, and its publication path provably does not block. Trace:

1. `LoggingAuditTrail` calls `logger.info(AUDIT_MARKER, logEntry)` on a transport/REST thread. log4j2 dispatches via `LoggerConfig.log(...)` → `callAppenders(...)` → `OpenTelemetryAppender.append(LogEvent)` on the *same calling thread*.
2. `OpenTelemetryAppender.append` does only the log4j → OTel record translation (CPU only) and calls `LogRecordBuilder.emit()`.
3. `SdkLogRecordBuilder.emit()` invokes `getLogRecordProcessor().onEmit(...)`, which resolves to `BatchLogRecordProcessor.onEmit`.
4. `BatchLogRecordProcessor.onEmit` calls `worker.addLogRecord(...)`, which `offer`s the record onto a bounded `ArrayBlockingQueue` (default capacity 2048). On full, `offer` returns false and the record is **dropped**, not held.
5. The actual OTLP gRPC call runs on a worker thread the `BatchLogRecordProcessor` owns, never on the caller.

Consequences:

- There is no back-pressure to the caller: a slow or unreachable gateway produces drops, not blocked transport threads.
- There is no I/O on the caller; network and serialization happen on the worker thread.

Per the [OTel logs SDK spec](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/logs/sdk.md#onemit), `LogRecordProcessor.OnEmit` *SHOULD NOT block* and overflow drops rather than back-pressures. We are in spec.

This is a property we *inherit* from the OTel SDK, not one we engineered into ES.

### <a id="sec-3-3"></a>3.3 `project.id` enrichment: *partially satisfies R3*

**Mechanism:**

- `LoggingAuditTrail.PROJECT_ID_FIELD_NAME` introduced as the audit field name.
- `LogEntryBuilder.withThreadContext(...)` reads `Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER` from `ThreadContext` and writes `PROJECT_ID_FIELD_NAME` on the audit `StringMapMessage` if non-null.
- Audit JSON pattern in `x-pack/plugin/core/src/main/config/log4j2.properties` updated so the field appears in the file appender too.
- Eleven of the twelve public audit-emit methods on `LoggingAuditTrail` call `.withThreadContext(...)` at build time: `authenticationSuccess`, `authenticationFailed`, `accessGranted`, `accessDenied`, `anonymousAccessDenied`, `tamperedRequest`, `runAsGranted`, `runAsDenied`, `connectionGranted`, `connectionDenied`, `explicitIndexAccessEvent`. (`coordinatingActionResponse` is currently a no-op.)
- **Gap:** `accessGranted`, when the action falls in `SECURITY_CHANGE_ACTIONS`, also emits a parallel `security_config_change` log entry through `securityChangeLogEntryBuilder(...)`, a private builder factory that never calls `withThreadContext`. So `event.type=security_config_change` events do not carry `project.id`. See [§4.3](#sec-4-3).

**Where the header gets populated:**

- Customer REST traffic on serverless: the ingress layer sets `X-Elastic-Project-Id` on the incoming HTTP request; ES copies it into `ThreadContext` before auth runs.
- Within-cluster transport fan-out: `ThreadContext` propagates the header via `HEADERS_TO_COPY` (handled in `ThreadContext.getRequestHeadersToCopy(...)`).
- Persistent tasks: `PersistentTasksNodeService.startTask(...)` puts the header for project-scoped tasks.
- Cluster-internal traffic (system users, internal cluster operations) is out of scope per [§2.14](#sec-2-14) (Audit TDD Reqs 3, 4; elasticsearch-team#2170).

**Tests:**

- `LoggingAuditTrailTests` covers the mechanism: a setUp helper randomly sets the project header on `ThreadContext`, and a `projectId(...)` assertion helper checks the field on each audit event. The non-security-change tests invoke that helper; the `testSecurityConfigChangeEventFormatting*` tests do *not*, consistent with the gap above. The full class (35 tests) passed against this change at last run.
- `OtelAuditLogsIT.testAuditEventArrivesAsOtlpLogRecord` exercises end-to-end OTLP delivery for one event type: `authentication_success` from `/_security/_authenticate`. Other event types and transport-fan-out cases are not exercised by tests.

**Open question on CPS routing direction**: see [§4.4](#sec-4-4).

### <a id="sec-3-4"></a>3.4 Semconv field names arrive on the wire: *partially satisfies R2*

`OtelAuditLogsIT` asserts that `event.action` and `event.type` are present on the received `LogRecord` attributes. So the *fields* survive the log4j → OTel hop end-to-end.

**Caveat:** they arrive prefixed as `log4j.map_message.event.action`, because the upstream `OpenTelemetryAppender` library hardcodes that prefix when capturing `StringMapMessage` entries with `setCaptureMapMessageAttributes(true)`. Bare-key shape is not yet satisfied; see [§5.1](#sec-5-1) for options.

### <a id="sec-3-5"></a>3.5 gRPC transport: *satisfies R5*

`OtelSdkExportLogsSupplier` uses `OtlpGrpcLogRecordExporter` backed by the okhttp-based gRPC sender — OTLP/gRPC framing over HTTP/2, no grpc-java dependency. `RecordingApmServer` in the integration test is dual-protocol: an HTTP server for metrics/traces/intake, plus a gRPC server on a separate ephemeral port for log records. Test passes end-to-end.

### <a id="sec-3-6"></a>3.6 Other tests (no regressions)

- `:modules:apm:thirdPartyAudit` runs clean: no jar-hell or banned-API regression from the new dependencies.
- `OtelSdkExportLogsSupplierTests` passing; covers install/uninstall lifecycle of the supplier (idempotent install, detach on close, behavior when audit logger config is absent). The `PR 1` version moves endpoint-validation tests to use `ClusterSettings.validate()` (where the `Setting.Validator` fires) rather than asserting on `install()` throwing.
- ES boots cleanly under `./gradlew run` with audit and OTel logs enabled. The supplier emits `OTel SDK logs export installed; endpoint=...`, and the node reaches `started`.
- `manage_threads` entitlement registered in `modules/apm/src/main/plugin-metadata/entitlement-policy.yaml` for `io.opentelemetry.sdk.logs`. Without this, `BatchLogRecordProcessor`'s worker thread fails to start with `NotEntitledException`. (See [Appendix B](#sec-appendix-b).)

## <a id="sec-4"></a>4. What's not implemented (gap analysis)

For each requirement that the PoC doesn't yet satisfy, what's needed to make it real.

### <a id="sec-4-1"></a>4.1 R2 (full): Bare semconv keys, no prefix

The upstream `OpenTelemetryAppender` library prepends `log4j.map_message.` to every attribute that comes from a `StringMapMessage`. Today's records carry `log4j.map_message.event.action` instead of bare `event.action`. The integration test asserts on the prefixed names with a comment noting it's PoC-only; production needs bare keys. [§5.1](#sec-5-1) lays out four options, ranging from a ~15 LOC v3-preview wrapper to a small custom appender to a larger SDK-config refactor. A decision is needed before further audit-event mapping work, since the option chosen also gates how [§5.2](#sec-5-2) (per-field translation) is implemented.

### <a id="sec-4-2"></a>4.2 R2 (mapping): Field-by-field semconv translation

The gateway TDD asks emitting services to use OTel semconv where it exists, ECS where it doesn't, custom names as last resort. About half the ES audit fields have clean semconv answers: `event.action`, `user.name`, `http.request.method`, timestamp. The rest are genuinely ambiguous: the `origin.address` REST/transport split, the `apikey.*` namespace, the `indices` array, `request.body`, and the nested `security_config_change` blobs. [§5.2](#sec-5-2) has the per-field table.

### <a id="sec-4-3"></a>4.3 R3 (chokepoint): `security_config_change` events bypass `withThreadContext`

`accessGranted` emits two log entries when its action falls in `SECURITY_CHANGE_ACTIONS` (put_user, put_role, change_password, create_api_key, delete_role, …): the regular `access_granted` event (which calls `.withThreadContext(threadContext)` at build time, so it carries `project.id`), and a separate `security_config_change` event built via the private `securityChangeLogEntryBuilder(...)` factory, which does *not* call `withThreadContext`. As a result, every `security_config_change` audit event leaves the JVM without a `project.id` attribute.

The test suite is consistent with this: `LoggingAuditTrailTests` invokes its `projectId(threadContext, checkedFields)` assertion helper from the non-security-change tests (`testAccessGranted`, `testAuthenticationFailed`, etc.) but not from any `testSecurityConfigChangeEventFormatting*` test.

We should do the following to make this robust:

1. **Open a bug ticket** for the missing `withThreadContext` call on the `security_config_change` path. Treating it as a bug (not a design choice) clarifies the intent: every audit event should carry the four thread-context-derived fields (`x_forwarded_for`, `opaque_id`, `trace.id`, `project.id`), and this path drops all four. The fix itself is small (add the call inside `securityChangeLogEntryBuilder(...)` or before each `.build()` on that chain), and `LoggingAuditTrailTests` should grow assertions on the security-change paths; and
2. **Make `withThreadContext` implicit in `build()`.** Once proposal 1 lands, every emit path is calling `withThreadContext` anyway. Folding it into the builder turns a convention into an invariant: future contributors who add new emit paths get the four fields automatically, and the doc's chokepoint claim becomes structural rather than empirical.

(Most of the work here is alignment, not code: agreeing that the missing call on the `security_config_change` path is a bug rather than expected behavior, and that the structural change is the right way to prevent the gap from recurring.)

Cross-Project Search has a separate R3 question ([§4.4](#sec-4-4)).

### <a id="sec-4-4"></a>4.4 R3 (CPS): `project.id` is absent on linked-cluster audit events

The original framing of this section asked "should an audit event for a CPS query in project A reaching into project B be routed to A (originator) or B (data-owner)?" That question is downstream of a more fundamental gap: under cross-cluster CPS today, **linked-side audit events carry no `project.id` at all**. The PoC's R3 mechanism (`LoggingAuditTrail.LogEntryBuilder.withThreadContext(...)` reads `X_ELASTIC_PROJECT_ID_HTTP_HEADER` from `ThreadContext`) depends on something upstream populating that header. On the linked side of a cross-cluster fan-out, nothing does:

- The origin cluster strips `X_ELASTIC_PROJECT_ID_HTTP_HEADER` at the cross-cluster boundary (`ThreadContext.getRequestHeadersToCopy(...)` for `REMOTE_CLUSTER`), so the header doesn't ride the transport request to the linked cluster.
- The linked cluster receives the request via transport, not HTTP, so the platform ingress that would normally set `X-Elastic-Project-Id` on inbound HTTP requests is never in the call path.
- There is no equivalent mechanism today that resets the header from the linked cluster's own configuration on inbound cross-cluster traffic.

Empirically: a local `internalClusterTest` we ran (subclassing `AbstractServerlessMultiProjectIntegTestCase`, attaching a capturing log4j appender to `LoggingAuditTrail`, issuing `FROM my-index | LIMIT 10` from origin's UIAM-authenticated client against an index that lives only on the linked side) produced 13 `access_granted` events — 3 from `my_origin_cluster`, 10 from `my_linked_cluster` — with `project.id` absent on every event. The fan-out is real; the field is not. The same-cluster (multi-tenant) CPS path via `AbstractProjectResolver.executeOnProject(...)` is a different code path and was not exercised here; whether it carries `project.id` correctly is a separate question.

Decisions and work needed:

1. **Decide how the linked cluster derives `project.id`.** Options: (a) cluster-config-derived (the linked cluster's `serverless.project_id` setting populates the audit field on inbound cross-cluster traffic, perhaps via the `CustomAuditLoggingMetadataProvider` extension proposed in [§5.4](#sec-5-4)); (b) propagate the originator's project ID explicitly via cross-cluster transport headers and capture it as `originating_project.id`; (c) both, depending on which routing intent the gateway team picks.
2. **Then** the original A-vs-B routing-direction call with the gateway team becomes meaningful. Until (1) is implemented, the linked side has no project ID to route on and the question is moot.

Caveat on the origin side: our local IT had no platform ingress, so origin events also lacked `project.id` — but this is a test-setup limitation rather than a production gap. In production, the ingress sets `X-Elastic-Project-Id` on inbound HTTP, so `withThreadContext` populates it correctly on the originating cluster. UIAM auth alone (no ingress) does *not* populate the header — see [§4.13](#sec-4-13).

### <a id="sec-4-5"></a>4.5 R4: mTLS to the gateway

The PoC uses plain gRPC without TLS. In production, the client certificate is distributed by Control-Plane (Vault → cert-manager → secret mount). We can reuse the existing pattern of `SSLService` + `PemKeyConfig` + `SSLConfigurationReloader` (used today by the monitoring HTTP exporter and Watcher's HTTP client). One open question is whether the okhttp-based gRPC sender exposes client TLS config at the builder level or whether we have to configure it at the `OkHttpClient` level.

### <a id="sec-4-6"></a>4.6 R6: Strip cluster/node fields on serverless

`LoggingAuditTrail.EntryCommonFields` populates `cluster.name`, `cluster.uuid`, `node.name`, and `node.id` into the audit `StringMapMessage` based on the corresponding `xpack.security.audit.logfile.emit_*` settings. With `setCaptureMapMessageAttributes(true)` on the `OpenTelemetryAppender`, whatever ends up in the `StringMapMessage` flows out via OTel, so setting these to `false` suppresses the fields from both the file appender and the OTel appender at the source.

The two settings that default to `true` (`emit_node_id`, `emit_cluster_uuid`) must be set to `false` in `serverless-default-settings.yml`. The settings are already `Property.Dynamic`, so no restart is needed. No ES code change required.

### <a id="sec-4-7"></a>4.7 R7: Stdout fallback on exhausted retries

Today the `BatchLogRecordProcessor` drops on queue overflow with no fallback. Production needs to catch exporter failure after retries, write the record to stdout in a recognizable format, and ensure the internal observability pipeline collects it. This is one half of the at-least-once story (R11); the other half is the platform-side "replay" service (Gateway TDD §"Unavailability issues mitigation" point 3), which is outside ES.

### <a id="sec-4-8"></a>4.8 R8: Per-project ID filter

Multi-project per-project filtering is explicitly out of scope. In a project-per-cluster deployment, R8 reduces to a cluster-level toggle: is OTel audit delivery currently active for this cluster? The toggle flows through the file-settings path (rendered by `elasticsearch-controller` from the Project API value) and is described in [§4.9](#sec-4-9).

### <a id="sec-4-9"></a>4.9 R12: Configuration without cluster restart

`telemetry.otel.logs.enabled` is a static (NodeScope) install-time gate and should remain so — it controls whether the OTel SDK is installed at all, not whether delivery is currently active. There is no dynamic on/off mechanism today.

The suggested approach: a second Dynamic NodeScope setting paired with a `LogRecordProcessor` that wraps `BatchLogRecordProcessor`. The processor gates delivery on a `BooleanSupplier` that a settings listener updates; records are dropped before they enter the queue when delivery is off. No SDK teardown, no SDK restart.

This processor sits at the seam between the `OpenTelemetryAppender` and the `BatchLogRecordProcessor`, making it naturally owned by the Security/audit layer rather than Core/Infra. The `BooleanSupplier` is also the right abstraction for the long-term multi-project case: today it is a volatile boolean; with ProjectScope it becomes a per-project setting accessor called on the original thread (which carries the right project context). The processor itself does not change. See [§6.3](#sec-6-3) (Ankit Sethi) for the coordination needed.

The audit filter settings (`xpack.security.audit.logfile.events.*` and `emit_*`) are already `NodeScope + Dynamic` with listeners registered in `LoggingAuditTrail`; no changes needed there.

Cluster-level dynamic on/off for `xpack.security.audit.enabled` is being addressed separately by Audit TDD Req 1 / [PR 147333](https://github.com/elastic/elasticsearch/pull/147333).

### <a id="sec-4-10"></a>4.10 R13: Tuned retry policy and buffer size

Default `BatchLogRecordProcessor` settings are in place. Explicit "retry up to 2 minutes / buffer 30–50 MB" targets aren't configured. Configuring them is cheap once the right knobs are picked.

### <a id="sec-4-11"></a>4.11 `request.body` PII story

The choice between default-off, redaction, and sampling needs security review before this field can leave the cluster. Tracked here rather than under R2 because the decision is about *whether* to ship the field at all, not how to map it.

### <a id="sec-4-12"></a>4.12 Integration test against the real gateway

The PoC's IT uses an in-process recording server, and that test still serves its purpose: it pins down the ES-side behavior without a cross-component dependency. What's missing is an additional IT that runs against the real `otel-delivery-gateway` component, to validate the contract between ES and the gateway end-to-end.

### <a id="sec-4-13"></a>4.13 Cloud API key audit shape is structurally incomplete

`LoggingAuditTrail.addAuthenticationFieldsToLogEntry(...)` carries an `assert false == authentication.isCloudApiKey()` (added in [PR #129227](https://github.com/elastic/elasticsearch/pull/129227), June 2025) labelled *"audit logging for Cloud API keys is not supported"*. The assert is a Java-`assert` only — it's a no-op in production (`-ea` is off) but fires in tests. Inspecting the method body, the path runs cleanly for Cloud API keys; the assert is documenting a structural-shape gap, not guarding a crash:

- `Authentication.isApiKey()` returns `false` for Cloud API keys (their `Subject.Type` is `CLOUD_API_KEY`, not `API_KEY`), so the `if (authentication.isApiKey() || authentication.isCrossClusterAccess())` branch on the API-key path never runs. As a result, **`api_key.id` and `api_key.name` are not populated** for any Cloud-API-key-authenticated audit event. The event ends up with `principal_realm = _cloud_api_key` and an `authentication_type = api_key` but no key identifier. An audit consumer expecting `api_key.id` for API-key-typed events sees a hole.
- Separately, on serverless the dominant authentication method is UIAM (Cloud API keys), and UIAM doesn't put `X-Elastic-Project-Id` into `ThreadContext` itself — its project context lives in `CloudAuthenticateProjectContext` carried through auth metadata. So when only UIAM is in the picture (no platform ingress also setting the header), `withThreadContext` finds nothing and `project.id` is absent. In production the platform ingress *also* sets `X-Elastic-Project-Id`, so the header path works; but the dependency on the ingress for the R3 chokepoint to fire is a fragility worth noting, and any non-ingress UIAM call path (e.g. service-to-service inside ECP) would emit audit events with no `project.id`. (The "ingress always sets the header" assumption should be confirmed authoritatively with the Control-Plane / Platform Ingress team, including the set of paths where it would *not* fire.) This is one specific manifestation of the broader audit-vs-auth divergence in [§4.14](#sec-4-14).

Decisions and work needed:

1. **Decide the audit shape for Cloud-API-key-authenticated requests.** Specifically: which fields populate the `api_key.*` namespace, given the source values live in Cloud API key metadata rather than the regular API key store? This is mostly an alignment conversation with the UIAM team (Slobodan Adamović owns the original assert).
2. **Source `project.id` from somewhere other than (or in addition to) the HTTP header.** Options: read from `CloudAuthenticateProjectContext` if present; pull from the `serverless.project_id` node setting; use the `CustomAuditLoggingMetadataProvider` extension point ([§5.4](#sec-5-4)) to let the serverless module inject project ID independently of the ingress header path. Whichever choice is made, decoupling R3 from ingress fragility is the win.
3. **Implement the chosen shape and remove the assert.** The fix unblocks the [§4.4](#sec-4-4) routing-direction conversation (which is gated on linked-side `project.id` being populated at all).

### <a id="sec-4-14"></a>4.14 Audit reads `project.id` via a different mechanism than authorization

The PoC populates `project.id` on audit records by having `LoggingAuditTrail.LogEntryBuilder.withThreadContext(...)` read `Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER` directly from `ThreadContext`. Authorization, by contrast, goes through `ProjectResolver.getProjectId()` — the canonical accessor — which has fallback logic on the same header. The two reads of "the current project" can disagree.

`ServerlessProjectResolver.getFallbackProjectId()` currently returns `Metadata.DEFAULT_PROJECT_ID` and is tagged `@FixForMultiProject(description = "This should throw an exception")`. A sibling check in `allowAccessToAllProjects(...)` is tagged `@FixForMultiProject(description = "Should require a user")` for the no-auth-allows-all-projects branch. Both are known multi-project defects scheduled for repair.

So today, the same request can be:

- **Authorized** as a real project (because `ProjectResolver` falls back to `DEFAULT_PROJECT_ID`, or grants access-to-all on missing-auth, per the annotations above), AND
- **Audited** with `project.id` absent (because the raw header is null and audit has no fallback).

That divergence is itself a structural bug: audit records should reflect what authorization actually used. It also subsumes more specific manifestations:

- The CPS linked-side gap ([§4.4](#sec-4-4)): header stripped at the cross-cluster boundary, resolver falls back to `DEFAULT_PROJECT_ID`, audit emits null.
- The Cloud API key / ingress dependency ([§4.13](#sec-4-13)): UIAM doesn't populate the header itself, so audit depends on the platform ingress; auth uses the resolver fallback or `CloudAuthenticateProjectContext` auth metadata.
- Any non-ingress-fronted serverless call path (e.g. service-to-service inside ECP) has the same shape: auth resolves something (legitimately or via the fallback bug), audit records nothing.

The fix is to pull audit and auth onto the same source of truth. [§5.4](#sec-5-4) lays out the constructor options; this session's analysis recommends `CustomAuditLoggingMetadataProvider`, since it handles the divergence in core ES *and* the linked-side and ingress-fragility manifestations in one place. See [§6.1.8](#sec-6-1-8).

Note on terminology: `DEFAULT_PROJECT_ID` is the legitimate marker only in non-serverless deployments (which have no project concept). In any serverless deployment — single-project per cluster or multi-project — every cluster has a real customer-assigned project ID, and `DEFAULT_PROJECT_ID` is always wrong. The storage-architecture axis (stateful = disk, stateless = object store) is orthogonal to this.

### <a id="sec-4-15"></a>4.15 Customer-facing audit configuration

Multi-project per-project settings scoping is explicitly out of scope for this work. In a project-per-cluster serverless deployment, the cluster is the project, so cluster-level settings are the right granularity.

The delivery path already exists. Customers configure audit logging via the Project API's public `PATCH /api/v1/serverless/projects/<type>/{id}` endpoint, already used today to enable audit logging at the project level. The API persists configuration in CosmosDB and propagates it via `elasticsearchappconfig` / `kibanaappconfig` Kubernetes resources to the regional `elasticsearch-controller` / `kibana-controller`, which renders file settings into the cluster. ES reads them via the generic `ReservedClusterSettingsAction` — the same handler used by data stream retention settings and other dynamically-delivered cluster settings. No dedicated handler is needed.

The audit filter and emit settings (`xpack.security.audit.logfile.events.include`, `events.exclude`, the five `events.ignore_filters.*` affix settings, and the six `emit_*` settings) are already declared `NodeScope + Dynamic` and already have `addSettingsUpdateConsumer` / `addAffixGroupUpdateConsumer` listeners registered in `LoggingAuditTrail`. Changes delivered via file settings fire the existing listeners and take effect without a restart. The only gap on the OTel-logs side is that `telemetry.otel.logs.enabled` and `telemetry.otel.logs.endpoint` are not yet `Dynamic`; see [§4.9](#sec-4-9).

The Customer-facing audit log configuration TDD ([§2](#sec-2) references) covers the full Project API shape, cross-app consistency between ES, Kibana, and Fleet. The customer-controllable settings inventory covers at least: five `xpack.security.audit.logfile.events.ignore_filters.*`, plus `events.include` and `events.exclude`. Valentin Crettaz is compiling a full mapping covering ES audit logs, Kibana audit logs, ES query logs, and Kibana user activity logs.

Two constraints bound the solution space:
- **`_cluster/settings` and any new in-app settings endpoint are off the table.** The cluster-settings endpoint is `@ServerlessScope(Scope.INTERNAL)`. Adding a new endpoint (e.g. `_security/audit/settings`) would split configuration between cloud UI and in-app, which is the thing the serverless architecture deliberately avoids (Tim Vernum, [#log-delivery-project-team, 2026-05-13](https://elastic.slack.com/archives/C09PANY7FFS/p1778666710166199)). The only customer path is the Project API.
- **The Project API has no arbitrary-cluster-settings facility.** Each exposed setting requires explicit Project API extension — it cannot be delegated to ES wholesale.

### <a id="sec-4-16"></a>4.16 Switch transport from HTTP-protobuf to gRPC

**Implemented.** See [§3.5](#sec-3-5).

## <a id="sec-5"></a>5. Decision points

Tradeoffs the team should weigh before further implementation work begins.

### <a id="sec-5-1"></a>5.1 Attribute-key shape

The upstream `OpenTelemetryAppender` library hardcodes a `log4j.map_message.` prefix on every attribute that comes from a `StringMapMessage`. Today's PoC records carry `log4j.map_message.event.action` etc. instead of bare `event.action`. The integration test asserts on the prefixed names with a comment noting it's PoC-only; production needs bare keys.

| Option | Pros | Cons |
|---|---|---|
| **Custom log4j appender (~50 LOC)** | Narrow scope. No incubator/extension deps. Aligned with where v3 of the upstream library is going (prefix off by default). Easy to extend with semconv translation later. | We own a small log4j component; reviewers may ask "why not use upstream?" |
| **`v3_preview` wrapper (~15 LOC)** | Uses upstream library as designed. Smallest delta. | Pulls in `opentelemetry-api-incubator` (marked unstable) and `opentelemetry-sdk-extension-declarative-config`. Enables *all* v3-preview-gated behavior in the OTel logs path, not just the prefix. |
| **Switch the apm module to `AutoConfiguredOpenTelemetrySdk`** | Aligns ES with OTel's standard config story; system properties / YAML drive SDK behavior. Worth doing for reasons beyond this feature. | Substantial refactor; affects metrics and traces too. Bigger conversation than this epic. |
| **`LogRecordProcessor` post-hoc rename (~30 LOC)** | Keeps upstream appender. Localized change. | Smell: prefix is added by one stage and stripped by the next. Doesn't solve semconv translation, only the prefix. |

### <a id="sec-5-2"></a>5.2 Field mapping shape: semconv vs ECS vs custom

**Update (2026-06-10):** Julio Camarero created a "Serverless Audit Log Field Reference" doc (2026-06-04, last updated 2026-06-10) that defines the canonical OTel/ECS field contract across ES, Kibana, and CP. That doc is the authoritative spec for PR 2 and supersedes the per-field table below. Read it before starting the custom appender implementation. Selected resolutions from that doc relevant to open questions in this table: `origin.address` → `source.address` (HTTP); `elasticsearch.audit.indices` and `elasticsearch.audit.request.name` confirmed as custom `⚙️` attributes; `trace.id` and `span.id` go in the OTel log record envelope (not as attributes); `data_stream.*` stamped by the gateway, not emitted by ES; `origin.type` explicitly excluded from Serverless.

The Gateway TDD asks emitting services to use OTel semconv where it exists, ECS where it doesn't, custom names as last resort. Most ES audit fields have a clean answer; some are genuinely ambiguous. The decision affects how downstream consumers query and join audit data.

ES and Kibana audit records must use consistent field names — customers configure audit logging as a unified journey across both products, not per-product. The Kibana team must be part of the naming decisions on the hard rows.

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

### <a id="sec-5-3"></a>5.3 Where the SDK setup lives

`OtelSdkExportLogsSupplier` lives in `modules/apm/`, alongside the meter/tracer suppliers. The apm module's stated purpose is "ES's own observability" (metrics + traces back to Elastic); see [TRACING.md](https://github.com/elastic/elasticsearch/blob/b037081bb93f8e9466438980c4855fd740a38a79/TRACING.md) for the existing infrastructure there. Customer-facing audit-log delivery is conceptually different; it ships data the customer cares about, not telemetry about ES itself.

Options:
- Keep it in `modules/apm/`. Cheap, consistent with the metrics/tracer pattern, tolerable in the short term.
- Carve out a new sibling module (e.g. `modules/customer-telemetry/`). Cleaner conceptual boundary; adds a module.

This is worth deciding before the audit-log path grows further.

### <a id="sec-5-4"></a>5.4 `LoggingAuditTrail` constructor signature for `project.id`

The PoC reads `X-Elastic-Project-Id` from `ThreadContext` directly inside `withThreadContext()`, which works for the common origin-customer-via-ingress path. [§4.14](#sec-4-14) makes a stronger argument for changing it: audit reads the raw header, auth uses `ProjectResolver` (with `@FixForMultiProject`-tagged fallbacks), and the two can disagree on what project the request is operating under. Three options:

- **`CustomAuditLoggingMetadataProvider`** (Audit TDD Req 7). **Recommended.** The Serverless module registers a provider that injects `project.id` from whichever source is right for the call path (resolver, cluster config, UIAM auth metadata). Non-serverless ES has no provider loaded, so the field is structurally absent. Cleanly handles the [§4.14](#sec-4-14) divergence, the [§4.4](#sec-4-4) linked-side gap, and the [§4.13](#sec-4-13) ingress fragility in one place; aligned with the audit TDD's design; lets the dual-deployment discriminator (serverless vs not) fall out of which modules load. Worth coordinating with the audit TDD owner on contract details.
- **Inject `ProjectResolver`** into the `LoggingAuditTrail` constructor. Reads from the canonical accessor, so audit and auth agree by construction. Doesn't solve the linked-side gap on its own (resolver returns `DEFAULT_PROJECT_ID` there until the `@FixForMultiProject` fallback is repaired) and doesn't isolate the project concept from non-serverless ES.
- **Keep the direct `ThreadContext` read.** Status quo. Leaves [§4.14](#sec-4-14) unresolved.

### <a id="sec-5-5"></a>5.5 Per-setting plumbing vs. `ProjectScope` setting type

Multi-project concern; deferred. See [Appendix C](#sec-appendix-c).

## <a id="sec-6"></a>6. Next steps

Pulled from [§4](#sec-4) and [§5](#sec-5). **What's implemented:** R1 (OTLP delivery) and R5 (gRPC transport) are satisfied end-to-end ([§3.1](#sec-3-1), [§3.5](#sec-3-5)); R9 (non-blocking emit) is satisfied ([§3.2](#sec-3-2)); R3 is partially satisfied — `project.id` on 11 of 12 emit methods ([§3.3](#sec-3-3)).

### <a id="sec-6-1"></a>6.1 Decisions needed

1. <a id="sec-6-1-1"></a>**Attribute-key shape ([§5.1](#sec-5-1)).** Choose among the four options in §5.1. *[Core/Infra. Gates [6.2.8](#sec-6-2-8), [6.2.9](#sec-6-2-9).]*
2. <a id="sec-6-1-2"></a>**Per-field semconv/ECS/custom mapping ([§5.2](#sec-5-2)).** Resolve the hard rows: `apikey.*` namespace, `indices` array, `security_config_change` nested blobs. ES and Kibana audit records must use consistent names; Kibana team input required. *[Core/Infra + Ankit Sethi + Kibana team. Gates [6.2.9](#sec-6-2-9).]*
3. <a id="sec-6-1-3"></a>**CPS routing direction ([§4.4](#sec-4-4)).** Originator project or data-owner project? **Gated on [6.1.9](#sec-6-1-9) and [6.1.10](#sec-6-1-10)** — the linked side has no `project.id` to route on today. *[otel-delivery-gateway team.]*
4. <a id="sec-6-1-4"></a>**SDK module placement ([§5.3](#sec-5-3)).** Keep in `modules/apm/` or new `modules/customer-telemetry/`. *[Core/Infra.]*
6. <a id="sec-6-1-6"></a>**`request.body` PII story ([§4.11](#sec-4-11)).** Default-off vs redaction vs sampling; requires security review before the field can leave the cluster. *[ES Security.]*
8. <a id="sec-6-1-8"></a>**`LoggingAuditTrail` constructor ([§5.4](#sec-5-4)).** `CustomAuditLoggingMetadataProvider` vs inject `ProjectResolver` vs keep direct `ThreadContext` read. Doc recommends the extension point: it resolves [§4.14](#sec-4-14), [§4.4](#sec-4-4), and [§4.13](#sec-4-13) in one place; effectively merges with [6.1.10](#sec-6-1-10) if chosen. *[Core/Infra + Ankit Sethi. Gates [6.1.10](#sec-6-1-10), [6.2.11](#sec-6-2-11), [6.2.12](#sec-6-2-12).]*
9. <a id="sec-6-1-9"></a>**Cloud API key audit shape ([§4.13](#sec-4-13)).** What `api_key.*` holds for UIAM-authenticated events. *[UIAM team (Slobodan Adamović). Gates [6.2.12](#sec-6-2-12), [6.1.3](#sec-6-1-3).]*
10. <a id="sec-6-1-10"></a>**Linked-side `project.id` mechanism ([§4.4](#sec-4-4)).** Cluster-config-derived, cross-cluster transport propagation, or via `CustomAuditLoggingMetadataProvider`. Resolving [6.1.8](#sec-6-1-8) in favor of the extension point collapses this into "implement the linked-side provider." *[CPS team. Gates [6.2.11](#sec-6-2-11), [6.1.3](#sec-6-1-3).]*
11. <a id="sec-6-1-11"></a>**Per-project settings: Plan A vs Plan B ([§5.5](#sec-5-5)).** Multi-project concern; deferred. See [Appendix C](#sec-appendix-c). *[Core/Infra + Tim Vernum + Yang Wang + Julio Camarero.]*

### <a id="sec-6-2"></a>6.2 Implementation work

1. <a id="sec-6-2-1"></a>**R3: `security_config_change` chokepoint ([§4.3](#sec-4-3)).** Confirm with Ankit that the missing `withThreadContext` call is a bug; fix it and make it implicit in `build()`. Most of the cost is the alignment conversation.
2. <a id="sec-6-2-2"></a>**R4: mTLS to gateway ([§4.5](#sec-4-5)).** Needs cert-identity alignment with the gateway team, cert-distribution alignment with Control-Plane, and an open question on whether the gRPC exporter exposes client TLS directly.
3. <a id="sec-6-2-3"></a>**R6: strip cluster/node fields ([§4.6](#sec-4-6)).** Set `emit_node_id` and `emit_cluster_uuid` to `false` in `serverless-default-settings.yml`. No ES code change.
4. <a id="sec-6-2-4"></a>**R7: stdout fallback on exhausted retries ([§4.7](#sec-4-7)).** Format and replay-ingestion contract need alignment with the gateway and replay-service teams.
5. <a id="sec-6-2-5"></a>**R8: audit delivery toggle ([§4.8](#sec-4-8)).** In project-per-cluster, R8 and R12 share the same mechanism; see [6.2.6](#sec-6-2-6).
6. <a id="sec-6-2-6"></a>**R12: dynamic OTel delivery toggle ([§4.9](#sec-4-9)).** Add a second Dynamic NodeScope setting for runtime on/off. Introduce a `LogRecordProcessor` wrapping `BatchLogRecordProcessor` that gates delivery on a `BooleanSupplier` updated by a settings listener — no SDK teardown needed. Suggested ownership: Security/audit team (Ankit Sethi), as this processor is the seam that evolves to per-project delivery in multi-project. See [§6.3](#sec-6-3).
7. <a id="sec-6-2-7"></a>**R13: retry/buffer tuning ([§4.10](#sec-4-10)).** Straightforward once the gateway team confirms the ~2 min retry / ~30–50 MB buffer targets.
8. <a id="sec-6-2-8"></a>**Bare semconv keys on the wire ([§4.1](#sec-4-1)).** Follows [6.1.1](#sec-6-1-1).
9. <a id="sec-6-2-9"></a>**Per-field translation ([§4.2](#sec-4-2)).** Follows [6.1.1](#sec-6-1-1) and [6.1.2](#sec-6-1-2).
10. <a id="sec-6-2-10"></a>**Real-gateway integration test ([§4.12](#sec-4-12)).** Add alongside (not replacing) the existing in-process IT; test shape and ownership need alignment with the gateway team.
11. <a id="sec-6-2-11"></a>**Linked-cluster `project.id` ([§4.4](#sec-4-4)).** Follows [6.1.10](#sec-6-1-10); gates [6.1.3](#sec-6-1-3).
12. <a id="sec-6-2-12"></a>**Decouple R3 from platform ingress; Cloud API key shape ([§4.13](#sec-4-13)).** Source `project.id` from UIAM auth context or node config; populate `api_key.*`; remove the structural assert. Follows [6.1.9](#sec-6-1-9).

### <a id="sec-6-3"></a>6.3 Questions by audience

Routing view of [§6.1](#sec-6-1) and [§6.2](#sec-6-2).

#### `otel-delivery-gateway` team (`#log-delivery-project-team`)

1. **mTLS contract.** Cert identity; whether gRPC exporter exposes client TLS directly. ([§4.5](#sec-4-5), [§6.2.2](#sec-6-2-2))
2. **Stdout fallback format.** ([§4.7](#sec-4-7), [§6.2.4](#sec-6-2-4))
3. **Retry/buffer targets.** Confirm ~2 min retry / ~30–50 MB buffer. ([§4.10](#sec-4-10), [§6.2.7](#sec-6-2-7))
4. **Real-gateway IT.** Test-environment shape, ownership. ([§4.12](#sec-4-12), [§6.2.10](#sec-6-2-10))
5. **CPS routing direction.** Gated on [§6.1.9](#sec-6-1-9) + [§6.1.10](#sec-6-1-10) first. ([§4.4](#sec-4-4), [§6.1.3](#sec-6-1-3))
6. **`request.body` PII handling.** Consulted; ES Security owns. ([§4.11](#sec-4-11), [§6.1.6](#sec-6-1-6))

#### Audit TDD owner (Ankit Sethi)

1. **Is `security_config_change` missing `withThreadContext` a bug?** ([§4.3](#sec-4-3), [§6.2.1](#sec-6-2-1))
2. **`CustomAuditLoggingMetadataProvider` contract** (Req 7 of his TDD). ([§5.4](#sec-5-4), [§6.1.8](#sec-6-1-8), [§6.1.10](#sec-6-1-10))
3. **Hard rows in the field-mapping table.** `apikey.*`, `indices`, nested `security_config_change` blobs. Needs Kibana team input for cross-product naming consistency. ([§5.2](#sec-5-2), [§6.1.2](#sec-6-1-2))
4. **R12 gating processor: ownership and design.** A `LogRecordProcessor` wrapping `BatchLogRecordProcessor` is the suggested home for the dynamic OTel delivery check — it gates on a `BooleanSupplier` that today is a volatile boolean and in multi-project becomes a per-project ProjectScope setting accessor. Worth confirming that the Security team owns this and agreeing on the interface before implementation. ([§4.9](#sec-4-9), [§6.2.6](#sec-6-2-6))

#### CPS team

1. **Linked-side `project.id` mechanism.** Three options at [§4.4](#sec-4-4). ([§6.1.10](#sec-6-1-10))
2. **Same-cluster CPS sanity check.** Does `executeOnProject(...)` carry `project.id` correctly today? ([§4.4](#sec-4-4))

#### UIAM team (Slobodan Adamović)

1. **Cloud API key `api_key.*` shape.** ([§4.13](#sec-4-13), [§6.1.9](#sec-6-1-9))

#### Control-Plane / Platform Ingress team

1. **`X-Elastic-Project-Id` invariant.** Confirm always-set on inbound customer HTTP; list paths where it doesn't fire. ([§3.3](#sec-3-3), [§4.13](#sec-4-13))

#### Control-Plane / `elasticsearch-controller` team (Julio Camarero)

1. **Project API audit-configuration fields** and their file-settings shape. ([§4.15](#sec-4-15), [§6.2.5](#sec-6-2-5))
2. **Cert distribution path** for the gateway client cert. ([§4.5](#sec-4-5), [§6.2.2](#sec-6-2-2))

#### ES Security team

1. **`request.body` PII story.** ([§4.11](#sec-4-11), [§6.1.6](#sec-6-1-6))

#### Platform-side replay-service team

1. **Stdout ingestion contract.** ([§4.7](#sec-4-7), [§6.2.4](#sec-6-2-4))

#### Serverless platform team

1. **R6 strip-fields.** Set `emit_node_id` and `emit_cluster_uuid` to `false` in `serverless-default-settings.yml`. ([§4.6](#sec-4-6), [§6.2.3](#sec-6-2-3))

## <a id="sec-appendix-a"></a>Appendix A: Options considered and rejected

Paths the PoC explored before evidence pushed us elsewhere.

### <a id="sec-a-1"></a>A.1 Architecture B: write to file, sidecar tails it

Keep the existing rolling-file audit appender, deploy a sidecar OTel collector with a `filelog` receiver to tail the file and forward via OTLP. It is operationally appealing because it survives ES-process death; the launcher can drain remaining file content like a heap dump, which is arguably the safer-by-default behavior for an audit pipeline.

We rejected this approach because the team that owns the gateway has scoped their pipeline assuming OTLP ingress from the application (see Gateway TDD §"OTel log delivery pipeline on ECP"). Architecture A is what the Jira description and the gateway TDD assume. If the gateway team changes their mind, B becomes attractive again, but that's a cross-team conversation.

### <a id="sec-a-2"></a>A.2 Declaring `OpenTelemetryAppender` in `log4j2.properties`

Initial implementation declared the OTel appender in `x-pack/plugin/core/src/main/config/log4j2.properties` like any other appender. ES boot failed:

```
main ERROR Unable to locate plugin type for OpenTelemetry
main ERROR Unable to locate plugin for OpenTelemetry
main ERROR Unable to invoke factory method ... NullPointerException
main ERROR Unable to locate appender "audit_rolling" for logger config "..."  ← cascade
main ERROR Unable to locate appender "console" for logger config "root"        ← cascade
```

The properties file is parsed at JVM startup, before plugin/module classloaders are set up; the appender plugin class is in the apm module's classloader and isn't reachable yet. The NPE that follows cascades and breaks **all** appender registration, not just the OpenTelemetry one; this means audit logging would be silently broken.

The working solution, now implemented, attaches the appender programmatically in `OtelSdkExportLogsSupplier.install(...)` after the apm module is loaded. It builds the appender via `OpenTelemetryAppender.builder().setOpenTelemetry(sdk).build()` (passing the SDK directly on the builder rather than via the static `install(...)` method, which has its own ordering subtleties) and attaches it to the audit logger's `LoggerConfig`.

### <a id="sec-a-3"></a>A.3 Putting the OTel deps in `:server`

We tried this as a fix for [A.2](#sec-a-2): with the appender JAR on `:server`'s classpath, log4j can resolve it at boot. ES then failed with **jar hell**: `opentelemetry-context` is also pulled transitively by `:x-pack:plugin:esql-datasource-gcs` at a different version, and ES rejects cross-classpath duplicate classes at plugin-load time. We unwound the `:server` change and kept everything in `modules/apm/`, going with the programmatic-attach path instead.

### <a id="sec-a-4"></a>A.4 Direct OTel SDK call from `LoggingAuditTrail`

Rather than going through log4j at all, have `LoggingAuditTrail.LogEntryBuilder` emit OTLP records directly via `OpenTelemetry.getLogsBridge()`. We considered this approach: it is the most controlled shape and bypasses the whole appender library. We didn't pursue it because the goal is for an appender (the natural log4j extension point) to handle the conversion. The custom-appender option in [§5.1](#sec-5-1) captures the right shape without modifying `LoggingAuditTrail`.

## <a id="sec-appendix-b"></a>Appendix B: Findings worth knowing

- **`manage_threads` entitlement** is required for `io.opentelemetry.sdk.logs` because `BatchLogRecordProcessor` spawns a worker thread. First test run failed with `NotEntitledException` until added to `entitlement-policy.yaml`.
- **Hand-wired OTel SDK and declarative config don't compose easily.** ES uses `OpenTelemetrySdk.builder()` (the bare SDK builder), which has no public way to set a `ConfigProvider`. As a result, instrumentation-config flags can't be set through the public SDK API. The most relevant example is `otel.instrumentation.common.v3-preview`, the upstream library's preview knob for dropping the `log4j.map_message.` prefix. They're settable via `AutoConfiguredOpenTelemetrySdk.builder().addPropertiesSupplier(...)` (auto-configure module) or by implementing `ExtendedOpenTelemetry` ourselves with a `YamlDeclarativeConfigProperties.create(Map, ComponentLoader)` (incubator + declarative-config extension modules). This is a real OTel-Java friction point that affects more than just this feature. The OTel configuration spec itself requires SDKs to expose programmatic config; the Java SDK does, but only outside the bare builder.
- **The `OpenTelemetryAppender.install(sdk)` static method is order-dependent.** It pushes the SDK to *already-registered* `OpenTelemetryAppender` instances. Calling it before the appender is in the log4j config is a silent no-op for that appender. Using `Builder.setOpenTelemetry(sdk)` directly is more robust.
- **Audit events use `StringMapMessage`, which is structured but bodyless.** The OTel appender library treats this as "no `LogRecord.body`, capture entries as attributes." With `setCaptureMapMessageAttributes(true)` this works but adds the prefix discussed in [§5.1](#sec-5-1).

## <a id="sec-appendix-c"></a>Appendix C: Multi-project considerations (deferred)

These considerations become relevant when multi-project support is added. They are preserved here rather than in the main sections to keep the project-per-cluster scope clear.

### <a id="sec-c-1"></a>C.1 R8 in multi-project: ES-side filtering rationale

In a multi-project cluster, ES must drop audit records for projects whose customers haven't opted into audit logging, *before* the records reach the gateway, so that disabled projects don't pay the network hop.

**Placement** (must be ES-side; downstream components can't filter per project): in the [#log-delivery-project-team thread of 2026-04-29](https://elastic.slack.com/archives/C09PANY7FFS/p1777474914646739), Ankit Sethi wrote *"a lot of this can/should happen within the security plugin itself"*; Valentin Crettaz concurred *"MOTel is too far away down the delivery pipeline to do this, the filtering needs to happen at the source (i.e. by ES/Serverless itself)"*; Julio Camarero wrote *"since these logs are sent over HTTP, we can save a lot of resources if only logs which should be routed are sent, therefore, I fully agree with the approach of filtering happening at elasticsearch."* In a parallel [#hosted-otel-collector thread the same day](https://elastic.slack.com/archives/C076MUD9BK8/p1777471821757409), Vignesh Shanmugam wrote *"the processing/redact bits should live as part of the SDK layer inside ES for now till we have support for managed processing layer"*, and Andrew Wilkins later noted *"otel-delivery-gateway is the thing that would be transforming logs before they hit MOTel"* (i.e. the eventual external home for transformations is the gateway, not MOTel). As of today, neither MOTel nor the gateway filters per project.

**Durable reason** (ES-side even if downstream filtering eventually becomes possible): cost. Emitting OTLP for N projects only to drop most of it downstream still pays the ES → gateway network hop. Even a future gateway-side filter wouldn't satisfy this; the cost is incurred before the filter sees the record.

### <a id="sec-c-2"></a>C.2 Per-project settings design: Plan A vs Plan B

In a multi-project cluster, each project may want different audit configuration, requiring per-project scoping of what are currently cluster-level settings. Exposing any of these per-project requires per-setting plumbing (Plan A) or a framework-level solution (Plan B). Audit alone would need at least seven settings; Valentin Crettaz is compiling a full inventory covering ES audit logs, Kibana audit logs, ES query logs, and Kibana user activity logs. Most cluster settings in serverless are already not user-configurable, so the universe of settings that need per-project scope is smaller than it might appear.

A structural alternative, raised in [#log-delivery-project-team 2026-05-13](https://elastic.slack.com/archives/C8UUBNASY/p1778614459655819): introduce a `ProjectScope` setting type alongside the existing `NodeScope`. The idea:

- A `ProjectScope` setting knows how to source its value contextually: from `Settings` / cluster settings in non-serverless deployments; from per-project file settings (rendered into a `ProjectCustom` by the reserved-state handler) in serverless.
- Existing `NodeScope` settings can be converted to `ProjectScope` over time without callsite refactors — the `Setting<T>` itself handles the dual lookup, and the listeners run in a thread context where `ProjectLocal.get()` returns the right per-project value.
- Mark Vieira (Core/Infra) green-lit the direction in principle: *"That should be possible. If they are existing settings, you'd just make them project scoped and then it would be set by the existing file-based settings mechanism for cluster settings."* (with the caveat: *"with some work in this area to make per-project settings more ergonomic"*).

| Option | Pros | Cons |
|---|---|---|
| **Plan A: per-setting plumbing.** Each setting we expose gets its own field in the Project API, its own field in a `ProjectCustom`, and its own consumer refactor. | Smallest delta for the specific audit settings needed. No new framework. Current recommended path for existing cluster settings that need per-project scope. | Repeats the four-step ritual for every future per-project setting. Drift risk between `Setting<T>` and `ProjectCustom` representations. |
| **Plan B: `ProjectScope` setting type.** Build the framework once; every `ProjectScope` setting routes correctly without per-callsite work. | Amortizes cost across settings beyond audit. Cleaner ergonomics; one `Setting<T>` per concept rather than parallel representations. Mark indicated this aligns with how Core/Infra would want per-project settings to work. | Larger upfront investment (framework + listener semantics + thread-context plumbing for `ProjectLocal`). Crosses into Tim Vernum / Yang Wang's design territory. The fallback mechanism is not yet built: in non-multi-project mode, asking for a per-project setting for the default project should return the cluster-setting value, and that plumbing doesn't exist today. |

This decision needs involvement from Tim Vernum (canonical project-id-resolution pattern; see [§4.14](#sec-4-14)), Yang Wang, and Julio Camarero. See also the Customer-facing audit log configuration TDD ([§2](#sec-2) references). An in-app settings endpoint is not an option; see [§4.15](#sec-4-15).
