# OTel audit log delivery: POC writeup for ES-14356

**Author:** Patrick Doyle
**Status:** Draft for review

The point of this writeup is twofold: (1) to record what's been validated by this PoC, and (2) to make the gaps between PoC scope and a production-ready feature explicit, so estimates of remaining work are grounded.

## 1. Goal

Make Elasticsearch audit logs deliverable to serverless customers via OTLP. Today, audit logs are written to a per-cluster JSON file by `LoggingAuditTrail`; in self-hosted deployments, customers ship that file with Filebeat. Serverless has no Filebeat sidecar and no shared disk, so the file path doesn't apply.

The cross-team plan: emitting services (ES, Kibana, Control-Plane services) send OTLP log records over the network to an `otel-delivery-gateway` running in the same ECP K8s cluster. That gateway routes records to per-project MOTel endpoints, which write to the customer's destination Elasticsearch project. Architecture is documented in the gateway TDD (see §2 references).

This PoC validates that ES can emit OTLP audit logs end-to-end and surfaces the work that remains.

## 2. Requirements

References for the requirements below:

- **Audit TDD**: *TDD - Audit Logging in Elasticsearch* (Ankit Sethi). [Google Doc](https://docs.google.com/document/d/1ox4Hc-Ga_HMQtpFNzvBivHbn0khBOzzL4hN6WsMH8ko/edit). Frames the audit-logic-side concerns (filtering, redaction, namespacing) as the broader audit-in-serverless effort. ES-14356 is the *delivery* slice of this; most of the audit TDD's eight requirements are owned outside this PoC (see §2.14 below).
- **Gateway TDD**: *Shipping Logs to Elastic Cloud customers (TDD)*. [Google Doc](https://docs.google.com/document/d/12IZxcX5uoFWvIhfff1I3DwRVMt8ab6tNkaIKs-Fx8Y8/edit). Frames the cross-team architecture from emitting service to MOTel.
- **ES-14356**: parent epic for the ES-side delivery work. [Jira](https://elasticco.atlassian.net/browse/ES-14356).
- **ES-13255**: broader "ES logs over OTel". [Jira](https://elasticco.atlassian.net/browse/ES-13255). Tracks any non-audit ES log streams; out of scope here.
- **elasticsearch-team#2170**: tracking issue for audit-in-serverless work; the issue body itself is a one-line pointer to the `#log-delivery-project-team` Slack channel where the work is being discussed. [GitHub](https://github.com/elastic/elasticsearch-team/issues/2170).

Each requirement below has a status of *Satisfied*, *Partially satisfied*, or *Gap*. The status line points to §3 evidence or §4 gap analysis.

### 2.1 R1: Emit audit events via OTLP to the OTel gateway

*Satisfied (§3.1).*

The headline behavior of ES-14356: each `LoggingAuditTrail` event must reach the `otel-delivery-gateway` (running in the same ECP K8s cluster as ES) over OTLP. The gateway then routes the record to a per-project MOTel endpoint, which writes to the customer's destination project.

Sources: Audit TDD §"Technical Dependencies": *"ES Core/Infra will take on adding support for a new Log4J appender that ships logs over the wire."* Gateway TDD §"How services emit (audit) log records": *"Must emit logs through the otlp protocol, on the network, towards the local otel-delivery-gateway service."*

### 2.2 R2: Use OTel semconv field names and JSON structure

*Partially satisfied (§3.4); both the bare-key shape and per-field translation are gaps (§4.1, §4.2).*

Records must use the OTel JSON top-level structure (`@timestamp`, `attributes`, `body`, `resource.attributes`) and OTel semconv field names where they exist, falling back to ECS, then to custom names only when neither has one.

Two pieces are open:
1. the upstream `OpenTelemetryAppender` library prepends `log4j.map_message.` to every attribute that comes from a `StringMapMessage`, so today's records carry `log4j.map_message.event.action` instead of `event.action`. §5.1 lists options for dropping the prefix.
2. the per-field translation from current ES audit names to semconv/ECS/custom hasn't been done. §5.2 has the per-field table.

Sources: Gateway TDD §"Standardized Audit Log schema" + §"How services emit"; Audit TDD §"Requirement 8".

### 2.3 R3: Enrich audit events with `project.id`

*Partially satisfied (§3.3). Two open gaps: `security_config_change` events bypass the chokepoint (§4.3); CPS routing direction is unconfirmed (§4.4).*

Audit events must carry `project.id` so the gateway can route per-project. The PoC plumbs the field through `LoggingAuditTrail.LogEntryBuilder.withThreadContext()`. The 12 public audit-emit methods all reach this chokepoint; however, `accessGranted` also emits a parallel `security_config_change` log entry through a private builder factory that bypasses `withThreadContext()`, so events of `event.type=security_config_change` (put_user, put_role, change_password, create_api_key, etc.) do not carry `project.id`. End-to-end OTLP delivery has been verified for one event type only.

Cross-Project Search introduces a design question: when a query fans out from project A to project B within a cluster, audit events on project B's side carry project B's ID, not project A's. Whether this matches the gateway's routing intent has not been confirmed; see §4.4. (Cluster-internal traffic, by contrast, is explicitly out of scope per §2.14.)

Sources: Audit TDD §"Requirement 7"; Gateway TDD §"Standardized Audit Log schema" / Appendix 5.

### 2.4 R4: Authenticate to the gateway via mTLS

*Gap (§4).*

The application identifies itself to the `otel-delivery-gateway` with a client certificate; the gateway uses the cert's identity to validate that a record's claimed source matches its TLS origin. The PoC uses plain HTTP without TLS.

Source: Gateway TDD §"OTel log delivery pipeline on ECP", subsection "Authentication & authorization".

### 2.5 R5: Transport over OTLP

*Satisfied (§3.5).*

Wire transport is OTLP. The PoC already uses OTLP/HTTP-protobuf, the planned production target. The Gateway TDD parenthetically writes "via the otlp protocol (grpc)"; the project's stance is to push back on gRPC and stay on HTTP unless a concrete reason for gRPC surfaces.

Sources: Gateway TDD §"How services emit (audit) log records": *"Must emit logs through the otlp protocol, on the network, towards the local otel-delivery-gateway service"*; §"OTel log delivery pipeline on ECP": *"via the otlp protocol (grpc)"* (parenthetical, not load-bearing).

### 2.6 R6: Strip cluster identity fields on serverless

*Gap (§4.6); the existing per-field `emit_*` settings make this small.*

On serverless, audit records must not carry `cluster.name`, `cluster.uuid`, `node.name`, or `node.id`. These expose platform internals customers shouldn't see. The Audit TDD §"Requirement 2" frames this as a `log4j2.serverless.properties` change (remove the four `%map{...}` PatternLayout entries from the file appender). That alone doesn't cover the OTel path: the fields aren't only in the PatternLayout, they're also in the audit `StringMapMessage` itself (populated by `LoggingAuditTrail.EntryCommonFields`), and with `setCaptureMapMessageAttributes(true)` on the `OpenTelemetryAppender` every OTel record picks them up too.

Fortunately, `LoggingAuditTrail` already gates each of these fields on a per-field `Property.Dynamic` setting: `xpack.security.audit.logfile.emit_node_name` (default `false`), `emit_node_id` (default `true`), `emit_cluster_name` (default `false`), `emit_cluster_uuid` (default `true`). Setting the two true-by-default ones to `false` on serverless suppresses the fields at the `StringMapMessage` source, so both the file appender and the OTel appender see them stripped. See §4.6.

Source: Audit TDD §"Requirement 2".

### 2.7 R7: Stdout fallback when retries are exhausted

*Gap (§4).*

If the application can't reach the gateway after its configured retries, it falls back to writing the audit record to stdout in a recognizable form. The internal observability pipeline picks that up, and a separate "otel-delivery-replay" service queries it back into the audit pipeline. Not implemented.

Source: Gateway TDD §"Unavailability issues mitigation" point 1: *"In case of continuous unavailability exhausting all retries, the service should fallback to logging the audit record on stdout, to be collected through the internal observability pipeline as regular application logs."*

### 2.8 R8: Filter audit events per project ID

*Gap (§4).*

In a multi-project setup, ES must drop audit records for projects whose customers haven't opted into audit logging, *before* the records reach the gateway, so that disabled projects don't pay the network hop.

**Placement** (must be ES-side; downstream components can't filter per project today): in the [#log-delivery-project-team thread of 2026-04-29](https://elastic.slack.com/archives/C09PANY7FFS/p1777474914646739), Ankit Sethi wrote *"a lot of this can/should happen within the security plugin itself"*; Valentin Crettaz concurred *"MOTel is too far away down the delivery pipeline to do this, the filtering needs to happen at the source (i.e. by ES/Serverless itself)"*; Julio Camarero wrote *"since these logs are sent over HTTP, we can save a lot of resources if only logs which should be routed are sent, therefore, I fully agree with the approach of filtering happening at elasticsearch."* In a parallel [#hosted-otel-collector thread the same day](https://elastic.slack.com/archives/C076MUD9BK8/p1777471821757409), Vignesh Shanmugam wrote *"the processing/redact bits should live as part of the SDK layer inside ES for now till we have support for managed processing layer"*, and Andrew Wilkins later noted *"otel-delivery-gateway is the thing that would be transforming logs before they hit MOTel"* (i.e. the eventual external home for transformations is the gateway, not MOTel). As of today, neither MOTel nor the gateway filters per project.

**Durable reason** (ES-side even if downstream filtering eventually becomes possible): cost. Emitting OTLP for N projects only to drop most of it downstream still pays the ES → gateway network hop. Even a future gateway-side filter wouldn't satisfy this; the cost is incurred before the filter sees the record.

### 2.9 R9: Non-blocking emit path

*Satisfied (§3.2).*

The audit-emit path must not block transport or network threads. The OTel SDK's `BatchLogRecordProcessor` queues records and ships them on its own worker thread, so the property is inherited from the SDK rather than engineered into ES (§3.2 walks the call chain).

Henning Andersen raised the requirement in a comment on the Audit TDD: *"I remember you had thoughts on how shipping audit logs could work in a way that did not require a new shipper, still allowed spill to disk and would not block transport threads. This seems to be a significant part of the work unless this is considered separately."* Tim Vernum followed up: *"Will the Core/Infra work guarantee that no blocking work is done on transport threads? I can't see where it's explicitly mentioned in this doc or the ticket referenced above."* The underlying rationale appears in the Audit TDD §"Why is audit logging not already enabled?" point 2: *"Writing to disk occurs on transport threads, adding a source of instability."*

### 2.10 R10: Best-effort delivery

*Partially satisfied (§3.2); becomes fully satisfied once R7 lands.*

A failed log send must not fail the originating operation. The OTel SDK's bounded queue drops on full rather than blocking, so a slow gateway doesn't fail the originating operation. Partial because the PoC doesn't yet implement R7: dropped records aren't preserved anywhere, so customers may lose audit records during gateway outages.

Sources: Gateway TDD §"Reliability & throughput": *"From the application/service perspective, we still move forward with executing an operation if writing the audit log fails. We favor availability of the service over consistency of the audit logs"*; §"How services emit": *"Should consider audit log delivery as 'best effort'..."*

### 2.11 R11: At-least-once delivery semantics

*Partially satisfied; full at-least-once requires R7.*

Records are not silently dropped during normal operation; occasional duplicates are acceptable. The PoC inherits the SDK's retry-and-buffer behavior, which gives at-least-once *while the gateway is reachable*. Persistent gateway unavailability still causes drops on queue overflow. Closing that gap is R7 (stdout fallback); once dropped records are written to stdout and replayed by the platform-side replay service, true at-least-once is reached.

Source: Gateway TDD §"Reliability & throughput" + §"Unavailability issues mitigation".

### 2.12 R12: Configuration changes without cluster restart

*Gap (§4).*

Toggling audit logging must take effect without a restart, both at cluster level and per project (R8). The PoC reads `telemetry.otel.logs.enabled` once in `OtelSdkExportLogsSupplier.install()` at startup, so toggling it requires a restart today.

For the cluster-level case, Audit TDD §"Requirement 1" calls for *"... enable or disable audit logging with a simple setting on the Cloud console without any downtime (cluster restart)"*; this is in flight under [PR 147333](https://github.com/elastic/elasticsearch/pull/147333) for `xpack.security.audit.enabled`. That setting governs the audit logger, not the OTel-logs path; we still need our own dynamic listener for `telemetry.otel.logs.enabled` and for R8's per-project state. (The R6 `emit_*` settings, by contrast, are already `Property.Dynamic`; toggling them on serverless requires no extra work.)

### 2.13 R13: Retry-policy and buffer-size targets

*Gap (§4).*

The OTel SDK should be configured to retry up to ~2 minutes (e.g. ~3 retries) and to bound the in-memory buffer at ~30–50 MB. The 4 MB/s per-container source-rate forecast in the Gateway TDD's Throughput section is an input for sizing this buffer (~10 s of headroom at the spec'd rate); it's not an ES-side requirement on its own, but it informs the buffer choice.

The PoC uses the SDK's defaults; specific tuning is straightforward once the values are decided.

Sources: Gateway TDD §"How services emit"; §"OTel log delivery pipeline on ECP"; §"Unavailability issues mitigation".

### 2.14 Non-requirements (explicitly out of scope for ES-14356)

These concerns belong to other tracks.

- **The existing `audit_rolling` file appender is unchanged.** Customers consuming `<cluster>_audit.json` with Filebeat keep doing so. Making `audit_rolling` non-blocking is also out of scope.
- **Non-audit ES log streams** (server, deprecation, slowlog, ESQL). Tracked under ES-13255.
- **Cluster-level dynamic enable/disable of audit logging** (Audit TDD Req 1; in flight via [PR 147333](https://github.com/elastic/elasticsearch/pull/147333)). Per-project granularity (R8 / R12) is on us; cluster-level is not.
- **Origin-type / realm filtering of internal traffic.** Audit TDD Reqs 3, 4. Owned by Ankit Sethi; tracked under elasticsearch-team#2170.
- **Operator-privilege placeholder for Elastic-employee access.** Audit TDD Req 5.
- **UIAM / CPS audit instrumentation.** Audit TDD Req 6.
- **Field namespacing currently done by Filebeat** (e.g. `user.realm` → `elasticsearch.audit.user.realm`). Audit TDD Req 8; lives in `log4j2.serverless.properties`, not here.
- **Internal infosec cluster delivery.** Gateway TDD §"Out of scope".
- **Production integration test against a real `otel-delivery-gateway`.** PoC's IT uses an in-process recording server.
- **The 99.9% delivery SLO** is a *system-level* property the ES side cannot uphold on its own; it depends on gateway, MOTel, and destination cluster availability. ES contributes to it via R7 (stdout fallback), R9 (non-blocking emit), R10 (best-effort), R11 (at-least-once), and R13 (retry/buffer sizing); it is not a standalone ES requirement.

## 3. What's implemented

The headline result: an audit event emitted by `LoggingAuditTrail` reaches a recording OTLP server end-to-end, in a security-enabled multi-node test, with `project.id` attached and the OTel logs SDK on the wire path.

### 3.1 End-to-end OTLP delivery: *satisfies R1*

The test is `OtelAuditLogsIT.testAuditEventArrivesAsOtlpLogRecord`.
The test boots a security-enabled `ElasticsearchCluster` with `xpack.security.audit.enabled=true`, `telemetry.otel.logs.enabled=true`, and the gateway endpoint pointed at an in-process `RecordingApmServer`. It hits `/_security/_authenticate` (which produces an `authentication_success` audit event), forces a flush via `/_flush_telemetry`, and asserts a `ReceivedTelemetry.ReceivedLog` arrives within `TELEMETRY_TIMEOUT`.

**Pipeline construction:**

- `OtelSdkExportLogsSupplier.install()` builds the OTel SDK (`SdkLoggerProvider` + `OtlpHttpLogRecordExporter` + `BatchLogRecordProcessor`), constructs an `OpenTelemetryAppender` programmatically, and attaches it to the audit logger's `LoggerConfig`. This must happen programmatically; see Appendix A.2 for why declaring the appender in `log4j2.properties` doesn't work.
- `APM.createComponents()` wires the supplier into the plugin lifecycle and exposes it via `APMTelemetryProvider`.
- Settings `telemetry.otel.logs.enabled` and `telemetry.otel.logs.endpoint` registered in `OtelSdkSettings` and added to the plugin's setting list.
- `attemptFlushLogs()` plumbed through `TelemetryProvider` and `APMTelemetryProvider` so tests and graceful shutdown can force-flush the `BatchLogRecordProcessor`.

### 3.2 Non-blocking on the calling thread: *satisfies R9; partially satisfies R10, R11*

The OTel SDK is used unmodified, and its publication path provably does not block. Trace:

1. `LoggingAuditTrail` calls `logger.info(AUDIT_MARKER, logEntry)` on a transport/REST thread. log4j2 dispatches via `LoggerConfig.log(...)` → `callAppenders(...)` → `OpenTelemetryAppender.append(LogEvent)` on the *same calling thread*.
2. `OpenTelemetryAppender.append` does only the log4j → OTel record translation (CPU only) and calls `LogRecordBuilder.emit()`.
3. `SdkLogRecordBuilder.emit()` invokes `getLogRecordProcessor().onEmit(...)`, which resolves to `BatchLogRecordProcessor.onEmit`.
4. `BatchLogRecordProcessor.onEmit` calls `worker.addLogRecord(...)`, which `offer`s the record onto a bounded `ArrayBlockingQueue` (default capacity 2048). On full, `offer` returns false and the record is **dropped**, not held.
5. The actual OTLP HTTP POST runs on a worker thread the `BatchLogRecordProcessor` owns, never on the caller.

Consequences:

- There is no back-pressure to the caller: a slow or unreachable gateway produces drops, not blocked transport threads.
- There is no I/O on the caller; network and serialization happen on the worker thread.

Per the [OTel logs SDK spec](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/logs/sdk.md#onemit), `LogRecordProcessor.OnEmit` *SHOULD NOT block* and overflow drops rather than back-pressures. We are in spec.

This is a property we *inherit* from the OTel SDK, not one we engineered into ES.

### 3.3 `project.id` enrichment: *partially satisfies R3*

**Mechanism:**

- `LoggingAuditTrail.PROJECT_ID_FIELD_NAME` introduced as the audit field name.
- `LogEntryBuilder.withThreadContext(...)` reads `Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER` from `ThreadContext` and writes `PROJECT_ID_FIELD_NAME` on the audit `StringMapMessage` if non-null.
- Audit JSON pattern in `x-pack/plugin/core/src/main/config/log4j2.properties` updated so the field appears in the file appender too.
- Eleven of the twelve public audit-emit methods on `LoggingAuditTrail` call `.withThreadContext(...)` at build time: `authenticationSuccess`, `authenticationFailed`, `accessGranted`, `accessDenied`, `anonymousAccessDenied`, `tamperedRequest`, `runAsGranted`, `runAsDenied`, `connectionGranted`, `connectionDenied`, `explicitIndexAccessEvent`. (`coordinatingActionResponse` is currently a no-op.)
- **Gap:** `accessGranted`, when the action falls in `SECURITY_CHANGE_ACTIONS`, also emits a parallel `security_config_change` log entry through `securityChangeLogEntryBuilder(...)`, a private builder factory that never calls `withThreadContext`. So `event.type=security_config_change` events do not carry `project.id`. See §4.3.

**Where the header gets populated:**

- Customer REST traffic on serverless: the ingress layer sets `X-Elastic-Project-Id` on the incoming HTTP request; ES copies it into `ThreadContext` before auth runs.
- Within-cluster transport fan-out: `ThreadContext` propagates the header via `HEADERS_TO_COPY` (handled in `ThreadContext.getRequestHeadersToCopy(...)`).
- Persistent tasks: `PersistentTasksNodeService.startTask(...)` puts the header for project-scoped tasks.
- Cluster-internal traffic (system users, internal cluster operations) is out of scope per §2.14 (Audit TDD Reqs 3, 4; elasticsearch-team#2170).

**Tests:**

- `LoggingAuditTrailTests` covers the mechanism: a setUp helper randomly sets the project header on `ThreadContext`, and a `projectId(...)` assertion helper checks the field on each audit event. The non-security-change tests invoke that helper; the `testSecurityConfigChangeEventFormatting*` tests do *not*, consistent with the gap above. The full class (35 tests) passed against this change at last run.
- `OtelAuditLogsIT.testAuditEventArrivesAsOtlpLogRecord` exercises end-to-end OTLP delivery for one event type: `authentication_success` from `/_security/_authenticate`. Other event types and transport-fan-out cases are not exercised by tests.

**Open question on CPS routing direction**: see §4.4.

### 3.4 Semconv field names arrive on the wire: *partially satisfies R2*

`OtelAuditLogsIT` asserts that `event.action` and `event.type` are present on the received `LogRecord` attributes. So the *fields* survive the log4j → OTel hop end-to-end.

**Caveat:** they arrive prefixed as `log4j.map_message.event.action`, because the upstream `OpenTelemetryAppender` library hardcodes that prefix when capturing `StringMapMessage` entries with `setCaptureMapMessageAttributes(true)`. Bare-key shape is not yet satisfied; see §5.1 for options.

### 3.5 Other tests (no regressions)

- `:modules:apm:thirdPartyAudit` runs clean: no jar-hell or banned-API regression from the new `opentelemetry-sdk-logs` and `opentelemetry-log4j-appender-2.17` dependencies. (See Appendix A.3 for the jar-hell episode that drove the module placement.)
- **OTLP/HTTP-protobuf wire encoding** is what the PoC actually uses, matching the planned production target. Exporter: `OtlpHttpLogRecordExporter.builder().setEndpoint(endpoint)` in `OtelSdkExportLogsSupplier`; the default encoding for that class is protobuf, not JSON. Confirmed on the receiving side by `OtlpLogsParser.parse(...)` which deserializes via `ExportLogsServiceRequest.parseFrom(InputStream)` (protobuf).
- `OtelSdkExportLogsSupplierTests` 7/7 passing; covers install/uninstall lifecycle of the supplier (idempotent install, detach on close, behavior when audit logger config is absent).
- ES boots cleanly under `./gradlew run` with audit and OTel logs enabled. The supplier emits `OTel SDK logs export installed; endpoint=...`, and the node reaches `started`.
- `manage_threads` entitlement registered in `modules/apm/src/main/plugin-metadata/entitlement-policy.yaml` for `io.opentelemetry.sdk.logs`. Without this, `BatchLogRecordProcessor`'s worker thread fails to start with `NotEntitledException`. (See Appendix B.)

## 4. What's not implemented (gap analysis)

For each requirement that the PoC doesn't yet satisfy, what's needed to make it real.

### 4.1 R2 (full): Bare semconv keys, no prefix

The upstream `OpenTelemetryAppender` library prepends `log4j.map_message.` to every attribute that comes from a `StringMapMessage`. Today's records carry `log4j.map_message.event.action` instead of bare `event.action`. The integration test asserts on the prefixed names with a comment noting it's PoC-only; production needs bare keys. §5.1 lays out four options, ranging from a ~15 LOC v3-preview wrapper to a small custom appender to a larger SDK-config refactor. A decision is needed before further audit-event mapping work, since the option chosen also gates how §5.2 (per-field translation) is implemented.

### 4.2 R2 (mapping): Field-by-field semconv translation

The gateway TDD asks emitting services to use OTel semconv where it exists, ECS where it doesn't, custom names as last resort. About half the ES audit fields have clean semconv answers: `event.action`, `user.name`, `http.request.method`, timestamp. The rest are genuinely ambiguous: the `origin.address` REST/transport split, the `apikey.*` namespace, the `indices` array, `request.body`, and the nested `security_config_change` blobs. §5.2 has the per-field table.

### 4.3 R3 (chokepoint): `security_config_change` events bypass `withThreadContext`

`accessGranted` emits two log entries when its action falls in `SECURITY_CHANGE_ACTIONS` (put_user, put_role, change_password, create_api_key, delete_role, …): the regular `access_granted` event (which calls `.withThreadContext(threadContext)` at build time, so it carries `project.id`), and a separate `security_config_change` event built via the private `securityChangeLogEntryBuilder(...)` factory, which does *not* call `withThreadContext`. As a result, every `security_config_change` audit event leaves the JVM without a `project.id` attribute.

The test suite is consistent with this: `LoggingAuditTrailTests` invokes its `projectId(threadContext, checkedFields)` assertion helper from the non-security-change tests (`testAccessGranted`, `testAuthenticationFailed`, etc.) but not from any `testSecurityConfigChangeEventFormatting*` test.

We should do the following to make this robust:

1. **Open a bug ticket** for the missing `withThreadContext` call on the `security_config_change` path. Treating it as a bug (not a design choice) clarifies the intent: every audit event should carry the four thread-context-derived fields (`x_forwarded_for`, `opaque_id`, `trace.id`, `project.id`), and this path drops all four. The fix itself is small (add the call inside `securityChangeLogEntryBuilder(...)` or before each `.build()` on that chain), and `LoggingAuditTrailTests` should grow assertions on the security-change paths; and
2. **Make `withThreadContext` implicit in `build()`.** Once proposal 1 lands, every emit path is calling `withThreadContext` anyway. Folding it into the builder turns a convention into an invariant: future contributors who add new emit paths get the four fields automatically, and the doc's chokepoint claim becomes structural rather than empirical.

(Most of the work here is alignment, not code: agreeing that the missing call on the `security_config_change` path is a bug rather than expected behavior, and that the structural change is the right way to prevent the gap from recurring.)

Cross-Project Search has a separate R3 question (§4.4).

### 4.4 R3 (CPS): `project.id` direction for Cross-Project Search

When a CPS query in project A reaches into project B, audit events on project B's side carry project B's `project.id`, not project A's. The mechanism is `AbstractProjectResolver.executeOnProject(...)` for the same-cluster case, and header strip-and-reset at the cluster boundary (`ThreadContext.getRequestHeadersToCopy(...)` removes `X_ELASTIC_PROJECT_ID_HTTP_HEADER` for `REMOTE_CLUSTER`) for the cross-cluster case.

Empirically, the Audit TDD's "Audit Logging x CPS: Current State" section reports that a single ESQL CPS query against `*:my-index` produced 5 audit events in a Serverless QA test: 2 from the local (project A) cluster and 3 from the remote (project B) cluster, all sharing the same `request.id`, with varying `origin.type` values (`rest`, `local_node`, `transport`). So this isn't a one-event-per-query question; it's a fan-out where each side's events would carry its own `project.id` under today's mechanism.

That matches the gateway's intent if each event should be delivered to the project that *owns the data touched*; it doesn't if events should follow the *originator*. A design call with the gateway team is needed. Implementation is mechanical once decided: leave as-is, or capture an `originating_project.id` separately. For the cross-cluster case, propagate it explicitly, since `X_ELASTIC_PROJECT_ID_HTTP_HEADER` is intentionally stripped at the boundary.

### 4.5 R4: mTLS to the gateway

The PoC uses HTTP without TLS. In production, the client certificate is distributed by Control-Plane (Vault → cert-manager → secret mount). We can reuse the existing pattern of `SSLService` + `PemKeyConfig` + `SSLConfigurationReloader` (used today by the monitoring HTTP exporter and Watcher's HTTP client). One open question is whether `OtlpHttpLogRecordExporterBuilder` in our OTel-Java version exposes client TLS config directly, or whether we have to wrap the underlying HTTP client.

### 4.6 R6: Strip cluster/node fields on serverless

`LoggingAuditTrail.EntryCommonFields` populates `cluster.name`, `cluster.uuid`, `node.name`, and `node.id` into the audit `StringMapMessage`, but only when the corresponding `EMIT_*` setting is `true`. The defaults are `node.id=true`, `cluster.uuid=true`, `node.name=false`, `cluster.name=false`. With `setCaptureMapMessageAttributes(true)` on the `OpenTelemetryAppender`, whatever ends up in the `StringMapMessage` flows out via OTel.

Three implementation options:

- (a) Set the existing per-field `xpack.security.audit.logfile.emit_*` settings to `false` on serverless (the two relevant defaults are `emit_node_id=true` and `emit_cluster_uuid=true`). The settings already gate the `commonFields` puts at the source; this suppresses the fields for both the file appender and the OTel appender. They're already `Property.Dynamic`, so there's no restart concern.
- (b) Branch in `LoggingAuditTrail` to skip the `commonFields.put(...)` calls for these fields when running serverless. More invasive than (a), but localizes the policy to ES code rather than serverless config.
- (c) Apply an attribute filter that drops these keys on the OTel emit path, implemented either as a custom `LogRecordProcessor` or as part of the §5.1 custom appender. The file appender continues to see the full set on hosted/self-hosted.

Option (a) is the lightest. The existing settings exist precisely for this purpose, and the serverless config can already override them.

### 4.7 R7: Stdout fallback on exhausted retries

Today the `BatchLogRecordProcessor` drops on queue overflow with no fallback. Production needs to catch exporter failure after retries, write the record to stdout in a recognizable format, and ensure the internal observability pipeline collects it. This is one half of the at-least-once story (R11); the other half is the platform-side "replay" service (Gateway TDD §"Unavailability issues mitigation" point 3), which is outside ES.

### 4.8 R8: Per-project ID filter

The PoC has no per-project gating; the only per-project field on the record today is `project.id` itself (R3). Production needs (a) an appender-path filter that drops events whose `project.id` is not in the enabled set, and (b) a config source for the enabled set. Per the Gateway TDD §"Conditionally enabling per-tenant log delivery in ECP services / applications", the mechanism is per-project file-based settings rendered by the `elasticsearch-controller`, which ES then reads. This is coupled to R12: the filter has to react to those settings changing without a restart.

### 4.9 R12: Configuration without cluster restart

The PoC reads `telemetry.otel.logs.enabled` once in `OtelSdkExportLogsSupplier.install()` at startup; toggling it requires a restart. R8's per-project state would have the same problem if implemented today. Production needs a settings/state listener that re-installs or reconfigures the appender path on change. Cluster-level dynamic on/off for `xpack.security.audit.enabled` is being addressed separately by Audit TDD Req 1 / [PR 147333](https://github.com/elastic/elasticsearch/pull/147333), but that's a different setting and doesn't cover the OTel-logs path.

### 4.10 R13: Tuned retry policy and buffer size

Default `BatchLogRecordProcessor` settings are in place. Explicit "retry up to 2 minutes / buffer 30–50 MB" targets aren't configured. Configuring them is cheap once the right knobs are picked.

### 4.11 `request.body` PII story

The choice between default-off, redaction, and sampling needs security review before this field can leave the cluster. Tracked here rather than under R2 because the decision is about *whether* to ship the field at all, not how to map it.

### 4.12 Integration test against the real gateway

The PoC's IT uses an in-process recording server, and that test still serves its purpose: it pins down the ES-side behavior without a cross-component dependency. What's missing is an additional IT that runs against the real `otel-delivery-gateway` component, to validate the contract between ES and the gateway end-to-end.

## 5. Decision points

Tradeoffs the team should weigh before further implementation work begins.

### 5.1 Attribute-key shape

The upstream `OpenTelemetryAppender` library hardcodes a `log4j.map_message.` prefix on every attribute that comes from a `StringMapMessage`. Today's PoC records carry `log4j.map_message.event.action` etc. instead of bare `event.action`. The integration test asserts on the prefixed names with a comment noting it's PoC-only; production needs bare keys.

| Option | Pros | Cons |
|---|---|---|
| **Custom log4j appender (~50 LOC)** | Narrow scope. No incubator/extension deps. Aligned with where v3 of the upstream library is going (prefix off by default). Easy to extend with semconv translation later. | We own a small log4j component; reviewers may ask "why not use upstream?" |
| **`v3_preview` wrapper (~15 LOC)** | Uses upstream library as designed. Smallest delta. | Pulls in `opentelemetry-api-incubator` (marked unstable) and `opentelemetry-sdk-extension-declarative-config`. Enables *all* v3-preview-gated behavior in the OTel logs path, not just the prefix. |
| **Switch the apm module to `AutoConfiguredOpenTelemetrySdk`** | Aligns ES with OTel's standard config story; system properties / YAML drive SDK behavior. Worth doing for reasons beyond this feature. | Substantial refactor; affects metrics and traces too. Bigger conversation than this epic. |
| **`LogRecordProcessor` post-hoc rename (~30 LOC)** | Keeps upstream appender. Localized change. | Smell: prefix is added by one stage and stripped by the next. Doesn't solve semconv translation, only the prefix. |

### 5.2 Field mapping shape: semconv vs ECS vs custom

The Gateway TDD asks emitting services to use OTel semconv where it exists, ECS where it doesn't, custom names as last resort. Most ES audit fields have a clean answer; some are genuinely ambiguous. The decision affects how downstream consumers query and join audit data.

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

### 5.3 Where the SDK setup lives

`OtelSdkExportLogsSupplier` lives in `modules/apm/`, alongside the meter/tracer suppliers. The apm module's stated purpose is "ES's own observability" (metrics + traces back to Elastic). Customer-facing audit-log delivery is conceptually different; it ships data the customer cares about, not telemetry about ES itself.

Options:
- Keep it in `modules/apm/`. Cheap, consistent with the metrics/tracer pattern, tolerable in the short term.
- Carve out a new sibling module (e.g. `modules/customer-telemetry/`). Cleaner conceptual boundary; adds a module.

This is worth deciding before the audit-log path grows further.

### 5.4 `LoggingAuditTrail` constructor signature for `project.id`

The PoC reads `X-Elastic-Project-Id` from `ThreadContext` directly inside `withThreadContext()`, which works because the header is always populated by the time audit events fire. Two alternatives are worth weighing:

- Inject `ProjectResolver` into the `LoggingAuditTrail` constructor. `ProjectResolver` is the canonical accessor and handles edge cases (cross-cluster, internal actions with no project) more gracefully.
- Use the `CustomAuditLoggingMetadataProvider` extension point proposed in Audit TDD Req 7. That keeps the OSS `LoggingAuditTrail` unchanged and lets the Serverless module inject `project.id` (and any future per-deployment metadata) via the existing extension pattern, similar to how `CustomAuthenticator` is used by UIAM. Worth coordinating with the audit TDD owner.

Not urgent; the header path is correct for the common case.

## 6. Next steps

Concrete items pulled from §4 and §5, split by what kind of action each needs.

### 6.1 Decisions and discussions

1. **Attribute-key shape (§5.1).** Pick one of: custom log4j appender, `v3_preview` wrapper, refactor to `AutoConfiguredOpenTelemetrySdk`, or `LogRecordProcessor` post-hoc rename. Gates 6.2.8 and 6.2.9.
2. **Per-field semconv/ECS/custom mapping (§5.2).** Resolve the "Hard" rows: `apikey.*` namespace, `indices` array, `security_config_change` nested blobs (flatten vs opaque-JSON-string).
3. **CPS routing direction (§4.4).** Design call with the gateway team: when a CPS query in project A reaches project B, should the audit event be delivered to A (originator) or B (data-owner)?
4. **R6 strip-fields placement (§4.6).** Use the existing `xpack.security.audit.logfile.emit_*` settings on serverless vs branch in `LoggingAuditTrail.EntryCommonFields` vs attribute filter on the OTel emit path. Doc leans toward the existing-settings option.
5. **Where the SDK setup lives (§5.3).** Keep in `modules/apm/` or carve out a new `modules/customer-telemetry/`. Worth deciding before more audit-log code accumulates here.
6. **`request.body` PII story (§4.11).** Default-off vs redaction vs sampling. Needs security review before the field can leave the cluster.
7. **gRPC vs HTTP (§2.5).** Confirm with the gateway team that OTLP/HTTP-protobuf is acceptable; the gateway TDD parenthetically says gRPC.
8. **`LoggingAuditTrail` constructor (§5.4).** Inject `ProjectResolver` vs use the Audit TDD's `CustomAuditLoggingMetadataProvider` extension point vs keep the direct `ThreadContext` read. Not urgent.

### 6.2 Implementation work

1. **R3 chokepoint gap for `security_config_change` (§4.3).** File a bug ticket for the missing `withThreadContext` on this path; once fixed, fold the `withThreadContext` call into `LogEntryBuilder.build()` so the chokepoint becomes structural. Most of the cost is alignment (agreement that the missing call is a bug, and that the structural change is desirable); the code changes themselves are small.
2. **R4 mTLS to the gateway (§4.5).** Reuse `SSLService` + `PemKeyConfig` + `SSLConfigurationReloader`. Open sub-question: does our OTel-Java version's `OtlpHttpLogRecordExporterBuilder` expose client TLS directly, or do we wrap the underlying HTTP client?
3. **R6 strip cluster/node fields on serverless (§4.6).** Implementation follows 6.1.4. If option (a) is chosen, this is a serverless-config change with no ES code change.
4. **R7 stdout fallback on exhausted retries (§4.7).** Catch exporter failure, write the record to stdout in a recognizable format. Half of the at-least-once story (R11).
5. **R8 per-project filter (§4.8).** Appender-path filter plus a config source for the enabled set, fed by per-project file-based settings rendered by the `elasticsearch-controller`. Coupled to 6.2.6.
6. **R12 dynamic config without restart (§4.9).** Settings/state listener that re-installs or reconfigures the appender path on change. Covers `telemetry.otel.logs.enabled` and the per-project state from 6.2.5.
7. **R13 retry/buffer tuning (§4.10).** Configure `BatchLogRecordProcessor` once the targets land (~2 min retry, ~30–50 MB buffer).
8. **Bare semconv keys on the wire (§4.1).** Implementation follows 6.1.1.
9. **Per-field translation (§4.2).** Implementation follows 6.1.1 and 6.1.2.
10. **Integration test against the real gateway (§4.12).** Add an IT that runs against the real `otel-delivery-gateway` component, alongside (not replacing) the existing in-process recording-server IT.

## Appendix A: Options considered and rejected

Paths the PoC explored before evidence pushed us elsewhere.

### A.1 Architecture B: write to file, sidecar tails it

Keep the existing rolling-file audit appender, deploy a sidecar OTel collector with a `filelog` receiver to tail the file and forward via OTLP. It is operationally appealing because it survives ES-process death; the launcher can drain remaining file content like a heap dump, which is arguably the safer-by-default behavior for an audit pipeline.

We rejected this approach because the team that owns the gateway has scoped their pipeline assuming OTLP ingress from the application (see Gateway TDD §"OTel log delivery pipeline on ECP"). Architecture A is what the Jira description and the gateway TDD assume. If the gateway team changes their mind, B becomes attractive again, but that's a cross-team conversation.

### A.2 Declaring `OpenTelemetryAppender` in `log4j2.properties`

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

### A.3 Putting the OTel deps in `:server`

We tried this as a fix for A.2: with the appender JAR on `:server`'s classpath, log4j can resolve it at boot. ES then failed with **jar hell**: `opentelemetry-context` is also pulled transitively by `:x-pack:plugin:esql-datasource-gcs` at a different version, and ES rejects cross-classpath duplicate classes at plugin-load time. We unwound the `:server` change and kept everything in `modules/apm/`, going with the programmatic-attach path instead.

### A.4 Direct OTel SDK call from `LoggingAuditTrail`

Rather than going through log4j at all, have `LoggingAuditTrail.LogEntryBuilder` emit OTLP records directly via `OpenTelemetry.getLogsBridge()`. We considered this approach: it is the most controlled shape and bypasses the whole appender library. We didn't pursue it because the goal is for an appender (the natural log4j extension point) to handle the conversion. The custom-appender option in §5.1 captures the right shape without modifying `LoggingAuditTrail`.

## Appendix B: Findings worth knowing

- **`manage_threads` entitlement** is required for `io.opentelemetry.sdk.logs` because `BatchLogRecordProcessor` spawns a worker thread. First test run failed with `NotEntitledException` until added to `entitlement-policy.yaml`.
- **Hand-wired OTel SDK and declarative config don't compose easily.** ES uses `OpenTelemetrySdk.builder()` (the bare SDK builder), which has no public way to set a `ConfigProvider`. As a result, instrumentation-config flags can't be set through the public SDK API. The most relevant example is `otel.instrumentation.common.v3-preview`, the upstream library's preview knob for dropping the `log4j.map_message.` prefix. They're settable via `AutoConfiguredOpenTelemetrySdk.builder().addPropertiesSupplier(...)` (auto-configure module) or by implementing `ExtendedOpenTelemetry` ourselves with a `YamlDeclarativeConfigProperties.create(Map, ComponentLoader)` (incubator + declarative-config extension modules). This is a real OTel-Java friction point that affects more than just this feature. The OTel configuration spec itself requires SDKs to expose programmatic config; the Java SDK does, but only outside the bare builder.
- **The `OpenTelemetryAppender.install(sdk)` static method is order-dependent.** It pushes the SDK to *already-registered* `OpenTelemetryAppender` instances. Calling it before the appender is in the log4j config is a silent no-op for that appender. Using `Builder.setOpenTelemetry(sdk)` directly is more robust.
- **Audit events use `StringMapMessage`, which is structured but bodyless.** The OTel appender library treats this as "no `LogRecord.body`, capture entries as attributes." With `setCaptureMapMessageAttributes(true)` this works but adds the prefix discussed in §5.1.
