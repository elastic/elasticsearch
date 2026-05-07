# OTel audit log delivery — POC writeup for ES-14356

**Author:** Patrick Doyle
**Status:** Draft for review

## 1. Goal

Make Elasticsearch audit logs deliverable to serverless customers via OTLP. Today, audit logs are written to a per-cluster JSON file by `LoggingAuditTrail`; in self-hosted deployments, customers ship that file with Filebeat. Serverless has no Filebeat sidecar and no shared disk, so the file-based path doesn't apply. The cross-team plan is for emitting services (ES, Kibana, Control-Plane services) to send OTLP log records over the network to a per-cluster `otel-delivery-gateway`, which routes them to the customer's chosen destination project.

This POC validates that ES can emit OTLP audit logs end-to-end and surfaces the work that remains, so we can plan the real implementation deliberately.

### Related material

- [ES-14356](https://elasticco.atlassian.net/browse/ES-14356) — parent epic
- [ES-13255](https://elasticco.atlassian.net/browse/ES-13255) — broader "ES logs over OTel" direction
- [TDD: Shipping Logs to Elastic Cloud customers](https://docs.google.com/document/d/12IZxcX5uoFWvIhfff1I3DwRVMt8ab6tNkaIKs-Fx8Y8/edit)
- [elasticsearch-team#2170](https://github.com/elastic/elasticsearch-team/issues/2170) — Tim Vernum's brain-dump of "audit logs in serverless" work areas

## 2. What's working

End-to-end flow proven: `LoggingAuditTrail` → log4j → `OpenTelemetryAppender` (programmatically attached) → in-process `SdkLoggerProvider` with `BatchLogRecordProcessor` and `OtlpHttpLogRecordExporter` → `RecordingApmServer` test fixture, which parses the OTLP request and exposes received `LogRecord`s for assertion.

### Code changes

| Where | Change |
|---|---|
| `modules/apm/build.gradle` | Promoted `opentelemetry-sdk-logs` to `implementation`; added `opentelemetry-log4j-appender-2.17`. |
| `modules/apm/.../export/otelsdk/OtelSdkSettings.java` | `telemetry.otel.logs.enabled`, `telemetry.otel.logs.endpoint`. |
| `modules/apm/.../export/otelsdk/OtelSdkExportLogsSupplier.java` (new) | Builds the SDK and programmatically attaches an `OpenTelemetryAppender` to the audit logger's `LoggerConfig`. Idempotent. Exposes `forceFlush()`. |
| `modules/apm/.../APM.java` | `createComponents` constructs the supplier, calls `install()`, registers the supplier in `APMTelemetryProvider`. |
| `modules/apm/src/main/plugin-metadata/entitlement-policy.yaml` | `manage_threads` entitlement for `io.opentelemetry.sdk.logs` (the batch processor spawns a worker thread). |
| `server/.../telemetry/TelemetryProvider.java` | `attemptFlushLogs()` (default no-op). Symmetric with the existing flush methods for metrics and traces. Useful both for tests and graceful shutdown. |
| `modules/apm/.../APMTelemetryProvider.java` | Implements `attemptFlushLogs()` by delegating to the supplier. |
| `x-pack/plugin/security/.../LoggingAuditTrail.java` | New `PROJECT_ID_FIELD_NAME` constant; one extra `setThreadContextField(...)` line in `LogEntryBuilder.withThreadContext(...)` reading from `Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER`. |
| `x-pack/plugin/core/.../log4j2.properties` | Added `project.id` to the audit JSON pattern. (No OTel appender is declared here; see Appendix A.) |
| `build-tools/.../testclusters/MockApmServer.java` | New `/v1/logs` handler. |
| `test/external-modules/apm-integration/.../{ReceivedTelemetry,OtlpLogsParser,RecordingApmServer}.java` | `ReceivedLog` record, OTLP parser, `/v1/logs` route on the recording server. |
| `test/external-modules/apm-integration/.../OtelAuditLogsIT.java` (new) | Boots a security-enabled cluster with audit + OTel logs, hits `/_security/_authenticate`, calls `/_flush_telemetry`, asserts a `ReceivedLog` arrives. |
| `test/external-modules/apm-integration/.../FlushTelemetryRestHandler.java` | Calls `attemptFlushLogs()` so `/_flush_telemetry` flushes all three signals. |
| `gradle/verification-metadata.xml` | Auto-regenerated for the new artifact. |

### Tests passing

- `:modules:apm:test` — `OtelSdkExportLogsSupplierTests`: 7/7
- `:x-pack:plugin:security:test` — `LoggingAuditTrailTests`: 35/35 (covers the new `project.id` field)
- `:test:external-modules:test-apm-integration:javaRestTest` — `OtelAuditLogsIT`: end-to-end OTLP delivery
- `:modules:apm:thirdPartyAudit`: clean
- Manual `./gradlew run`: ES boots cleanly with audit + OTel logs settings; supplier emits `OTel SDK logs export installed`; node reaches `started`.

## 3. How this fits with the rest of ES's logging

This is an **additive** OTLP path; it does not replace anything.

- `LoggingAuditTrail` continues to emit a single `StringMapMessage` per audit event via `logger.info(AUDIT_MARKER, logEntry)`. log4j routes that event to its appenders.
- The existing `audit_rolling` file appender (`<cluster>_audit.json`) is unchanged. Customers who already consume the file with Filebeat keep doing so.
- The OTel appender is attached to the same audit logger by `OtelSdkExportLogsSupplier` at startup, after plugin classloaders are available. Audit events flow to both sinks.
- The OTel appender path is **non-blocking on the calling thread**. `OpenTelemetryAppender.append` does the log4j→OTel mapping (CPU only) and hands the record to a `BatchLogRecordProcessor`, which `offer`s it onto a bounded in-memory queue (default size 2048) and returns. The actual OTLP HTTP POST runs on a worker thread the processor owns. The queue applies **no back-pressure**: if the worker thread can't keep up — because the gateway is slow or unreachable — records are dropped rather than blocking the caller. This is what the "audit logging must not block transport threads" requirement cares about. Per the OTel logs SDK spec: `LogRecordProcessor.OnEmit` SHOULD NOT block, and overflow drops rather than back-pressures. (The existing `audit_rolling` file appender, by contrast, *is* synchronous; making that non-blocking is out of scope here.)
- Other ES log streams (server, deprecation, slowlog, ESQL, etc.) are **not** on this path. The Jira description and Ryan's stated scope keep this PoC narrowly to audit. Broader "ES logs via OTel" is tracked in ES-13255.
- The OTel SDK lives in `modules/apm/`, the same module that already wires up metrics and traces SDKs in the same hand-wired style.

## 4. What remains

This section is split into **decision points** (where there are real tradeoffs the team should weigh) and **known work** (items we know need doing once a decision is made).

### 4.1 Decision points

#### 4.1.1 Attribute-key shape

The upstream `OpenTelemetryAppender` library hardcodes a `log4j.map_message.` prefix on every attribute that comes from a `StringMapMessage`. So today the PoC's records carry `log4j.map_message.event.action` etc. instead of bare `event.action`. The integration test asserts on the prefixed names with a comment noting it's PoC-only; production needs bare keys.

| Option | Pros | Cons |
|---|---|---|
| **Custom log4j appender (~50 LOC)** | Narrow scope. No incubator/extension deps. Aligned with where v3 of the upstream library is going (prefix off by default). Easy to extend with semconv translation later. | We own a small log4j component; reviewers may ask "why not use upstream?" |
| **`v3_preview` wrapper (~15 LOC)** | Uses upstream library as designed. Smallest delta. | Pulls in `opentelemetry-api-incubator` (marked unstable) and `opentelemetry-sdk-extension-declarative-config`. Enables *all* v3-preview-gated behavior in OTel logs path, not just the prefix. |
| **Switch the apm module to `AutoConfiguredOpenTelemetrySdk`** | Aligns ES with OTel's standard config story; system properties / YAML drive SDK behavior. Worth doing for reasons beyond this feature. | Substantial refactor; affects metrics and traces too. Bigger conversation than this epic. |
| **`LogRecordProcessor` post-hoc rename (~30 LOC)** | Keeps upstream appender. Localised change. | Smell: the prefix is added by one stage and stripped by the next. Doesn't solve semconv translation, only the prefix. |

#### 4.1.2 Field mapping shape: semconv vs ECS vs custom

The TDD asks emitting services to use OTel semconv where it exists, ECS where it doesn't, and custom names as last resort. Most ES audit fields have a clean answer; some are genuinely ambiguous. The decision affects how downstream consumers query and join audit data.

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

Today `OtelSdkExportLogsSupplier` lives in `modules/apm/`, alongside the meter/tracer suppliers. The apm module's stated purpose is "ES's own observability" (metrics + traces back to Elastic). Customer-facing audit log delivery is conceptually different — it ships data the customer cares about, not telemetry about ES itself.

Options:

- Keep it in `modules/apm/`. Cheap, consistent with the metrics/tracer pattern, tolerable in the short term.
- Carve out a new sibling module (e.g. `modules/customer-telemetry/`). Cleaner conceptual boundary; adds a module.

Worth deciding before the audit-log path grows further.

#### 4.1.4 `LoggingAuditTrail` constructor signature for `project.id`

The PoC reads `X-Elastic-Project-Id` from `ThreadContext` directly inside `withThreadContext()`, which works because the header is always populated by the time audit events fire. A more orthodox approach injects `ProjectResolver` into the `LoggingAuditTrail` constructor. The `ProjectResolver` is the canonical accessor and handles edge cases (cross-cluster, internal actions with no project) more gracefully.

Not urgent — the header path is correct for the common case — but it's the kind of thing a code reviewer will ask about.

### 4.2 Known work

These don't need a design decision; they need engineering effort.

- **Field mapping implementation.** Pick the §4.1.1 path, then translate audit fields to whichever shape §4.1.2 settles on, with tests across all 13 audit event types.
- **`request.body` PII story.** Either omit by default (simplest), redact, or sample. Needs security review.
- **mTLS to the gateway.** Production requires mTLS, with client certs distributed by Control-Plane (likely a Kubernetes secret mounted at a known path). Reuse pattern: `SSLService` + `PemKeyConfig` + `SSLConfigurationReloader` (used by the monitoring HTTP exporter and Watcher's HTTP client). Need to verify whether `OtlpHttpLogRecordExporter`'s builder exposes client TLS config in our OTel version, or whether we wrap its underlying HTTP client.
- **Field stripping for serverless.** Cluster name, node ID, host name — fields the gateway TDD says shouldn't surface in customer-facing audit logs. Confirmed (in the cross-team thread Val Crettaz led) that this happens at the source, not at the gateway.
- **Internal-action filtering.** Out of scope for this epic; Ankit Sethi is the owner. Some events (cluster-internal traffic, system-user actions, on-call/SRE operations) shouldn't reach customers. Tracked under elasticsearch-team#2170.
- **gRPC transport.** TDD specifies OTLP/gRPC; PoC uses OTLP/HTTP because the in-repo example (`OTLPLogsIndexingRestIT`) uses HTTP and the dep footprint is smaller. Migrating is later, modest scope.
- **Production-side integration test.** The PoC's IT uses HTTP and an in-process recording server. A test against a real `otel-delivery-gateway` (or a sufficiently faithful mock) would validate the wire-level interaction.

## Appendix A — Options considered and rejected

These are paths the PoC explored, where evidence pushed us elsewhere. Captured here so we don't relitigate.

### A.1 Architecture B: write to file, sidecar tails it

Keep the existing rolling-file audit appender, deploy a sidecar OTel collector with a `filelog` receiver to tail the file and forward via OTLP. Operationally appealing — survives ES-process death since the launcher can drain remaining file content like a heap dump. Discussed with Ryan Ernst, who agreed this is the safer strategy in principle.

Rejected because: the team that owns the gateway has scoped their pipeline assuming OTLP ingress from the application. Architecture A is what the Jira description and the gateway TDD assume. If the gateway team changes their mind, B becomes attractive again, but that's a cross-team conversation.

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

## Appendix B — Findings worth knowing about

Things that surprised us; worth knowing for the implementation.

- **`manage_threads` entitlement** is required for `io.opentelemetry.sdk.logs` because `BatchLogRecordProcessor` spawns a worker thread. First test run failed with `NotEntitledException` until added to `entitlement-policy.yaml`. The kind of finding that's a 30-minute mystery in a real PR cycle if you don't expect it.
- **`gradle/verification-metadata.xml` regen** is required for any new third-party artifact. `./gradlew --write-verification-metadata sha256 ...` does it; takes ~2 minutes and produces a sizeable diff.
- **Hand-wired OTel SDK and declarative config don't compose easily.** ES uses `OpenTelemetrySdk.builder()` (the bare SDK builder), which has no public way to set a `ConfigProvider`. As a result, instrumentation-config flags like `otel.instrumentation.common.v3-preview` — the upstream library's preview knob for dropping the `log4j.map_message.` prefix — can't be set through the public SDK API. They're settable via `AutoConfiguredOpenTelemetrySdk.builder().addPropertiesSupplier(...)` (auto-configure module) or by implementing `ExtendedOpenTelemetry` ourselves with a `YamlDeclarativeConfigProperties.create(Map, ComponentLoader)` (incubator + declarative-config extension modules). This is a real OTel-Java friction point that affects more than just this feature. The OTel configuration spec itself requires SDKs to expose programmatic config; the Java SDK does, but only outside the bare builder.
- **The `OpenTelemetryAppender.install(sdk)` static method is order-dependent.** It pushes the SDK to *already-registered* `OpenTelemetryAppender` instances. Calling it before the appender is in the log4j config is a silent no-op for that appender. Using `Builder.setOpenTelemetry(sdk)` directly is more robust.
- **Audit events use `StringMapMessage`, which is structured but bodyless.** The OTel appender library treats this as "no `LogRecord.body`, capture entries as attributes." With `setCaptureMapMessageAttributes(true)` this works but adds the prefix in §4.1.1.
