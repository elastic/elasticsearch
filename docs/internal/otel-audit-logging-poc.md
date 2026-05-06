# OTel audit log delivery — POC for ES-14356

**Author:** Patrick Doyle
**Status:** POC writeup, draft
**Related:** [ES-14356](https://elasticco.atlassian.net/browse/ES-14356), [ES-13255](https://elasticco.atlassian.net/browse/ES-13255), [elasticsearch-team#2170](https://github.com/elastic/elasticsearch-team/issues/2170), [TDD: Shipping Logs to Elastic Cloud customers](https://docs.google.com/document/d/12IZxcX5uoFWvIhfff1I3DwRVMt8ab6tNkaIKs-Fx8Y8/edit?tab=t.0)

This is a proof-of-concept, not a prototype. The goal is to surface every part of the work that is non-obvious or expensive, so we can produce a credible estimate. Where the POC stops short of working code, the section ends with a t-shirt size and reasoning.

---

## 1. Summary

Architecture A (in-process OTel SDK shipping OTLP to a gateway) is feasible with the existing OTel SDK already pulled in by the `modules/apm/` module. The minimal "audit events leave ES via OTLP" path is small: a few hundred lines plus build-system updates. The expensive parts are not the wiring — they are (a) field mapping to OTel semconv, (b) mTLS to the gateway, and (c) emitting `project.id` for multi-project routing. None of these are research-grade unknowns; they are scoping unknowns.

Headline estimate to land production-ready audit-log delivery for serverless: **~5–6 engineering weeks** of ES core/infra work, with a separate work track owned by ES Security for in-plugin filtering / suppression of internal actions.

## 2. Architectural choice

We picked **architecture A**: ES emits OTLP from the JVM via the OTel SDK to a per-cluster `otel-delivery-gateway`. The alternative, **architecture B**, would be to keep writing the existing `*_audit.json` rolling file and have a sidecar OTel collector with a `filelog` receiver tail it. Architecture B is operationally appealing — Ryan Ernst noted in private discussion that it survives ES-process death because the launcher can drain remaining file content the way it does for heap dumps — but the team that owns the gateway has scoped their pipeline assuming OTLP ingress from the application. Architecture A is what the Jira description and the gateway TDD assume; the POC validates that.

Once architecture A has a credible estimate, the team can decide whether to revisit B.

## 3. What this POC built (working code)

All code lives in the apm module unless noted.

| File | Change |
|---|---|
| `build-tools/src/main/java/org/elasticsearch/gradle/testclusters/MockApmServer.java` | Added `OtlpLogsHandler` for `/v1/logs` so the existing test fixture can receive OTLP log records (mirrors `OtlpMetricsHandler`). |
| `modules/apm/build.gradle` | Promoted `opentelemetry-sdk-logs` from `runtimeOnly` to `implementation`; added `io.opentelemetry.instrumentation:opentelemetry-log4j-appender-2.17`. |
| `modules/apm/.../export/otelsdk/OtelSdkSettings.java` | Added `telemetry.otel.logs.enabled` (boolean, default false) and `telemetry.otel.logs.endpoint` (string). |
| `modules/apm/.../export/otelsdk/OtelSdkExportLogsSupplier.java` | New. Builds an `OpenTelemetrySdk` with `SdkLoggerProvider` + `BatchLogRecordProcessor` + `OtlpHttpLogRecordExporter`, then **programmatically** attaches an `OpenTelemetryAppender` to the audit logger via the log4j `Configuration` API (see §6.1). Idempotent. |
| `modules/apm/.../APM.java` | `createComponents` constructs the supplier, calls `install()`, and registers it as a returned component so its lifecycle ends with the plugin's. |
| `modules/apm/.../OtelSdkExportLogsSupplierTests.java` | Unit tests covering disabled-is-noop, missing-endpoint-throws, idempotent install, double-close, plus a direct SDK→exporter end-to-end via `InMemoryLogRecordExporter`. 7 tests, all passing. |
| `modules/apm/src/main/plugin-metadata/entitlement-policy.yaml` | Added `manage_threads` entitlement for the `io.opentelemetry.sdk.logs` module — required because `BatchLogRecordProcessor` spawns a worker thread (see §6.5). |
| `x-pack/plugin/core/src/main/config/log4j2.properties` | Added a `project.id` entry to the audit JSON pattern. (No `audit_otel` appender is declared here — see §6.1; that appender is attached programmatically.) |
| `x-pack/plugin/security/.../LoggingAuditTrail.java` | Added `PROJECT_ID_FIELD_NAME = "project.id"` constant and one `setThreadContextField(...)` line in `LogEntryBuilder.withThreadContext(...)` so audit events stamp the project id from the `X-Elastic-Project-Id` header (see §4.1). |
| `x-pack/plugin/security/.../LoggingAuditTrailTests.java` | `projectId(...)` helper; 31 call sites updated; randomised header injection in `setup()`. |
| `gradle/verification-metadata.xml` | Auto-regenerated to add SHA-256 entries for the new `opentelemetry-log4j-appender-2.17` artifact (and any transitive deps). |
| `test/external-modules/apm-integration/.../ReceivedTelemetry.java` | Added `ReceivedLog` record (timeUnixNano, severity, body, attributes, optional traceId). |
| `test/external-modules/apm-integration/.../OtlpLogsParser.java` | New. Parses OTLP `ExportLogsServiceRequest` into `ReceivedLog`. |
| `test/external-modules/apm-integration/.../RecordingApmServer.java` | Added `/v1/logs` route. |
| `test/external-modules/apm-integration/.../OtelAuditLogsIT.java` | New. End-to-end IT: boots a security-enabled cluster with audit + OTel logs pointed at the recording server, hits `/_security/_authenticate`, asserts a `ReceivedLog` arrives. |
| `test/external-modules/apm-integration/build.gradle` | `usesDefaultDistribution(...)` so the security-enabled test cluster gets x-pack. |
| `server/src/main/java/.../telemetry/TelemetryProvider.java` | Added `attemptFlushLogs()` (default no-op). Symmetric with the existing `attemptFlushMetrics()` / `attemptFlushTraces()`. Needed both for tests *and* graceful shutdown so audit events emitted just before stop aren't dropped. |
| `modules/apm/.../APMTelemetryProvider.java` | Implements `attemptFlushLogs()` by delegating to the supplier's new `forceFlush()` method. |
| `test/external-modules/apm-integration/.../FlushTelemetryRestHandler.java` | Calls `attemptFlushLogs()` so `/_flush_telemetry` flushes all three signal types. |

### Verified by build

- `:modules:apm:compileJava` / `:compileTestJava` — clean.
- `:modules:apm:test --tests *OtelSdkExportLogsSupplierTests*` — 7/7 passing.
- `:modules:apm:thirdPartyAudit` — clean.
- `:x-pack:plugin:core:processResources :compileJava` — clean.
- `:x-pack:plugin:security:test --tests *LoggingAuditTrailTests*` — 35/35 passing (covers the new `project.id` field).
- `./gradlew run` end-to-end — ES boots cleanly with `xpack.security.audit.enabled=true` and `telemetry.otel.logs.enabled=true`; the supplier emits `OTel SDK logs export installed`, log4j parses the audit logger config without errors, ES reaches `[o.e.n.Node] started`.
- **`:test:external-modules:test-apm-integration:javaRestTest --tests *OtelAuditLogsIT*` — passing.** Boots a security-enabled cluster, hits `/_security/_authenticate`, calls `/_flush_telemetry`, asserts a `ReceivedLog` arrives at `RecordingApmServer` with audit attributes (`event.action`, `event.type`, `user.name`, `project.id`, etc.) carried as OTLP attributes. This is the load-bearing end-to-end validation: log4j → `OpenTelemetryAppender` → `SdkLoggerProvider` → `OtlpHttpLogRecordExporter` → recording server.

## 4. What this POC investigated but did not build

These are the items where doing the work would have eaten the whole day. The investigations below produce the estimate inputs.

### 4.1 `project.id` for multi-project routing — *implemented*

The gateway routes each `LogRecord` based on a `project.id` resource/attribute. ES already carries the project context per request via the `X-Elastic-Project-Id` HTTP/transport header, materialised in `ThreadContext` as `Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER`.

`LoggingAuditTrail` already holds a `ThreadContext` and already extracts other request headers in `LogEntryBuilder.withThreadContext(...)`. Adding `project.id` was one extra `setThreadContextField(...)` call at the same site, plus a `PROJECT_ID_FIELD_NAME` constant, plus a `%map{project.id}` entry in the audit JSON pattern in `log4j2.properties`. No constructor change to `LoggingAuditTrail`, no `ProjectResolver` injection, no Security.java change — the header is already in `ThreadContext` by the time audit fires.

Tests: a header-randomisation line was added next to the existing `X_OPAQUE_ID` / `TRACE_ID` randomisation in `LoggingAuditTrailTests.setup()`, and a `projectId(...)` helper was added next to the existing `opaqueId(...)` / `traceId(...)` helpers. The 31 sites that call `opaqueId(...)` were updated to also call `projectId(...)`. All 35 LoggingAuditTrail tests pass.

**Actual cost: ~30 minutes** for the wiring + test plumbing. (Original estimate was 1–2 hours, which assumed constructor injection of `ProjectResolver`; the simpler header-extraction path is shorter.)

### 4.2 OTel semconv field mapping

The `audit_rolling` PatternLayout in `x-pack/plugin/core/src/main/config/log4j2.properties` (lines ~5–54) emits ~42 distinct field keys; values are constructed in `LogEntryBuilder` (around lines 1090+ in `LoggingAuditTrail.java`).

The TDD requires us to emit OTel semconv keys with ECS as a fallback. Most ES audit fields already have direct semconv equivalents (`event.action`, `user.name`, `http.request.method`); some sit cleanly in ECS only (`authentication.type`, `user.roles`); and a few have no good answer:

| Field | Difficulty | Notes |
|---|---|---|
| `timestamp` | Easy | String → `LogRecord.timeUnixNano` |
| `event.action`, `user.name`, `request.method` | Easy | Direct semconv match |
| `origin.address` | Easy-ish | Split into `client.address` (REST) vs `server.address` (transport) |
| `authentication.type`, `user.roles` | Medium | ECS-only; emit as custom attributes |
| `apikey.id`, `apikey.name` | Decision | No `apikey.*` namespace in semconv. Choose ECS-style or invent OTel-flavored nesting (`user.api_key.*`). |
| `indices` (array) | Decision | OTel doesn't have a multi-database convention; emit as JSON array attribute |
| `request.body` | Hard | Potentially large + PII; semconv discourages full bodies. Default-off today; need a redaction or sampling story before this can leave the cluster. |
| `trace.id` | Decision | Already W3C-shaped. Goes in `LogRecord.traceId` for native span correlation, or stays a flat attribute. The first option is right but requires parsing/validation. |
| `put` / `delete` / `change` / `create` / `invalidate` blobs | Hard | Nested arbitrary objects for `security_config_change` events. Decide between flatten-to-attributes vs opaque-JSON-string. Each approach has downstream querying implications. |

The implementation lives in either an alternate `LogEntryBuilder` selected by setting, or a sidecar emitter that publishes a parallel OTLP record. Either way the code shape is straightforward; the cost is decisions and test coverage across all 13 audit event types.

**T-shirt: medium (~3–4 weeks).** Dominated by: (a) cross-team alignment on the long-tail fields, (b) PII review for `request.body`, (c) tests across all event categories.

#### 4.2.1 Attribute-key shape — implementation options

The POC's IT currently asserts on prefixed keys (`log4j.map_message.event.action`) because that is what arrives today. The prefix is hardcoded by the upstream `opentelemetry-log4j-appender-2.17` library: `LogEventMapper` writes every `MapMessage` entry under `"log4j.map_message." + key`. Production needs bare keys (or semconv keys after §4.2 lands).

There is no off-the-shelf knob on the upstream library to drop the prefix today. The library has a `v3_preview` flag (read via `commonConfig.getBoolean("v3_preview", false)`) that does drop the prefix, but it is preview behavior, undocumented in the library README, and not currently enabled.

Four implementation paths, all viable, each with trade-offs:

| Path | Code volume | New deps | Scope of behavior change | Notes |
|---|---|---|---|---|
| Custom log4j appender | ~50 LOC | none | Only attribute key shape | Subclass `AbstractAppender`, talk directly to OTel SDK via `OpenTelemetry.getLogsBridge()`. Drops the `OpenTelemetryAppender` library dependency. Aligned with where v3 of the upstream library is going (prefix off by default). |
| `v3_preview` wrapper | ~15 LOC | `opentelemetry-sdk-extension-declarative-config`, `opentelemetry-api-incubator` | All v3-preview-gated changes in OTel logs path | Wrap our SDK as `ExtendedOpenTelemetry` whose `getInstrumentationConfig("common")` returns a `YamlDeclarativeConfigProperties.create(Map.of("v3_preview", true), loader)`. Uses upstream library as designed; relies on incubator (unstable) APIs. |
| Switch to `AutoConfiguredOpenTelemetrySdk` | non-trivial refactor of `OtelSdkExportLogsSupplier` (and probably the meter/tracer suppliers too, for consistency) | `opentelemetry-sdk-extension-autoconfigure` | All auto-configure-driven config; v3-preview flips via system property | The closest thing to "config-driven SDK setup" that ES doesn't currently use. Worth considering for the apm module overall, not just for this feature. |
| `LogRecordProcessor` rewriter | ~30 LOC | none | Strips `log4j.map_message.` prefix post-hoc; doesn't address semconv translation directly | Smell: prefixed keys are added by one stage and stripped by the next. Works but feels like cleanup-after-the-fact. |

Recommendation: pick within the §4.2 work item, not before. The custom appender is the lowest-risk path if the answer is "just get bare keys"; the AutoConfigured switch is worth a separate conversation about ES's overall OTel SDK setup.

A note on the OTel-Java config story: per the [OTel configuration spec](https://opentelemetry.io/docs/specs/otel/configuration/#programmatic), the SDK is required to expose a programmatic configuration interface. The Java implementation does — `YamlDeclarativeConfigProperties.create(Map, ComponentLoader)` — but it lives in a separate extension module, and the public `OpenTelemetrySdkBuilder` does not expose a `setConfigProvider(...)` method (it's package-private internally). So programmatic config is reachable, but not via the bare hand-wired SDK builder. This is friction worth knowing about beyond ES-14356.

### 4.3 mTLS to the otel-delivery-gateway

The TDD specifies mTLS, with client certs distributed by Control-Plane. The POC speaks plaintext OTLP/HTTP to a local mock — production is different.

**Prior art in ES:**
- `HttpExporter` (monitoring) at `x-pack/plugin/monitoring/.../HttpExporter.java` configures an `SSLIOSessionStrategy` from `xpack.monitoring.exporters.<name>.ssl.*` settings using ES's `SSLService` abstraction.
- Watcher's `HttpClient` does the same via `xpack.http.ssl.*`.
- Both use `PemKeyConfig` for filesystem-loaded cert/key material and `SSLConfigurationReloader` for hot-reload on rotation (cf. `x-pack/plugin/core/src/main/java/org/elasticsearch/xpack/core/ssl/SSLConfigurationReloader.java`).

**Gap:** The OTel exporter (`OtlpHttpLogRecordExporter.builder()`) needs client TLS material. Recent OTel versions expose `setClientTls(byte[] privateKeyPem, byte[] certificatePem)` and `setTrustedCertificates(byte[] certificatePem)` — needs verifying at the OTel version we're on. If the version shipped here doesn't expose these, the fallback is to wrap the exporter's HTTP client (Apache HttpClient or OkHttp) — adds an integration layer.

**Cert delivery from Control-Plane:** Almost certainly a Kubernetes secret mounted at a known path. ES would point at filesystem paths via new settings (e.g. `telemetry.otel.logs.client.cert`, `.client.key`, `.trusted_certificates`) and watch for rotation via `FileWatcher`. The mount path is a Control-Plane design decision, not engineering effort on our side.

**T-shirt: medium (~1 week).** Reuse of `SSLService` / `PemKeyConfig` / `SSLConfigurationReloader` is direct. Risk is whether the OTel HTTP client cooperates; if not, add ~2–3 days for a custom HTTP client adapter.

## 5. Out of scope (deferred or owned elsewhere)

- **In-app buffering / retry / spill-to-disk.** The principle here is that telemetry reliability is the telemetry infrastructure's job, not the application's. ES will use the OTel SDK's default `BatchLogRecordProcessor` settings; if that proves insufficient, the gateway/MOTel can absorb retries. Tracked separately for metrics in [ES-14439](https://elasticco.atlassian.net/browse/ES-14439); we want the same outcome here.
- **Internal-action filtering / suppression.** Tim Vernum's enumeration in elasticsearch-team#2170 calls out three classes of "internal" actions that should not be exposed to customers in serverless audit logs. Ankit Sethi (ES Security) is the owner of this work; we should not double up.
- **Customer-configured redaction.** Confirmed by Val Crettaz to be out of scope: the UX is "enable in Cloud UI; jump to your Security project for the curated audit log view."
- **gRPC transport.** TDD specifies OTLP/gRPC; POC uses OTLP/HTTP because (a) it's smaller in dep footprint, (b) the in-repo OTLP example (`OTLPLogsIndexingRestIT`) uses HTTP. Migrating to gRPC is later, modest scope.
- **Stateful vs serverless gating.** The new appender is loadable everywhere and off by default. Settings remain inert in stateful clusters.

## 6. Open issues / risks discovered during the POC

1. **Log4j plugin discovery across classloaders.** *(Resolved — required programmatic appender attachment instead of `log4j2.properties` declaration.)* Initial implementation declared the OpenTelemetry appender in `x-pack/plugin/core/src/main/config/log4j2.properties`. Booting ES revealed that fails — the appender plugin class is in the apm module, but x-pack-core's `log4j2.properties` is parsed at JVM startup before plugin/module classloaders are set up. log4j errors observed:
   ```
   main ERROR Unable to locate plugin type for OpenTelemetry
   main ERROR Unable to locate plugin for OpenTelemetry
   main ERROR Unable to invoke factory method ... NullPointerException
   main ERROR Unable to locate appender "audit_rolling" for logger config "..."  ← cascade
   main ERROR Unable to locate appender "console" for logger config "root"        ← cascade
   ```
   The NPE cascaded across log4j config parsing and broke *all* appender registration, not just the OpenTelemetry one. Adding the dep to `:server` (so it's on the boot classpath) caused **jar hell** — `opentelemetry-context` is also pulled transitively by `:x-pack:plugin:esql-datasource-gcs` at a different version, and ES rejects cross-classpath duplicate classes at plugin-load time.

   The working solution: don't declare the appender in `log4j2.properties` at all. Instead, in `OtelSdkExportLogsSupplier.install(...)`, programmatically build an `OpenTelemetryAppender` via its log4j builder, look up the audit logger's `LoggerConfig`, and attach. This runs after plugin classloaders are in scope, so the class is reachable. A second subtlety: pass the `OpenTelemetry` instance to the appender via `Builder.setOpenTelemetry(sdk)` rather than the static `OpenTelemetryAppender.install(sdk)` indirection — the latter only pushes the SDK to appenders that are already in the log4j config at install time, which makes ordering brittle. Verified by booting ES end-to-end and by `OtelAuditLogsIT` (see §3).
2. **Third-party audit on the new appender JAR.** *(Resolved — clean.)* `:modules:apm:thirdPartyAudit` ran cleanly with the new dep added; no `ignoreViolations` or `ignoreMissingClasses` entries needed.
3. **Bootstrap timing.** `OpenTelemetryAppender.install(sdk)` happens during `createComponents`, after log4j has parsed the properties file. Audit events emitted before that point would be silently dropped by the appender. In practice, audit events fire during request handling, well after plugin init — theoretical, but worth a note.
4. **End-to-end assertion test.** Wired up via `RecordingApmServer` (under `:test:external-modules:test-apm-integration`) — a more capable test fixture than `MockApmServer`. Added `ReceivedLog` to the protocol-neutral `ReceivedTelemetry` hierarchy, an `OtlpLogsParser` for the OTLP protobuf, and a `/v1/logs` route on `RecordingApmServer`. New `OtelAuditLogsIT` boots a security-enabled cluster with audit + OTel logs, hits `/_security/_authenticate`, and asserts a `ReceivedLog` arrives. (Note: `MockApmServer` also got an `/v1/logs` handler in this POC, but the recording server is the right primitive for assertion-based tests since it queues records for inspection.)
5. **Entitlement grant required.** *(Resolved.)* First test run failed with `NotEntitledException: component [apm], module [io.opentelemetry.sdk.logs], class [class io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor], entitlement [manage_threads]`. The OTel logs SDK's batch processor spawns a worker thread, which the entitlement system blocks unless explicitly granted. Adding `io.opentelemetry.sdk.logs: [manage_threads]` to `entitlement-policy.yaml` resolved it. This is the kind of finding the POC was meant to surface — a "small" task that would have been a 30-minute mystery in a real PR cycle.
6. **Dependency verification metadata.** *(Resolved.)* New artifact required `gradle/verification-metadata.xml` regeneration (`./gradlew --write-verification-metadata sha256 ...`). Mechanical, but worth knowing the cost upfront — the regen takes ~2 minutes and produces a sizeable diff.

## 7. Composite estimate

For ES-14356, audit-log delivery in serverless, ES core/infra side only:

| Work item | Estimate | Status |
|---|---|---|
| Land the POC code (deps, supplier, log4j wiring) cleanly with thirdPartyAudit + dependency-verification | 3–5 days | mostly done in POC; review polish remains |
| `project.id` propagation into audit records | originally 1–2 hours | **done** in POC (~30 min, simpler than expected — header read from `ThreadContext`) |
| OTel semconv field mapping across all 13 audit event types, including `request.body` PII story | 3–4 weeks | not started |
| mTLS to the gateway, with cert hot-reload | ~1 week | not started |
| Integration test against the recording APM server | 0.5 day | **done** in POC (`OtelAuditLogsIT`) |
| Cross-team review (security, observability, gateway) | ~1 week of calendar time, mostly waiting | not started |
| **Total ES core/infra effort (remaining)** | **~5–6 engineering weeks** | |

**Not included:** Ankit Sethi's filter/suppression work, gateway team work, MOTel project configuration, the cloud UI surface for enabling per-project audit shipping, or the Kibana-side analogue. Those are tracked separately.

This is a load-bearing estimate: if the field-mapping work turns out to need real semconv working-group input, that's the line that grows. Everything else is mechanical.

## 8. Loose ends to chase before merging anything

- Verify the log4j-appender artifact's actual GAV at the OTel version in use, and confirm it doesn't pull a transitive that violates ES's third-party policy.
- Decide whether to keep the SDK install in `modules/apm/` long-term or carve out a sibling module for "customer-visible telemetry" (the apm module is named after Elastic-internal observability; audit-log delivery is conceptually different). For the POC, keeping it in apm is cheaper.
- Sketch the project_id wiring change (4.1) as a single follow-up PR so the eventual full implementation has a known landing pattern.
