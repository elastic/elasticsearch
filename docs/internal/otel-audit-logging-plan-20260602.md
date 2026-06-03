# OTel audit log delivery: plan of record (2026-06-02)

The PoC ([otel-audit-logging-poc.md](otel-audit-logging-poc.md)) validated that an audit event emitted by `LoggingAuditTrail` reaches a recording OTLP server end-to-end over OTLP/gRPC — security-enabled multi-node, `project.id` attached for the common origin-via-ingress path, I/O off the calling thread. This doc tracks what's left to make that production-ready.

For the *why* behind any item, follow its `[§x.y]` link into the PoC doc.

## Division of responsibilities

**Patrick (Core/Infra — this plan):** The OTel delivery pipeline — the bridge between what `LoggingAuditTrail` emits into log4j and what arrives at the gateway as an OTLP record. This includes the SDK, gRPC transport, the appender (and therefore the on-wire field shape), mTLS, and retry/buffer tuning. The `serverless-default-settings.yml` config change to strip cluster/node fields. Kicking off the audit config delivery coordination with Julio. Rebase cleanup once Ankit's PRs land.

**Ankit Sethi (Security):** Audit semantics — what events are emitted, what fields they carry internally, which get suppressed or enriched, how operator actions are redacted. Concretely: the `AuditLogCustomizer` extension point and its wiring through `LoggingAuditTrail`; the serverless-side implementation (`ServerlessAuditLogCustomizer`); the `log4j2.serverless.properties` field-rename schema (which the delivery pipeline adopts as its spec); Cloud API key audit logging (CPS/UIAM follow-up).

**Julio Camarero (Control-Plane / `elasticsearch-controller`):** File-based settings delivery for customer-configurable audit settings; cert delivery path for the gateway mTLS client cert (expected to follow the same pattern as workload-identity certs).

**Gateway team:** mTLS identity contract, retry/buffer targets, real-gateway integration test.

**CPS team:** Empirical verification that the node-setting source populates `project.id` on linked-cluster events; A-vs-B routing call with the gateway team.

## PR delivery sequence

**PR 1 — OTel audit log delivery foundation** *(submit now)*

Establishes the permanent delivery pipeline. Files:
- `modules/apm/`: `OtelSdkExportLogsSupplier` (SDK setup, gRPC exporter, `BatchLogRecordProcessor` with default settings, programmatic appender attachment), `OtelSdkSettings`, `APM`/`APMTelemetryProvider` wiring, `manage_threads` entitlement.
- `server/`: `TelemetryProvider.attemptFlushLogs`.
- `test/external-modules/apm-integration/`: `RecordingApmServer` (gRPC dual-protocol), `OtelAuditLogsIT`, `OtlpLogsParser`, `ReceivedTelemetry`, build deps.
- `x-pack/plugin/core/src/main/config/log4j2.properties`: comment block explaining programmatic attachment only — not the `project.id` pattern line.
- `gradle/verification-metadata.xml`, `modules/apm/build.gradle`.

The IT asserts on `log4j.map_message.`-prefixed attribute names — intentionally, because PR 2 is what fixes the prefix. No `project.id` assertion.

**Item 2 — Strip-fields on serverless** *(submit any time after PR 1)*

One-line change: set `emit_node_id` and `emit_cluster_uuid` to `false` in `serverless-default-settings.yml`. No trigger dependency.

**PR 2 — Custom appender with ECS/semconv field mapping** *(plan item 1)*

*Trigger: PR 6718 in `elastic/elasticsearch-serverless` merges.* To check: `ghool with-key elastic gh api repos/elastic/elasticsearch-serverless/pulls/6718 --jq '.merged'` — proceed when the result is `true`.

Replaces the raw `OpenTelemetryAppender` with a custom appender that drops the `log4j.map_message.` prefix, applies the field rename table matching the now-merged `log4j2.serverless.properties` schema, and puts `trace.id` in native `LogRecord.traceId`. Updates IT assertions to use unprefixed names.

**PR 3 — mTLS and retry/buffer tuning** *(plan items 3 and 4)*

*Trigger: PR 2 merges and gateway team has confirmed retry/buffer targets.*

**Item 6 — `project.id` reconciliation** *(plan item 6)*

*Trigger: PR 6718 in `elastic/elasticsearch-serverless` merges.* To check: `ghool with-key elastic gh api repos/elastic/elasticsearch-serverless/pulls/6718 --jq '.merged'` — proceed when the result is `true`.

Delete the `withThreadContext` `project.id` write. If PR 149210 has also landed by then, expect a conflict on `LoggingAuditTrail` — take Ankit's version and drop our two lines. Also verify `OtelAuditLogsIT` still has a meaningful `project.id` assertion; may require wiring a test `AuditLogCustomizer`.

---

## Branch code status

The PoC branch is the implementation basis for the in-scope items, not a throwaway prototype.

- **Kept and built on** — the OTel delivery layer in `modules/apm`: `OtelSdkExportLogsSupplier`, `OtelSdkSettings`, the `APM`/`APMTelemetryProvider` wiring, `manage_threads` entitlement, `attemptFlushLogs` plumbing, and the gRPC integration-test harness (`OtelAuditLogsIT`, `RecordingApmServer`). No overlap with Ankit's PRs. Complementary to PR 6718's `ServerlessAuditLoggingIT`: that test covers the customizer through the file appender; ours proves OTLP-on-the-wire. Items 1, 3, and 4 extend this code directly.
- **Temporary shim, excised by item 6** — the header-based `project.id` plumbing: two lines in `LoggingAuditTrail` (the `setThreadContextField(… X_ELASTIC_PROJECT_ID_HTTP_HEADER …)` write in `withThreadContext`), one pattern line in core `log4j2.properties`, and the `projectId()` assertion helper and its ~30 call sites in `LoggingAuditTrailTests`. Kept so the branch stays demonstrable without depending on the unmerged PRs; superseded by `ServerlessAuditLogCustomizer.enrich()` once PR 6718 lands. PR 149210 touches `LoggingAuditTrail` substantially (+113/−32), so a merge conflict is expected: take Ankit's version and drop our two lines.
- **Build nothing new on the shim** — the OTel appender emits whatever is in the audit `StringMapMessage` and is agnostic to how `project.id` got there. When `enrich()` replaces the header-write, the appender is unchanged, so nothing built in the meantime needs to be redone.

**Settled architecture decisions (recorded here for clarity):**
- **Transport**: OTLP/gRPC. Julio Camarero confirmed HTTP caused uneven K8s load distribution due to connection reuse. Implemented; see [§3.5](otel-audit-logging-poc.md#sec-3-5).
- **Audit `project.id` source**: via `AuditLogCustomizer.enrich()`, registered through the `SecurityExtension.getAuditLogCustomizer(SecurityComponents, SystemIndices)` hook (PR 149210). `enrich()` is the first statement in `LogEntryBuilder.build()`, before `logger.info()`. The serverless wiring (`ServerlessAuditLogCustomizer`, PR 6718) sources `project.id` from `() -> PROJECT_ID.get(components.settings())` — the node's `serverless.project_id` cluster setting, tagged `@FixForMultiProject`. For project-per-cluster, the node setting is authoritative; multi-project implications are out of scope.
- **Field naming**: The PR 6718 rewrite of `log4j2.serverless.properties` establishes the target names: `elasticsearch.audit.*` for custom fields (`apikey.{id,name}`, `indices`, `action`, `request.name`, `origin.{type,address}`, `request.id`, `security_config_change.*`); standard ECS/semconv for the remainder (`event.action`, `event.type`, `user.name`, `http.request.method`, `url.path`, `url.query`, `trace.id`, etc.). The OTel path adopts these names verbatim. Cross-product consistency with Kibana is a heads-up, not a blocker.
- **`trace.id`**: native `LogRecord.traceId` field (confirmed acceptable per `logs-otel@mapping` component template).
- **Module**: `modules/apm/` — `OtelSdkExportLogsSupplier` stays there for initial delivery. Longer-term reconsideration tracked in [§5.3](otel-audit-logging-poc.md#sec-5-3).
- **`request.body`**: Deferred from initial implementation. The field contains potential PII and requires a security review before it can leave the cluster. May be revisited as a follow-on if customer demand warrants it; see [§4.11](otel-audit-logging-poc.md#sec-4-11).
- **Multi-project**: Out of scope for initial delivery. Project-per-cluster is the assumption; multi-project considerations are preserved in [PoC Appendix C](otel-audit-logging-poc.md#sec-appendix-c).

**Open decisions:**
- **Stateful vs serverless gating**. `OtelSdkExportLogsSupplier.install()` activates on any cluster where `telemetry.otel.logs.enabled=true`; there is no runtime check against serverless mode. Options: (a) rely on the setting being configured only in serverless deployments — simpler, and allows stateful operators with their own gateway to opt in; (b) add an explicit serverless-mode guard in `install()` — more defensive, locks out stateful use. Decide before the code hardens.
**Implementation requirements from those decisions:**

- Use `OtlpGrpcLogRecordExporter` (okhttp-based gRPC sender). gRPC transport via OkHttp is already validated end-to-end on this branch ([§3.5](otel-audit-logging-poc.md#sec-3-5)).
- For mTLS: the gateway cert chain has multiple parents that cause full-chain validation failures with a naive custom-CA trust store. The fix (identified from Kibana's `otel_tls.ts` → `toGrpcRootCerts`) is to concatenate the configured CA cert with the JVM default trusted CAs before building the `TrustManagerFactory`, so intermediate CAs whose root is in the system store are verified. Build the `SSLContext` following the `WorkloadIdentitySslConfig` pattern (`modules/workload-identity` in the ES codebase): `SslConfigurationLoader` with a `telemetry.otel.logs.ssl.*` prefix, `PemKeyConfig` for key material, `ResourceWatcherService` for hot-reload.
- Emit field names as bare OTLP attributes matching the serverless file-layout schema — no `log4j.map_message.` prefix. Field naming is settled (see above).
- Set `trace.id` in the native `LogRecord.traceId` field, not as an attribute. OTel expects a 32-character lowercase hex string (128-bit trace ID); validate before setting.
- All new code goes in `modules/apm/`. No new module.

---

## In scope (initial delivery)

Items 1, 2, and 3 can be started in parallel.

1. **Build OTel-path attribute transformation** ([§5.1](otel-audit-logging-poc.md#sec-5-1), [§4.2](otel-audit-logging-poc.md#sec-4-2)). The serverless `log4j2.serverless.properties` rewrite in PR 6718 is the spec: the OTel path must produce the same field names. A custom log4j appender (~50 LOC, preferred) is the natural home: it drops the `log4j.map_message.` prefix and applies the rename map from internal ES audit names to the `elasticsearch.audit.*` / ECS / semconv target schema. `trace.id` goes in native `LogRecord.traceId`. Since `enrich()` is the first call in `build()` — guaranteed by construction — the appender always sees operator-redacted `action`/`request.name`; no special ordering care is needed. No stakeholder coordination required; the target names are settled.
   *Unblocks: item 4.*

2. **R6 strip-fields on serverless** ([§4.6](otel-audit-logging-poc.md#sec-4-6)). Set `emit_node_id` and `emit_cluster_uuid` to `false` in `serverless-default-settings.yml`. With these settings false, `EntryCommonFields` never puts `node.id` or `cluster.uuid` into the `StringMapMessage`, so both the file appender and the OTel appender path are covered at the source. No ES code change. (PR 6718 also removes these fields from the file-appender `PatternLayout` directly, but that change doesn't cover the OTel path — the settings file change is what does.)

3. **R13 retry/buffer tuning** ([§4.10](otel-audit-logging-poc.md#sec-4-10)). Configure `BatchLogRecordProcessor` to retry up to ~2 min and bound the in-memory buffer at ~30–50 MB (gateway team's targets). A few lines in `OtelSdkExportLogsSupplier`.

4. **R4 mTLS to the gateway** ([§4.5](otel-audit-logging-poc.md#sec-4-5)). Protocol is gRPC. Follow the `WorkloadIdentitySslConfig` pattern (`modules/workload-identity`): add `telemetry.otel.logs.ssl.*` settings (certificate authorities, certificate, key, passphrase, verification mode), load via `SslConfigurationLoader`, build a combined trust store (configured CAs + JVM default roots — the fix for the gateway cert's multiple-parent chain), inject via `OtlpGrpcLogRecordExporterBuilder.setSslContext(...)`, wire `ResourceWatcherService` for hot-reload on rotation. Client cert and key path are delivered to the node by `elasticsearch-controller` alongside the `telemetry.otel.logs.ssl.*` path-settings (coordinate with Julio Camarero — expected to follow the workload-identity cert delivery pattern). *Needs: item 1.*

5. **Audit config delivery coordination** ([§4.15](otel-audit-logging-poc.md#sec-4-15)). Work with Julio Camarero (Control-Plane / `elasticsearch-controller`) on file-based delivery of customer-configurable audit settings to ES nodes. The settings are already NodeScope + Dynamic with listeners on the ES side: `xpack.security.audit.logfile.events.include`, `.events.exclude`, the five `events.ignore_filters.{policy}.*` affixes, and the six `emit_*` flags. The work is confirming which of these are customer-facing vs. platform-managed, and how the controller renders them into the file-based settings it already delivers to nodes. No ES code change. *Coordination, not implementation.*

---

## Conditional on external PRs landing

6. **`project.id` reconciliation** ([§3.3](otel-audit-logging-poc.md#sec-3-3), [§5.4](otel-audit-logging-poc.md#sec-5-4)). Once PR 6718 lands, delete the PoC's `withThreadContext` `project.id` write — `ServerlessAuditLogCustomizer.enrich` sources it from the node setting and writes it in `build()`, so the `withThreadContext` path produces a duplicate. One-line deletion. Also check that `OtelAuditLogsIT` still has a meaningful `project.id` assertion; this may require wiring a test `AuditLogCustomizer` that populates the field.

---

## Follow-on (deferred)

7. **Gateway integration test** ([§4.12](otel-audit-logging-poc.md#sec-4-12)). IT against the real `otel-delivery-gateway`, alongside the existing in-process recording-server IT. *Deferred: PoC's in-process IT pins the ES-side contract; real-gateway IT is the natural next step after mTLS.*

8. **Linked-side `project.id` for CPS** ([§4.4](otel-audit-logging-poc.md#sec-4-4)). With the node-setting source, each linked cluster reports its own `project.id` from `serverless.project_id` — no separate header-propagation mechanism is needed. Remaining work: (a) verify empirically with a CPS IT that the node setting correctly populates the field on linked-side events; (b) agree with the gateway team on A-vs-B routing (origin project vs data-owner project). *Downgraded from implementation to verification + coordination; needs CPS-team alignment.*

9. **R7 stdout fallback on exhausted retries** ([§4.7](otel-audit-logging-poc.md#sec-4-7)). Catch exporter failure after retries, write the record to stdout in a recognizable format for the internal o11y pipeline. *Deferred: without R7, we lose audit records on persistent gateway outage. Acceptable for v1; the platform-side replay service is the other half of at-least-once and is also out of this scope.*

---

## External gates

- **[PR 149210](https://github.com/elastic/elasticsearch/pull/149210)** (Ankit, `elastic/elasticsearch`, in review) — adds `AuditLogCustomizer` (with `enrich`/`suppress`), `AuditEntry`, `AuditEventContext` to `:x-pack:plugin:core`; wires `suppress` at most emit call sites; folds `enrich` into `LogEntryBuilder.build()`; exposes `SecurityExtension.getAuditLogCustomizer(SecurityComponents, SystemIndices)` as the registration hook. *Expected to conflict with the PoC shim in `LoggingAuditTrail`; rebase cleanup is part of item 6.*
- **[PR 6718](https://github.com/elastic/elasticsearch-serverless/pull/6718)** (Ankit, `elastic/elasticsearch-serverless`, in review; linked to 149210) — `ServerlessAuditLogCustomizer`: suppress by realm/system-indices, operator action redaction to `"elastic.maintenance"`, `project.id` enrichment from the node's `serverless.project_id` setting (tagged `@FixForMultiProject`); rewrites `log4j2.serverless.properties` to establish the `elasticsearch.audit.*` field-name scheme used as the OTel-path spec in item 1; includes `ServerlessAuditLoggingIT`. *Gates PR 2 and item 6.*
- **`elasticsearch-controller` audit config delivery** (Control-Plane / Julio Camarero) — file-based delivery of customer-configurable audit settings and mTLS client cert to ES nodes. *Coordinated via item 5.*
