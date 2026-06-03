# OTel audit log delivery: plan of record (2026-06-02)

The PoC ([otel-audit-logging-poc.md](otel-audit-logging-poc.md)) validated that an audit event emitted by `LoggingAuditTrail` reaches a recording OTLP server end-to-end over OTLP/gRPC — security-enabled multi-node, `project.id` attached for the common origin-via-ingress path, I/O off the calling thread. This doc tracks what's left to make that production-ready.

For the *why* behind any item, follow its `[§x.y]` link into the PoC doc.

## Division of responsibilities

**Patrick (Core/Infra — this plan):** The OTel delivery pipeline — the bridge between what `LoggingAuditTrail` emits into log4j and what arrives at the gateway as an OTLP record. This includes the SDK, gRPC transport, the appender (and therefore the on-wire field shape), mTLS, and retry/buffer tuning. The `serverless-default-settings.yml` config change to strip cluster/node fields. Filing the `suppress`-coverage bug and initiating the `withThreadContext`-in-`build()` alignment conversation with Ankit. Kicking off the Project API audit-config coordination with Julio. Rebase cleanup once Ankit's PRs land.

**Ankit Sethi (Security):** Audit semantics — what events are emitted, what fields they carry internally, which get suppressed or enriched, how operator actions are redacted. Concretely: the `AuditLogCustomizer` extension point and its wiring through `LoggingAuditTrail`; the serverless-side implementation (`ServerlessAuditLogCustomizer`); the `log4j2.serverless.properties` field-rename schema (which the delivery pipeline adopts as its spec); Cloud API key audit logging (CPS/UIAM follow-up). Dynamic OTel delivery toggle (R12) — suggested owner since that processor is the seam that evolves to per-project delivery in multi-project.

**afharo (Alejandro Fernández, Kibana):** TLS partial-chain workaround for the gateway cert (Patrick needs this before implementing mTLS).

**Julio Camarero (Control-Plane / `elasticsearch-controller`):** Project API shape for customer-configurable audit settings; cert distribution path for the gateway client cert.

**Gateway team:** mTLS identity contract, retry/buffer targets, real-gateway integration test.

**UIAM team (Slobodan Adamović):** Cloud API key audit field shape (`api_key.{id,name}` for UIAM-authenticated events).

**CPS team:** Empirical verification that the node-setting source populates `project.id` on linked-cluster events; A-vs-B routing call with the gateway team.

## Branch code status

The PoC branch is the implementation basis for the in-scope items, not a throwaway prototype.

- **Kept and built on** — the OTel delivery layer in `modules/apm`: `OtelSdkExportLogsSupplier`, `OtelSdkSettings`, the `APM`/`APMTelemetryProvider` wiring, `manage_threads` entitlement, `attemptFlushLogs` plumbing, and the gRPC integration-test harness (`OtelAuditLogsIT`, `RecordingApmServer`). No overlap with Ankit's PRs. Complementary to PR 6718's `ServerlessAuditLoggingIT`: that test covers the customizer through the file appender; ours proves OTLP-on-the-wire. Items 1, 4, 5, and 13 extend this code directly.
- **Temporary shim, excised by items 7–8** — the header-based `project.id` plumbing: two lines in `LoggingAuditTrail` (the `setThreadContextField(… X_ELASTIC_PROJECT_ID_HTTP_HEADER …)` write in `withThreadContext`), one pattern line in core `log4j2.properties`, and the `projectId()` assertion helper and its ~30 call sites in `LoggingAuditTrailTests`. Kept so the branch stays demonstrable without depending on the unmerged PRs; superseded by `ServerlessAuditLogCustomizer.enrich()` once PR 6718 lands. PR 149210 touches `LoggingAuditTrail` substantially (+113/−32), so a merge conflict is expected: take Ankit's version and drop our two lines.
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
- **Cloud API key audit shape** ([§4.13](otel-audit-logging-poc.md#sec-4-13)). What populates `api_key.id` / `api_key.name` for `CLOUD_API_KEY`-typed events, given the source values live in UIAM metadata rather than the regular API key store? Alignment conversation with UIAM team (Slobodan Adamović).

**Implementation requirements from those decisions:**

- Use `OtlpGrpcLogRecordExporter` (okhttp-based gRPC sender). gRPC transport via OkHttp is already validated end-to-end on this branch ([§3.5](otel-audit-logging-poc.md#sec-3-5)).
- Before implementing mTLS, get the TLS partial-chain workaround from afharo (Alejandro Fernández, Kibana). The gateway certificate chain has multiple parents and standard gRPC TLS clients reject it. Kibana already solved this.
- Emit field names as bare OTLP attributes matching the serverless file-layout schema — no `log4j.map_message.` prefix. Field naming is settled (see above).
- Set `trace.id` in the native `LogRecord.traceId` field, not as an attribute. OTel expects a 32-character lowercase hex string (128-bit trace ID); validate before setting.
- All new code goes in `modules/apm/`. No new module.

---

## In scope (initial delivery)

Items 1–4 can be started in parallel.

1. **Build OTel-path attribute transformation** ([§5.1](otel-audit-logging-poc.md#sec-5-1), [§4.2](otel-audit-logging-poc.md#sec-4-2)). The serverless `log4j2.serverless.properties` rewrite in PR 6718 is the spec: the OTel path must produce the same field names. A custom log4j appender (~50 LOC, preferred) is the natural home: it drops the `log4j.map_message.` prefix and applies the rename map from internal ES audit names to the `elasticsearch.audit.*` / ECS / semconv target schema. `trace.id` goes in native `LogRecord.traceId`. Since `enrich()` is the first call in `build()` — guaranteed by construction — the appender always sees operator-redacted `action`/`request.name`; no special ordering care is needed. No stakeholder coordination required; the target names are settled.
   *Unblocks: item 5.*

2. **R6 strip-fields on serverless** ([§4.6](otel-audit-logging-poc.md#sec-4-6)). Set `emit_node_id` and `emit_cluster_uuid` to `false` in `serverless-default-settings.yml`. With these settings false, `EntryCommonFields` never puts `node.id` or `cluster.uuid` into the `StringMapMessage`, so both the file appender and the OTel appender path are covered at the source. No ES code change. (PR 6718 also removes these fields from the file-appender `PatternLayout` directly, but that change doesn't cover the OTel path — the settings file change is what does.)

3. **File bug + discuss `suppress` coverage gap with Ankit** ([§4.3](otel-audit-logging-poc.md#sec-4-3)). PR 149210 adds `suppress` at most call sites but misses the `HttpPreRequest` variants of `anonymousAccessDenied`, `authenticationFailed`, and `tamperedRequest`, as well as `connectionGranted` and `connectionDenied`. File a bug with those specific method names. Also agree with Ankit on folding `withThreadContext` into `LogEntryBuilder.build()` so all four thread-context fields become structural rather than convention.

4. **R13 retry/buffer tuning** ([§4.10](otel-audit-logging-poc.md#sec-4-10)). Configure `BatchLogRecordProcessor` to retry up to ~2 min and bound the in-memory buffer at ~30–50 MB (gateway team's targets). A few lines in `OtelSdkExportLogsSupplier`.

5. **R4 mTLS to the gateway** ([§4.5](otel-audit-logging-poc.md#sec-4-5)). Protocol is gRPC. Build the `SSLContext` via `SSLService` + `PemKeyConfig` with the afharo partial-chain workaround applied, inject via `OtlpGrpcLogRecordExporterBuilder.setSslContext(...)`, integrate `SSLConfigurationReloader` for rotation. Coordinate with afharo (Alejandro Fernández, Kibana) before implementing — the gateway certificate chain has multiple parents that cause full-chain validation failures, and Kibana already solved this. *Needs: item 1.*

6. **Project API audit configuration coordination** ([§4.15](otel-audit-logging-poc.md#sec-4-15)). Work with Julio Camarero (Control-Plane / `elasticsearch-controller`) on the Project API shape and file-settings rendering for customer-configurable audit settings (five `events.ignore_filters.*`, `events.include`, `events.exclude`, plus six `emit_*`). ES side is already wired — these settings are `NodeScope + Dynamic` with existing listeners. The work is *what to expose and how* in the Project API, not ES code. *Coordination, not implementation.*

---

## Conditional on external PRs landing

Items 7–8 become in-scope if Ankit's PRs 149210 + 6718 land.

7. **`project.id` reconciliation** ([§3.3](otel-audit-logging-poc.md#sec-3-3), [§5.4](otel-audit-logging-poc.md#sec-5-4)). Once PR 6718 lands, delete the PoC's `withThreadContext` `project.id` write — `ServerlessAuditLogCustomizer.enrich` sources it from the node setting and writes it in `build()`, so the `withThreadContext` path produces a duplicate. One-line deletion. Also check that `OtelAuditLogsIT` still has a meaningful `project.id` assertion; this may require wiring a test `AuditLogCustomizer` that populates the field.

8. **`suppress` + `withThreadContext` structural fix** ([§4.3](otel-audit-logging-poc.md#sec-4-3)). Once PR 149210 lands, add `suppress` to the HTTP-variant emit methods listed in item 3, and fold `withThreadContext` into `LogEntryBuilder.build()`. The `enrich`-in-`build()` part is already done by PR 149210; what remains is closing the `suppress` gaps and making `withThreadContext` structural.

---

## Follow-on (deferred)

9. **Gateway integration test** ([§4.12](otel-audit-logging-poc.md#sec-4-12)). IT against the real `otel-delivery-gateway`, alongside the existing in-process recording-server IT. *Deferred: PoC's in-process IT pins the ES-side contract; real-gateway IT is the natural next step after mTLS.*

10. **Cloud API key `api_key.*` fields** ([§4.13](otel-audit-logging-poc.md#sec-4-13)). Populate `api_key.id` and `api_key.name` for `CLOUD_API_KEY`-typed events and remove the structural `assert false == authentication.isCloudApiKey()` in `addAuthenticationFieldsToLogEntry`. The `project.id` fragility noted in the PoC is resolved — `ServerlessAuditLogCustomizer` sources it from the node setting independently of UIAM. What remains is the field-population question: which values fill `api_key.{id,name}`, given that the source lives in UIAM metadata rather than the regular API key store. *Deferred: gated on Ankit's CPS/UIAM follow-up branch and UIAM-team alignment (Slobodan Adamović).*

11. **Linked-side `project.id` for CPS** ([§4.4](otel-audit-logging-poc.md#sec-4-4)). With the node-setting source, each linked cluster reports its own `project.id` from `serverless.project_id` — no separate header-propagation mechanism is needed. Remaining work: (a) verify empirically with a CPS IT that the node setting correctly populates the field on linked-side events; (b) agree with the gateway team on A-vs-B routing (origin project vs data-owner project). *Downgraded from implementation to verification + coordination; needs CPS-team alignment.*

12. **R7 stdout fallback on exhausted retries** ([§4.7](otel-audit-logging-poc.md#sec-4-7)). Catch exporter failure after retries, write the record to stdout in a recognizable format for the internal o11y pipeline. *Deferred: without R7, we lose audit records on persistent gateway outage. Acceptable for v1; the platform-side replay service is the other half of at-least-once and is also out of this scope.*

13. **R12 dynamic OTel delivery toggle** ([§4.9](otel-audit-logging-poc.md#sec-4-9)). `telemetry.otel.logs.enabled` stays NodeScope/static — it controls whether the SDK is installed at all. For runtime on/off without restart, add a second Dynamic NodeScope setting paired with a `LogRecordProcessor` that wraps `BatchLogRecordProcessor`. The processor gates delivery on a `BooleanSupplier` updated by a settings listener; records are dropped before they enter the queue when delivery is off. No SDK teardown. **Suggested ownership: Security/audit team (Ankit Sethi)**, since the processor is the seam that evolves to per-project delivery in multi-project. R8 collapses into the same mechanism for project-per-cluster. *Deferred: customers need a restart to flip the toggle. Acceptable for v1 if the setting is provisioned-then-static.*

---

## External gates

- **[PR 149210](https://github.com/elastic/elasticsearch/pull/149210)** (Ankit, `elastic/elasticsearch`, in review) — adds `AuditLogCustomizer` (with `enrich`/`suppress`), `AuditEntry`, `AuditEventContext` to `:x-pack:plugin:core`; wires `suppress` at most emit call sites (HTTP-variant gaps documented in item 3); folds `enrich` into `LogEntryBuilder.build()`; exposes `SecurityExtension.getAuditLogCustomizer(SecurityComponents, SystemIndices)` as the registration hook. *Gates items 7 and 8.*
- **[PR 6718](https://github.com/elastic/elasticsearch-serverless/pull/6718)** (Ankit, `elastic/elasticsearch-serverless`, in review; linked to 149210) — `ServerlessAuditLogCustomizer`: suppress by realm/system-indices, operator action redaction to `"elastic.maintenance"`, `project.id` enrichment from the node's `serverless.project_id` setting (tagged `@FixForMultiProject`); rewrites `log4j2.serverless.properties` to establish the `elasticsearch.audit.*` field-name scheme used as the OTel-path spec in item 1; includes `ServerlessAuditLoggingIT`. *Gates item 7.*
- **CPS/UIAM follow-up branch** (Ankit's fork, not yet a PR) — enables Cloud API key audit logging; planned after 149210 + 6718 merge. *Gates item 10.*
- **`elasticsearch-controller` Project API extensions** (Control-Plane / Julio Camarero) — per-project audit configuration fields. *Coordinated via item 6.*
