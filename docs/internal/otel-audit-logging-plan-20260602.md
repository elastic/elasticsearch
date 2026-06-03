# OTel audit log delivery: plan of record (2026-06-02)

The PoC ([otel-audit-logging-poc.md](otel-audit-logging-poc.md)) validated that an audit event emitted by `LoggingAuditTrail` reaches a recording OTLP server end-to-end — security-enabled multi-node, `project.id` attached, I/O off the calling thread. This doc tracks what's left to make that production-ready, in implementation order.

For the *why* behind any item, follow its `[§x.y]` link into the PoC doc.

**Settled architecture decisions (recorded here for clarity):**
- **Transport**: gRPC (not HTTP). Julio Camarero confirmed HTTP caused uneven K8s load distribution due to connection reuse.
- **Wire protocol**: OTLP/gRPC from ES to the gateway. The gateway's downstream path (MOTel vs elasticsearch-exporter `_bulk`) is the gateway team's decision and doesn't affect what ES emits.
- **Field naming**: ECS in OTLP attributes. OTLP is transport only; ECS names are correct.
- **`trace.id`**: native `LogRecord.traceId` field (confirmed acceptable per `logs-otel@mapping` component template).
- **Module**: `modules/apm/` — `OtelSdkExportLogsSupplier` stays there.
- **CPS routing**: each cluster reports events about its own actions with its own `project.id`. No code change needed.
- **`request.body`**: Omitted from the initial implementation. The field contains potential PII and requires a security review before it can leave the cluster. Deferring it unblocks field translation work; the review can be picked up as a follow-on if customer demand warrants it.

**Open decisions:**
- **Stateful vs serverless gating**: `OtelSdkExportLogsSupplier.install()` currently activates on any cluster where `telemetry.otel.logs.enabled=true`; there is no runtime check against serverless mode. Options: (a) rely on the setting being configured only in serverless deployments — simpler, and allows stateful operators with their own gateway to opt in; (b) add an explicit serverless-mode guard in `install()` — more defensive, locks out stateful use. Decide before the code hardens.

**Implementation requirements from those decisions:**

- Use `OtlpGrpcLogRecordExporter`; the HTTP exporter is not used in production. The test recording APM server needs a gRPC endpoint; the HTTP endpoint can be removed once the APM agent is gone.
- Before implementing mTLS, get the TLS partial-chain workaround from afharo (Alejandro Fernández, Kibana). The gateway certificate chain has multiple parents and standard gRPC TLS clients reject it. Kibana already solved this.
- Emit ECS field names as bare OTLP attributes — no `log4j.map_message.` prefix, no remapping to OTel semconv names. `event.action` stays `event.action`.
- Set `trace.id` in the native `LogRecord.traceId` field, not as an attribute. OTel expects a 32-character lowercase hex string (128-bit trace ID); validate before setting.
- All new code goes in `modules/apm/`. No new module.

---

Items 1–4 below can be started in parallel. They're ordered by how much downstream work each unblocks.

---

1. **Implement attribute-key shape fix** ([§5.1](otel-audit-logging-poc.md#sec-5-1)). The upstream `OpenTelemetryAppender` prepends `log4j.map_message.` to every attribute. Fix options, pick one: custom log4j appender (~50 LOC, preferred), `v3_preview` wrapper (~15 LOC, pulls in incubator deps), `LogRecordProcessor` post-hoc rename (~30 LOC), or `AutoConfiguredOpenTelemetrySdk` refactor (large). Internal engineering call — no stakeholder coordination needed.
   *Unblocks: items 5 and 6.*

2. **Decide strip-fields placement + implement R6** ([§4.6](otel-audit-logging-poc.md#sec-4-6)). Suppress `cluster.name`, `cluster.uuid`, `node.name`, `node.id` on serverless. Preferred option: set the existing `xpack.security.audit.logfile.emit_*` settings in serverless config — no ES code change needed.

3. **File bug + discuss `suppress` coverage gap with Ankit** ([§4.3](otel-audit-logging-poc.md#sec-4-3)). jfreden flagged, during review of PRs 149210/6718, four audit event methods that skip the `suppress` call. File a bug now. Also agree with Ankit on folding `withThreadContext` into `LogEntryBuilder.build()` so all four thread-context fields become structural.

4. **R13 retry/buffer tuning** ([§4.10](otel-audit-logging-poc.md#sec-4-10)). Configure `BatchLogRecordProcessor`: ~2 min retry, ~30–50 MB buffer. A few lines in `OtelSdkExportLogsSupplier`.

---

5. **Per-field ECS translation** ([§4.2](otel-audit-logging-poc.md#sec-4-2)). Field-by-field mapping for remaining audit fields. ECS naming is settled. `trace.id` goes in native `LogRecord.traceId`. *Needs: item 1.*

6. **R4 mTLS to the gateway** ([§4.5](otel-audit-logging-poc.md#sec-4-5)). Protocol is gRPC. Reuse `SSLService` + `PemKeyConfig` + `SSLConfigurationReloader`. Known challenge: the gateway's TLS certificate chain has multiple parents (region CA, environment CA, offline CA) that cause full-chain validation failures in standard gRPC TLS clients. Kibana (afharo / Alejandro Fernández) hit this first and has a workaround — coordinate with them before implementing.
   Before scoping the implementation, spike on whether `OtlpGrpcLogRecordExporterBuilder` exposes a raw `SSLContext` or only PEM bytes. If it accepts `SSLContext` directly, `SSLService` can inject one and the afharo workaround applies cleanly via the builder API. If it only accepts PEM bytes it builds the context internally, and applying the partial-chain workaround requires wrapping the underlying `ManagedChannel` instead — a meaningfully different approach.
   *Needs: item 1 (both items modify `OtelSdkExportLogsSupplier`; item 1 establishes the baseline).*
   *Unblocks: item 9.*

---

*Items 7–8 are gated on Ankit's PRs 149210 + 6718 landing. See "External gates" below.*

7. **`project.id` reconciliation** ([§3.3](otel-audit-logging-poc.md#sec-3-3)). Once PR 6718 lands, drop `withThreadContext`'s `project.id` write — `ServerlessAuditLogCustomizer.enrich` owns it via `ProjectResolver.getProjectId()`. Small change; agreement already in item 3.

8. **`suppress` + `withThreadContext` structural fix** ([§4.3](otel-audit-logging-poc.md#sec-4-3)). Fold both `withThreadContext` and `suppress` into `LogEntryBuilder.build()`. Closes the `suppress` coverage gap and makes all four thread-context fields structural. Agreed in item 3.

---

9. **Gateway integration test** ([§4.12](otel-audit-logging-poc.md#sec-4-12)). IT against the real `otel-delivery-gateway`, alongside the existing in-process recording-server IT. *Needs: item 6 (mTLS) and overall pipeline solid.*

---

*Items 10–11 are not on the critical path for initial delivery and can be deferred.*

10. **R7 stdout fallback on exhausted retries** ([§4.7](otel-audit-logging-poc.md#sec-4-7)). Catch exporter failure after retries, write the record to stdout in a recognizable format for the internal o11y pipeline. Moderate complexity; semi-independent.

11. **R12 dynamic config (OTel logs path)** ([§4.9](otel-audit-logging-poc.md#sec-4-9)). Settings/state listener that re-installs the appender path when `telemetry.otel.logs.enabled` changes without a restart.

---

*Item 12 is gated on the elasticsearch-controller integration (external). See "External gates" below.*

12. **R8 per-project filter + R12 per-project dynamic config** ([§4.8](otel-audit-logging-poc.md#sec-4-8), [§4.9](otel-audit-logging-poc.md#sec-4-9)). Appender-path filter that drops events for projects that haven't opted in, driven by per-project file-based settings from the `elasticsearch-controller`. These two belong to the same unit of work: the filter must react to config changes without a restart.

---

## External gates

- **[PR 149210](https://github.com/elastic/elasticsearch/pull/149210)** (Ankit, `elastic/elasticsearch`, in review) — adds `AuditLogCustomizer` with `enrich` and `suppress` hooks to base `LoggingAuditTrail`. *Gates items 7 and 8.*
- **[PR 6718](https://github.com/elastic/elasticsearch-serverless/pull/6718)** (Ankit, `elastic/elasticsearch-serverless`, in review; linked to 149210) — `ServerlessAuditLogCustomizer`: suppress by realm/system-indices, `project.id` enrichment, field renames. Open in review: `suppress` coverage gap (item 3), operator action frequency concern. *Gates item 7.*
- **CPS/UIAM follow-up branch** (Ankit's fork, not yet a PR) — enables Cloud API key audit logging; planned after 149210 + 6718 merge.
- **elasticsearch-controller integration** (separate team) — per-project file-based settings consumed by ES. *Gates item 12.*
