# OTel audit log delivery: full inventory of decisions and actions

Every distinct thing this project ever proposed to **do** or **decide**, gathered
from across the full git history of [otel-audit-logging-poc.md](otel-audit-logging-poc.md)
(May 6 – Jun 2, 2026) and de-duplicated. The goal is a single checklist so nothing
that was ever raised gets silently dropped.

For the forward-looking, ordered plan see [otel-audit-logging-plan-20260602.md](otel-audit-logging-plan-20260602.md);
for the *why* behind any item, follow it into the PoC doc.

## Decisions

1. **Architecture A (OTLP from app) vs B (file + sidecar collector tailing).** B is operationally appealing because it survives ES-process death; A is what the Jira description and the gateway TDD assume.
2. **gRPC vs OTLP/HTTP-protobuf transport.** (Julio Camarero: HTTP caused uneven K8s load distribution via connection reuse.)
3. **Attribute-key shape** — how to drop the `log4j.map_message.` prefix: custom appender / `v3_preview` wrapper / `AutoConfiguredOpenTelemetrySdk` refactor / `LogRecordProcessor` rename.
4. **Field-mapping shape: OTLP-as-transport / ECS vs OTel semconv vs custom.**
5. **Per-field translation of the ambiguous fields** — `origin.address` REST/transport split (`client.address` vs `server.address`), `authentication.type` / `user.roles`, the `apikey.*` namespace, the `indices` array, and the `security_config_change` nested `put`/`delete`/`change`/`create`/`invalidate` blobs (flatten-to-attributes vs opaque-JSON-string).
6. **`trace.id` placement** — native `LogRecord.traceId` vs attribute.
7. **`request.body` PII story** — default-off vs redaction vs sampling (needs security review).
8. **`project.id` direction for CPS** — originator vs data-owner.
9. **R6 strip-fields placement** — existing `xpack.security.audit.logfile.emit_*` settings vs branch in `EntryCommonFields` vs OTel-path attribute filter.
10. **Where the SDK setup lives** — `modules/apm` vs a new `modules/customer-telemetry`.
11. **`LoggingAuditTrail` constructor for `project.id`** — inject `ProjectResolver` vs `CustomAuditLoggingMetadataProvider` extension vs direct `ThreadContext` read.
12. **Is the missing `withThreadContext` on `security_config_change` a bug or by-design?**
13. **Fold `withThreadContext` into `build()` to make the chokepoint structural?**
14. **In-app buffering / retry / spill-to-disk: build vs rely on SDK defaults.** (Reasoning: reliability is the telemetry infra's job; ES-14439 is the metrics analogue.)
15. **Customer-configured redaction — whether to support it.** (Val Crettaz framing: "enable in Cloud UI, view in the Security project".)
16. **Stateful vs serverless gating of the new appender.**
17. **Direct OTel SDK call from `LoggingAuditTrail` vs an appender.**

## Implementation actions

18. Land PoC code cleanly — deps, `OtelSdkExportLogsSupplier`, log4j wiring.
19. Implement the attribute-key shape fix (per #3).
20. Per-field ECS translation (per #4 / #5).
21. `project.id` reconciliation — drop the `withThreadContext` write, let `enrich` own it (sequenced after PR 6718).
22. File a bug for the missing `withThreadContext` on `security_config_change`; add `LoggingAuditTrailTests` assertions on the security-change paths.
23. Fold `withThreadContext` into `build()` (structural chokepoint).
24. File a bug for the `suppress()` call-site gaps (jfreden's four flagged methods).
25. R4 mTLS — reuse `SSLService` + `PemKeyConfig` + `SSLConfigurationReloader` (cert hot-reload on rotation); get afharo's partial-chain workaround first.
26. R6 strip cluster/node fields on serverless (per #9).
27. R7 stdout fallback on exhausted retries.
28. R8 per-project filter — appender-path filter + config source from the `elasticsearch-controller`.
29. R12 dynamic config without restart — `telemetry.logs.enabled` + per-project state.
30. R13 retry/buffer tuning (~2 min retry, ~30–50 MB buffer).
31. Integration test against the real `otel-delivery-gateway` (alongside the in-process recording IT).
32. Add a gRPC endpoint to the recording test server; remove HTTP once the APM agent is gone.
33. Dependency hygiene for new artifacts — verify GAV + transitive deps against ES third-party policy, re-run `:modules:apm:thirdPartyAudit`, regenerate `gradle/verification-metadata.xml`. (Reopens when switching to the gRPC exporter and/or pulling incubator deps for `v3_preview`.)
34. Entitlement policy — `manage_threads` for `io.opentelemetry.sdk.logs`, plus any additional network/socket entitlements the gRPC sender needs.
35. Kick off the `request.body` security review (per #7).
36. Sketch `project_id` wiring as a single follow-up PR (landing pattern).

## Questions and risks raised

37. Log4j plugin discovery across classloaders — whether log4j can find the `OpenTelemetry` appender plugin class when parsing `log4j2.properties` at boot, given it lives in the apm module's classloader.
38. Bootstrap timing — appender `install` ordering; audit events emitted before install would be dropped.
39. Third-party audit on the appender JAR — whether a new dep introduces forbidden classes or unresolved references.
40. Assertion test against MockApmServer — standing up a JUnit-driven OTLP capture against the test fixture.
41. `manage_threads` entitlement — the batch processor's worker thread needs it granted.
42. `verification-metadata` regeneration cost when adding artifacts.
43. Does `OtlpHttp/GrpcLogRecordExporterBuilder` expose client TLS directly, or must we wrap the underlying client?
44. gRPC TLS partial-chain failure — the gateway cert has multiple CA parents; workaround from afharo.
45. Bare SDK builder can't set a `ConfigProvider` / the v3-preview flag.
46. `OpenTelemetryAppender.install(sdk)` is order-dependent.
47. `StringMapMessage` is bodyless → captured as prefixed attributes.
48. Operator actions possibly triggered by automation (could flood customer logs) — jfreden/consulthys asked Ankit to confirm with the platform team.

## External / other-team dependencies

49. PR 149210 (`AuditLogCustomizer`), PR 6718 (`ServerlessAuditLogCustomizer`), CPS/UIAM fork branch.
50. `elasticsearch-controller` per-project file-based settings.
51. Platform "replay" service — the other half of at-least-once (R11).
52. Gateway team work; MOTel project config; Cloud UI for per-project enablement; Kibana-side analogue.

## Estimate notes

53. Per-field mapping is the line that grows (originally sized 3–4 weeks).
54. Cross-team review ~1 week of wall-clock time (security / observability / gateway).
