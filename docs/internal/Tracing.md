# Native tracing in Elasticsearch

Elasticsearch instrumentation uses the OpenTelemetry API supplied by
`TelemetryProvider.getOpenTelemetry()`. The APM module owns one tracing SDK per
node. Instrumentation must not create an SDK, install a global provider, flush
exporters, or shut the provider down. Metrics and logging retain their existing
integration points.

This foundation replaces the ES `Tracer`, `Traceable`, and `TraceContext`
interfaces. There is no legacy instrumentation path and no span-ID lookup map.
It migrates existing instrumentation, including the search query phase, but does
not add ES|QL engine-specific spans.

## Responsibilities

| Component | Responsibility |
| --- | --- |
| `APMTracingService` | SDK lifecycle and live Elasticsearch tracing settings |
| `PolicyOpenTelemetry` | Local capture policy using standard OTel API interfaces |
| `SanitizingSpanExporter` | Attribute privacy at the final export boundary |
| `ThreadContext` | Capture and restore native context across ES execution boundaries |
| `TracingContext` | Explicit activation, wire propagation, and common failure classification |
| `Task` / `RestRequest` | Own their span's context and its exactly-once completion |
| Instrumentation | Meaningful span boundaries, native attributes, and terminal outcomes |

Core code depends on the OTel API and context libraries, not SDK implementations.
Do not test for the identity of `OpenTelemetry.noop()` to decide whether recording
is enabled: provider and tracer handles remain stable while settings change.

## Ordinary component instrumentation

Instrumentation does not need a transport action or an Elasticsearch task. A
component can keep a tracer obtained from the node provider and use ordinary OTel
spans and scopes:

```java
var tracer = telemetryProvider.getOpenTelemetry().getTracer("elasticsearch.component");
var span = tracer.spanBuilder("component.operation").startSpan();
try (var scope = span.makeCurrent()) {
    operation.run();
    span.setAttribute("es.outcome", "success");
} catch (Exception failure) {
    TracingContext.recordFailure(span, failure);
    throw failure;
} finally {
    span.end();
}
```

Closing a scope restores the caller's context; it does **not** end a span. Close
each scope on the thread that opened it. End the span when its operation actually
completes, which may be on another thread. OTel contexts are immutable and can be
captured for later execution; scopes cannot be transferred between threads.

The local capture decorator carries depth through native `Span.makeCurrent()`
and `Context.with(span)`. Callers do not need an ES-specific activation helper to
make filtering work. The SDK remains responsible for distributed sampling.

## Asynchronous execution

Elasticsearch executors, `ThreadContext.preserveContext`, and
`ContextPreservingActionListener` carry native context along with request context.
Capture the caller's listener **before** activating a child span. Capture the
operation's completion listener **inside** the child scope. Record the outcome
and end the child before invoking the caller's context-preserving listener.

For work submitted to executors outside Elasticsearch, explicitly preserve the
context or use OTel's context-wrapping APIs. Merely retaining a span does not make
it current on a worker thread.

`newEmptyContext`, `newEmptySystemContext`, and `clearTraceContext` deliberately
detach work from its caller. Generic stored-context restoration preserves wire
headers as captured; it must not erase headers just because no native span was
active at capture time.

## Tasks, HTTP, and transport

Task registration creates and attaches a span when a parent context exists. It
does not activate it. Standard action execution activates the task automatically.
Manual registration sites must use `TaskManager.withTaskContext(task)` while
executing or scheduling the task's work. An intentionally untraced task borrows
context without gaining permission to mutate or end the parent's span.

Registration attaches the span before the task is published, so a concurrent
unregister can never end a span that has not been attached yet.

Unregistering ends the owned task span. A task span records lifetime and
parentage only: it carries no `es.outcome` or `error.type` attribute and its
status is left `UNSET`, because `TaskManager.unregister` has no result to
inspect and failure is already reported to the caller. Failure is never inferred
from child spans. An initial async response is not the completion of the
background operation, so the span ends when the background work does. Duplicate
cleanup must not end a parent or export an additional span.

HTTP spans end at the response boundary, not when dispatch returns. Unlike task
unregistration, HTTP completion has a `RestResponse` to inspect, so HTTP spans
keep their response attributes and status. A response that reaches the end hook
without a start hook, and a duplicated end, annotate and end nothing. HTTP
request wrappers and copies share the original request's tracing state, even
when the copy was made before tracing started. Request dispatch and deferred
interceptor continuations activate that context before invoking handlers.

Incoming transport work extracts W3C context from the received headers using a
root context, never the reused worker's ambient context. Outgoing work snapshots
the active context at dispatch. Existing header-map serialization and W3C header
names are unchanged; this migration adds no transport-version fields. Log
correlation continues to use `trace.id`.

Failure classification for component spans unwraps Elasticsearch wrapper
exceptions. A transported `TaskCancelledException` yields `es.outcome=cancelled`,
not `StatusCode.ERROR`.

## Preserved policy and deliberate changes

- Existing settings, defaults, OTLP export, authentication, batching, name
  filters, and sampling configuration remain in place. An empty
  `telemetry.tracing.sanitize_field_names` list disables field-name redaction.
  The tracing-specific sampling setting takes precedence over the legacy
  agent-setting fallback.
- `telemetry.tracing.max_depth=0` retains root-only local recording. Remote
  parents do not consume the local depth budget. A suppressed local span borrows
  its effective parent's identity and does not change distributed sampling.
- Native spans obey the same policy regardless of their instrumentation scope.
  Instrumentation selection and volume safeguards are future design work, not a
  reason to make the API action-specific.
- HTTP instrumentation retains general request/response header capture and
  explicit correlation metadata. An allowlist is a possible future refinement.
- Field-name redaction applies to exported span, event, and link attributes,
  including late mutations and SDK-generated events. Redacted values become the
  string `[REDACTED]`, regardless of their original type. It does not scan arbitrary
  message text for secrets. Raw attributes may exist inside SDK memory before
  export; instrumentation should still avoid collecting sensitive data.
- Exception recording honors the existing stack-capture setting. Disabling it
  avoids generating exception stack strings through the native span API.

HTTP output uses modern attributes only: `http.request.method`,
`http.response.status_code`, the server-side `url.scheme` / `url.path` / `url.query`
attributes, `network.protocol.version`, and
`http.request.header.*` / `http.response.header.*`. Captured headers use string
arrays. Consumers of legacy HTTP fields must migrate. Legacy sanitizer names are
also checked for the renamed fields so that schema migration does not silently
remove privacy protection. Legacy `http.url` patterns protect the extracted path
and query; a relative request target is not relabeled as an absolute `url.full`.
HTTP header matching normalizes casing so selectors written for original-case
response headers remain effective after canonicalization. Other attribute names
retain their existing case-sensitive matching behavior.

## Validation

Tests use a real SDK and an in-memory exporter. They cover non-action native
instrumentation, context isolation and detachment, cached tracer handles,
late/event/link redaction, HTTP lifecycle, local suppression versus sampling,
TCP proxy propagation, transported cancellation, async SQL/EQL/search failures,
and scheduled enrich execution. Existing task, thread-context, HTTP, master
service, and deduplication tests remain part of the regression suite.

Run strict scope diagnostics separately:

```sh
./gradlew :modules:apm:test --tests '*NativeTracingStrictContextTests' \
  '-Dtests.jvm.argline=-Dio.opentelemetry.context.enableStrictContext=true'
```

This opt-in suite disables entitlements only for the diagnostic run: the SDK's
strict checker starts a JVM-lifetime watchdog that triggers `manage_threads` in
the server-owned context library. Normal native tracing tests retain entitlement
coverage. No production entitlement is added for the diagnostic watchdog.

SDK shutdown belongs to node lifecycle. Stopping performs a bounded best-effort
flush without closing the SDK while other node components may still emit spans;
closing the tracing service releases SDK resources.
