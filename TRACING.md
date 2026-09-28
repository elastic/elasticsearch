# Tracing in Elasticsearch

Elasticsearch is instrumented using the [OpenTelemetry][otel] API, which allows
ES developers to gather traces and analyze what Elasticsearch is doing.

## How is tracing implemented?

Instrumentation uses the native OpenTelemetry API supplied by
`TelemetryProvider.getOpenTelemetry()`. The [apm](./modules/apm) module owns the
OpenTelemetry SDK and exports spans using OTLP/gRPC. Core server code depends
only on the OTel API and context libraries. See the [native tracing guide](./docs/internal/Tracing.md)
for context, lifecycle, and privacy details.

## How is tracing configured?

You must supply configuration and credentials for the APM server (see below).
In your `elasticsearch.yml` add the following configuration:

```
telemetry.tracing.enabled: true
telemetry.export.endpoint: https://<your-otlp-grpc-endpoint>:443
```

When using a secret token to authenticate with the APM server, you must add it to the Elasticsearch keystore under `telemetry.secret_token`. For example, execute:

    bin/elasticsearch-keystore add telemetry.secret_token

then enter the token when prompted. If you are using API keys, change the keystore key name to `telemetry.api_key`.

`telemetry.agent.server_url` remains a fallback for the OTLP endpoint.
`telemetry.tracing.sample_rate` defaults to the legacy
`telemetry.agent.transaction_sample_rate` when set, otherwise `0.001`.
`telemetry.tracing.max_depth` defaults to `0`, so only root spans are exported
unless a greater depth is configured. The enable flag, max depth, name filters,
and field-name redaction can be updated through cluster settings; the SDK's
sampling rate and exporter configuration are node settings.

## Where is tracing data sent?

The endpoint must accept OTLP/gRPC traces. For example, an APM intake endpoint
in [Elastic Cloud](https://www.elastic.co/cloud/) can receive them.

## What do we trace?

We primarily trace "tasks". The tasks framework in Elasticsearch allows work to
be scheduled for execution, cancelled, executed in a different thread pool, and
so on. Tracing a task results in a "span", which represents the execution of the
task in the tracing system. We also instrument REST requests, which are not (at
present) modelled by tasks.

A span can be associated with a parent span, which allows all spans in, for
example, a REST request to be grouped together. Spans can track work across
different Elasticsearch nodes.

Elasticsearch also supports distributed tracing via [W3c Trace Context][w3c]
headers. If clients of Elasticsearch send these headers with their requests,
then that data will be forwarded to the APM server in order to yield a trace
across systems.

In rare circumstances, it is possible to avoid tracing a task using
`TaskManager#register(String,String,TaskAwareRequest,boolean)`. For example,
Machine Learning uses tasks to record which models are loaded on each node. Such
tasks are long-lived and are not suitable candidates for APM tracing.

## Thread contexts and nested spans

`ThreadContext` preserves native OTel context across Elasticsearch executors
and context-preserving listeners. A scope must be closed on the thread that
opened it; an asynchronous span can be ended on a different thread when the
operation completes. Use `ThreadContext.clearTraceContext()` or an empty context
when background work must be detached from a request. Incoming REST and
transport requests extract W3C context from their wire headers.

## How do I trace something that isn't a task?

Use `telemetryProvider.getOpenTelemetry().getTracer("elasticsearch.component")`
to create a span. Activate it with `span.makeCurrent()` while scheduling or
performing its work, set relevant attributes, and end it at completion. For
example:

```java
var tracer = telemetryProvider.getOpenTelemetry().getTracer("elasticsearch.component");
var span = tracer.spanBuilder("component.operation").startSpan();
try (var scope = span.makeCurrent()) {
    operation.run();
} catch (Exception failure) {
    TracingContext.recordFailure(span, failure);
    throw failure;
} finally {
    span.end();
}
```

For asynchronous operations, capture the execution context before handing work
to another thread and keep span ownership with the operation, not its scope.
The [native tracing guide](./docs/internal/Tracing.md) covers this pattern.

## What additional attributes should I set?

That's up to you. Be careful not to capture anything that could leak sensitive
or personal information.

[otel]: https://opentelemetry.io/
[w3c]: https://www.w3.org/TR/trace-context/
