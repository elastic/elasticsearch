# Tracing in Elasticsearch

This document briefly describes the set of tracing APIs used in Elasticsearch,
and specifies the behavior of the APM implementation located in this module.

A few essential pieces of information:
- A trace consists of one or more spans, it's a tree of spans;
- A span represents an operation that has a name, a start time and duration,
  key-value attributes describing it, and may record one or more exceptions;
- Traces are distributed: they include spans from one or more services, one or
  more ES nodes;
- Trace context is propagated across services/nodes through HTTP/transport
  headers `traceparent` and `tracestate` (W3C standard);
- ES uses OpenTelemetry to record spans.

For more detailed explanations see the
[OpenTelemetry docs](https://opentelemetry.io/docs/concepts/observability-primer/).

## API

You can interact with tracing in ES through the [Tracer] interface, which you can
get from the [TelemetryProvider]. [Span]s are constructed through the [SpanBuilder]
interface, for example:

```java
Tracer tracer = // ...
Span span = tracer.spanBuilder("example span")
        .setAttribute("test", 42L)
        .startSpan();

try (var ignored = span.storeInThreadContext()) {
    // processing covered by the span
} catch (Exception e) {
    span.recordException(e);
    // react to exception
} finally {
    span.end();
}
```

Span names passed to the `spanBuilder()` method must be low-cardinality names.
When storing attributes on a span, take care to avoid passing sensitive
information.

The explicit `span.storeInThreadContext()` method stores the span as current in
the `ThreadContext` and propagates its context into the headers -- this way
whenever a child span is created, it can implicitly find its parent span in the
`ThreadContext` and continue its trace.

`Span`'s lifetime is separate from its presence in the `ThreadContext`, it has
to be ended separately by calling `end()`.

It is possible to set the span parent explicitly:

```java
Span parentSpan = // ...
Span childSpan = tracer.spanBuilder("child span")
        .setParent(parentSpan)
        .startSpan();
```

Or explicitly start a new trace by setting no parent:

```java
Span rootSpan = tracer.spanBuilder("root span")
        .setNoParent()
        .startSpan();
```

Explicit parent setting overrides the implicit one, i.e. the `ThreadContext`
inheritance.

Instrumentations may (but are not required to) use the `Span.isRecording()`
method to check whether the `Span` they just built and started actually records
anything -- for example, to skip expensive computation of span attributes:

```java
Span span = tracer.spanBuilder("example span").startSpan();
if (span.isRecording()) {
    span.setAttribute("expensive", expensiveComputation());
}
```

A span that does not record is not necessarily a dropped trace: it still can
be propagated over `ThreadContext` and the wire. It carries the trace context,
including the sampling decision, all of which needs to be propagated to remote
nodes (one exception to that is disabled tracing: nothing is propagated in that
case). Other than that, they are effectively no-op.

The `Tracer` has a `currentSpan()` method that returns the current span located
in `ThreadContext`:

```java
Tracer tracer = // ...
Span span = tracer.spanBuilder("example span")
        .setAttribute("test", 42L)
        .startSpan();

try (var ignored = span.storeInThreadContext()) {
    assert tracer.currentSpan() == span;
    // ...
}

tracer.currentSpan(); // not `span` anymore
```

It's a contextual method that may return different span instances depending on
the `ThreadContext` state during the call: instrumentations must be very
careful with this method, and only use it if passing state (`Span` instance)
is not possible in any other way.

## APM tracing implementation spec

Spans are recorded with the OpenTelemetry SDK and exported over OTLP. The export
settings (`telemetry.export.*`) are shared with metrics and logs; nothing is
exported without `telemetry.export.endpoint`.

Four mechanisms decide what happens to a span: [disabled tracing](#disabled-tracing),
[span filtering](#span-filtering), [sampling](#sampling), and
[context propagation](#context-propagation). Filtering is a decision of the node; sampling is a
decision of the whole trace, and the only one that is propagated. Disabling
stops the propagation (and exporting of spans), filtering does not.

### Settings

| Setting                                     | Code               | Dynamic | Description                                                                           |
|---------------------------------------------|--------------------|---------|---------------------------------------------------------------------------------------|
| `telemetry.tracing.enabled`                 | [APMAgentSettings] | Y       | Turns tracing on or off.                                                              |
| `telemetry.tracing.sample_rate`             | [OtelSdkSettings]  | N       | Fraction of the traces that start on this node and are sampled.                       |
| `telemetry.tracing.max_depth`               | [OtelSdkSettings]  | Y       | Maximum depth of the local spans under the local root.                                |
| `telemetry.tracing.max_spans`               | [OtelSdkSettings]  | Y       | Maximum number of local spans under a local root, root included.                      |
| `telemetry.tracing.names.include`           | [APMAgentSettings] | Y       | Wildcard patterns of the names of the spans to record; empty means all.               |
| `telemetry.tracing.names.exclude`           | [APMAgentSettings] | Y       | Wildcard patterns of the names of the spans not to record; wins over `include`.       |
| `telemetry.tracing.sanitize_field_names`    | [APMAgentSettings] | Y       | Attributes with matching keys have their value replaced with `[REDACTED]`.            |
| `telemetry.tracing.record_exception_stacks` | [OtelSdkSettings]  | Y       | Whether `recordException` attaches the stack trace, or only the type and the message. |
| `telemetry.tracing.max_queue_size`          | [OtelSdkSettings]  | N       | Spans buffered for export before they are dropped.                                    |
| `telemetry.tracing.max_batch_size`          | [OtelSdkSettings]  | N       | Spans per export request; at most `max_queue_size`.                                   |

### Context propagation

- **In a node**: the current span is kept in the `ThreadContext` (not serialized).
  `Span#storeInThreadContext()` sets it; it follows the context across threads,
  unless explicitly broken (by e.g. `.setNoParent()` or choosing to run with an
  empty context).
- **Across nodes**, only the `traceparent` and `tracestate` headers travel.
  `Span#storeInThreadContext()` writes them from the span's context, only if
  tracing is enabled. Incoming request headers are copied into the
  `ThreadContext` and become the remote parent.
- **The parent of a span** is the first of: the span given to `setParent`; none, if
  `setNoParent` was called; the current local span in `ThreadContext`;
  the remote parent from the `ThreadContext` headers; none (a new trace).
- **A local root** of the trace is the first span on the node: one without
  a parent, or with a remote parent. Depth and span count are tracked per local
  trace.

### Disabled tracing

`telemetry.tracing.enabled = false` means that no trace data is produced and
nothing is propagated to other nodes. It applies to the spans started after the
change.

- The span is not recorded, and the OTel SDK is not used (it is not even
  created, if tracing was disabled from the start).
- The span is read-only, and holds the context of its parent (the given one, the
  current one, or the remote one; none if there is no parent).
- `storeInThreadContext()` removes `traceparent` and `tracestate` and does not
  propagate anything either through `ThreadContext` or to remote nodes (other
  nodes in the cluster most likely have turned off tracing as well).
- Spans that were started earlier keep recording and are exported, but they are
  not propagated either.
- Once tracing is enabled again, a span started under a span that was started
  while disabled continues the trace of that span's context; if it has none, it
  starts a new trace.

### Span filtering

Filtering is the local limit of what this node records: the names, the depth
and the number of the local spans. It does not drop the trace on other nodes.

The checks run for every new span, in this order, and the first one that fails
drops the span: tracing is enabled, the parent is sampled, the name is
accepted by `names.include` / `names.exclude`, the depth is within `max_depth`,
the count is within `max_spans`.

- A filtered out span is not recorded and is read-only. It holds the
  context of its parent, so what it leaves behind is the parent: the parent's
  context is stored and propagated as it is, **sampled**. Remote nodes keep
  recording, with their own limits and their own counts.
- Spans started under a filtered out span have the parent of the filtered out
  span as their parent.
- Depth is the number of local ancestors: `0` records only the local root. Spans
  of other nodes are not counted.
- The count is shared by all the spans under a local root; the root is one of
  them, so `max_spans = 0` drops the root. Spans filtered out by name or trace
  depth are not counted.
- A local root that is filtered out has no parent context to leave: nothing is
  propagated, unless there is a remote parent, which passes through unchanged.
  The spans that follow start their own local traces.

### Sampling

Sampling is the decision of the whole trace, made once, by the SDK sampler
([ElasticTracestateSampler]), when a span that passed filtering is started.

- It is a parent-based ratio sampler: a span without a parent is sampled
  with probability `sample_rate`, by the trace id; a span with a sampled parent
  (local or remote) is sampled; a span with a parent that is not sampled is not.
- A span that is not sampled is not recorded, but its context is valid and may
  be propagated. It is stored and propagated as `traceparent` with the sampled
  flag off, so that every span under it, on every node, is not recorded.
- A sampled span gets `ot=p:<n>` in `tracestate`, from the rate, or from the
  `es=s:<rate>` of the parent, so that APM Server can compute the representative
  count of the span.

[TelemetryProvider]: ../../server/src/main/java/org/elasticsearch/telemetry/TelemetryProvider.java
[Tracer]: ../../server/src/main/java/org/elasticsearch/telemetry/tracing/Tracer.java
[SpanBuilder]: ../../server/src/main/java/org/elasticsearch/telemetry/tracing/SpanBuilder.java
[Span]: ../../server/src/main/java/org/elasticsearch/telemetry/tracing/Span.java
[ElasticTracestateSampler]: src/main/java/org/elasticsearch/telemetry/apm/internal/export/otelsdk/ElasticTracestateSampler.java
[APMAgentSettings]: src/main/java/org/elasticsearch/telemetry/apm/internal/APMAgentSettings.java
[OtelSdkSettings]: src/main/java/org/elasticsearch/telemetry/apm/internal/export/otelsdk/OtelSdkSettings.java
