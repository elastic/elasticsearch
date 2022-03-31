# Tracing in Elasticsearch

Elasticsearch is instrumented using the [OpenTelemetry][otel] API, which allows
us to gather traces and analyze what Elasticsearch is doing.

## How is tracing implemented?

The Elasticsearch server code contains a
[`tracing`](./server/src/main/java/org/elasticsearch/tracing/) package, which is
an abstraction over the OpenTelemetry API. All locations in the code that
performing instrumentation and tracing must use these abstractions.

Separately, there is the [`apm-integration`](./x-pack/plugins/apm-integration/)
module, which works with the OpenTelemetry API directly to manipulate spans.

## Where is tracing data sent?

You need to have an OpenTelemetry server running somewhere. For example, you can
create a deployment in Elastic Cloud, and use Elastic's APM integration.

## How is tracing data sent?

This branch uses the OpenTelemetry SDK, which is a reference implementation of
the API. Work is underway to use the Elastic APM agent for Java, which attaches
at runtime and removes the need for Elasticsearch to hard-code the use of an SDK.

## What do we trace?

We primarily trace "tasks". The tasks framework in Elasticsearch allows work to
scheduled for execution, cancelled, executed in a different thread pool, and so
on. Tracing a task results in a "span", which represents the execution of the
task in the tracing system. We also instrument REST requests, which are not (at
present) modelled by tasks.

A span can be associated with a parent span, which allows all spans in, for
example, a REST request to be grouped together. Spans can track the
Elasticsearch supports the [W3c
Trace Context](https://www.w3.org/TR/trace-context/) headers. It also uses these

## Thread contexts and nested spans

When a span is started, Elasticsearch tracks information about that span in the
current [thread
context](./server/src/main/java/org/elasticsearch/common/util/concurrent/ThreadContext.java).
When a nested span is started, a new thread context is created, and the current
span information is moved so that it becomes the parent span information.

Sometimes we need to detach new spans from their parent. For example, creating
an index starts some related background tasks, but these shouldn't be associated
with the REST request, otherwise all the background task spans will be
associated with the REST request for as long as Elasticsearch is running.

## How to I trace something that isn't a task?

First work out if you can turn it into a task. No, really.

If you can't do that, you'll need to ensure that your class can access the
`Node`'s tracers, then call the appropriate methods on the tracers when a span
should start and end.

## What attributes should I set?

TODO.

[otel]: https://opentelemetry.io/
