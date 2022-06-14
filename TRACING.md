# Tracing in Elasticsearch

Elasticsearch is instrumented using the [OpenTelemetry][otel] API, which allows
us to gather traces and analyze what Elasticsearch is doing.

## How is tracing implemented?

The Elasticsearch server code contains a [`tracing`][tracing] package, which is
an abstraction over the OpenTelemetry API. All locations in the code that
perform instrumentation and tracing must use these abstractions.

Separately, there is the [`apm-integration`](./x-pack/plugins/apm-integration/)
module, which works with the OpenTelemetry API directly to record trace data.
Underneath the OTel API, we use Elastic's [APM agent for Java][agent], which
attaches at runtime to the Elasticsearch JVM and removes the need for
Elasticsearch to hard-code the use of an SDK.

## How is tracing configured?

   * The `xpack.apm.enabled` setting must be set to `true`
   * The APM agent must be both enabled and configured with server credentials.
     See below.

We have a config file in [`config/elasticapm.properties`][config], which
configures settings that are not dynamic, or should not be changed at runtime.
Other settings can be configured at runtime by using the cluster settings API,
and setting `xpack.apm.agent.<key>` with a string value, where `<key>`
is the APM agent key that you want to configure. For example, to change the
sampling rate:

    curl -XPUT \
      -H "Content-type: application/json" \
      -u "$USERNAME:$PASSWORD" \
      -d '{ "persistent": { "xpack.apm.agent.transaction_sample_rate": "0.75" } }' \
      https://localhost:9200/_cluster/settings

### More details about configuration

The APM agent pulls configuration from [multiple sources][agent-config], with a
hierarchy that means, for example, that options set in the config file cannot be
overridden via system properties. This means that Elasticsearch cannot ship with
sensible defaults for dynamic settings in the config file and override them via
system properties.

Instead, static or sensitive config values are put in the config file, and
dynamic settings are left entirely to the system properties. The Elasticsearch
APM plugin has appropriate security access to set the APM-related system
properties. Calls to the ES settings REST API are translated into system
property writes, which the agent later picks up and applies.

## Where is tracing data sent?

You need to have an APM server running somewhere. For example, you can
create a deployment in Elastic Cloud with Elastic's APM integration.

## What do we trace?

We primarily trace "tasks". The tasks framework in Elasticsearch allows work to
scheduled for execution, cancelled, executed in a different thread pool, and so
on. Tracing a task results in a "span", which represents the execution of the
task in the tracing system. We also instrument REST requests, which are not (at
present) modelled by tasks.

A span can be associated with a parent span, which allows all spans in, for
example, a REST request to be grouped together. Spans can track work across
different Elasticsearch nodes.

Elasticsearch also supports distributed tracing via [W3c Trace Context][w3c]
headers. If clients of Elasticsearch send these headers with their requests,
then that data will be forwarded to the APM server in order to yield a trace
across systems.

## Thread contexts and nested spans

When a span is started, Elasticsearch tracks information about that span in the
current [thread context][thread-context].  If a new thread context is created,
then current span information is propagated but renamed, so that (1) it doesn't
interfere when new trace information is set in the context, and (2) the previous
trace information is available to establish a parent / child span relationship.

Sometimes we need to detach new spans from their parent. For example, creating
an index starts some related background tasks, but these shouldn't be associated
with the REST request, otherwise all the background task spans will be
associated with the REST request for as long as Elasticsearch is running.
`ThreadContext` provides the `clearTraceContext`() method for this purpose.

## How to I trace something that isn't a task?

First work out if you can turn it into a task. No, really.

If you can't do that, you'll need to ensure that your class can get access to a
`Tracer` instance (this is available to inject, or you'll need to pass it when
your class is created). Then you need to call the appropriate methods on the
tracer when a span should start and end.

## What additional attributes should I set?

That's up to you. Be careful about capture anything that could leak sensitive
or personal information.

## What is "scope" and when should I used it?

Usually you won't need to.

That said, sometimes you may want more details to be captured about a particular
section of code. You can think of "scope" as representing the currently active
tracing context. Using scope allows the APM agent to do the following:

* Enables automatic correlation between the "active span" and logging, where
  logs have also been captured.
* Enables capturing any exceptions thrown when the span is active, and linking
  those exceptions to the span
* Allows the sampling profiler to be used as it allows samples to be linked to
  the active span (if any), so the agent can automatically get extra spans
  without manual instrumentation.

However, a scope must be closed in the same thread in which it was opened, which
cannot be guaranteed when using tasks.

In the OpenTelemetry documentation, spans, scope and context are fairly
straightforward to use, since `Scope` is an `AutoCloseable` and so can be
easily created and cleaned up use try-with-resources blocks. Unfortunately,
Elasticsearch is a complex piece of software, and also extremely asynchronous,
so the typical OpenTelemetry examples do not work.

Nonetheless, it is possible to manually use scope where we need more detail by
explicitly opening a scope via the `Tracer`.


[otel]: https://opentelemetry.io/
[thread-context]: ./server/src/main/java/org/elasticsearch/common/util/concurrent/ThreadContext.java).
[w3c]: https://www.w3.org/TR/trace-context/
[tracing]: ./server/src/main/java/org/elasticsearch/tracing/
[config]: ./x-pack/plugin/apm-integration/src/main/config/elasticapm.properties
[agent-config]: https://www.elastic.co/guide/en/apm/agent/java/master/configuration.html
[agent]: https://www.elastic.co/guide/en/apm/agent/java/current/index.html
