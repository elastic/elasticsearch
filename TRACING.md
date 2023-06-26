# Tracing in Elasticsearch

Elasticsearch is instrumented using the [OpenTelemetry][otel] API, which allows
us to gather traces and analyze what Elasticsearch is doing.


## How is tracing implemented?

The Elasticsearch server code contains a [`tracing`][tracing] package, which is
an abstraction over the OpenTelemetry API. All locations in the code that
perform instrumentation and tracing must use these abstractions.

Separately, there is the [`apm`](./modules/apm/) module, which works with the
OpenTelemetry API directly to record trace data.  Underneath the OTel API, we
use Elastic's [APM agent for Java][agent], which attaches at runtime to the
Elasticsearch JVM and removes the need for Elasticsearch to hard-code the use of
an OTel implementation. Note that while it is possible to programmatically start
the APM agent, the Security Manager permissions required make this essentially
impossible.


## How is tracing configured?

You must supply configuration and credentials for the APM server (see below).
You must also set `tracing.apm.enabled` to `true`, but this can be toggled at
runtime.

All APM settings live under `tracing.apm`. All settings related to the Java agent
go under `tracing.apm.agent`. Anything you set under there will be propagated to
the agent.

For agent settings that can be changed dynamically, you can use the cluster
settings REST API. For example, to change the sampling rate:

    curl -XPUT \
      -H "Content-type: application/json" \
      -u "$USERNAME:$PASSWORD" \
      -d '{ "persistent": { "tracing.apm.agent.transaction_sample_rate": "0.75" } }' \
      https://localhost:9200/_cluster/settings


### More details about configuration

For context, the APM agent pulls configuration from [multiple
sources][agent-config], with a hierarchy that means, for example, that options
set in the config file cannot be overridden via system properties.

Now, in order to send tracing data to the APM server, ES needs to be configured with
either a `secret_key` or an `api_key`. We could configure these in the agent via
system properties, but then their values would be available to any Java code in
Elasticsearch that can read system properties.

Instead, when Elasticsearch bootstraps itself, it compiles all APM settings
together, including any `secret_key` or `api_key` values from the ES keystore,
and writes out a temporary APM config file containing all static configuration
(i.e. values that cannot change after the agent starts).  This file is deleted
as soon as possible after ES starts up. Settings that are not sensitive and can
be changed dynamically are configured via system properties. Calls to the ES
settings REST API are translated into system property writes, which the agent
later picks up and applies.

## Where is tracing data sent?

You need to have an APM server running somewhere. For example, you can create a
deployment in [Elastic Cloud](https://www.elastic.co/cloud/) with Elastic's APM
integration.

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

When a span is started, Elasticsearch tracks information about that span in the
current [thread context][thread-context].  If a new thread context is created,
then the current span information must not propagated but instead renamed, so
that (1) it doesn't interfere when new trace information is set in the context,
and (2) the previous trace information is available to establish a parent /
child span relationship.  This is done with `ThreadContext#newTraceContext()`.

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
tracer when a span should start and end. You'll also need to manage the creation
of new trace contexts when child spans need to be created.

## What additional attributes should I set?

That's up to you. Be careful not to capture anything that could leak sensitive
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
cannot be guaranteed when using tasks, making scope largely useless to
Elasticsearch.

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
[config]: ./modules/apm/src/main/config/elasticapm.properties
[agent-config]: https://www.elastic.co/guide/en/apm/agent/java/master/configuration.html
[agent]: https://www.elastic.co/guide/en/apm/agent/java/current/index.html
