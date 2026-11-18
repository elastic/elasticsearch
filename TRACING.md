# Tracing in Elasticsearch

The tracing API and implementation is described in detail in [the APM module
docs](modules/apm/TRACING.md).

## How is tracing configured?

You must supply configuration and credentials for the APM server (see below).
In your `elasticsearch.yml` add the following configuration:

```
telemetry.tracing.enabled: true
telemetry.export.endpoint: https://<your-apm-server>:443
```

When using a secret token to authenticate with the APM server, you must add it to the Elasticsearch keystore under `telemetry.secret_token`. For example, execute:

    bin/elasticsearch-keystore add telemetry.secret_token

then enter the token when prompted. If you are using API keys, change the keystore key name to `telemetry.api_key`.

APM settings are described in detail [here](modules/apm/TRACING.md#settings).

For settings that can be changed dynamically, you can use the cluster
settings REST API. For example, to change the maximum span depth:

    curl -XPUT \
      -H "Content-type: application/json" \
      -u "$USERNAME:$PASSWORD" \
      -d '{ "persistent": { "telemetry.tracing.max_depth": 2 } }' \
      https://localhost:9200/_cluster/settings


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

## How to I trace something that isn't a task?

See [tracing API](modules/apm/TRACING.md#api).


[w3c]: https://www.w3.org/TR/trace-context/
