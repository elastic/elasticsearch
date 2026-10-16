---
navigation_title: "Telemetry"
applies_to:
  deployment:
    self: ga 9.6
  serverless: unavailable
---

# Telemetry settings in {{es}}

{{es}} can export metrics and traces about its own operation to an OTLP/gRPC endpoint, such as [APM Server](docs-content://solutions/observability/apm/apm-server/index.md), using the OpenTelemetry SDK built into {{es}}. The `telemetry.agent.*` settings of earlier versions are [deprecated](#telemetry-agent-settings).

::::{warning}
Telemetry export exists mainly for Elastic's own observability of {{es}}. Settings, exported data, and defaults can change between versions without notice, and are not covered by {{es}} backward-compatibility guarantees.
::::

For example, to export metrics and traces to an APM Server:

```yaml
telemetry.metrics.enabled: true
telemetry.tracing.enabled: true
telemetry.export.endpoint: https://my-apm-server:8200
```

Then add the APM Server secret token to the [{{es}} keystore](docs-content://deploy-manage/security/secure-settings.md):

```sh
bin/elasticsearch-keystore add telemetry.secret_token
```

## Export settings [telemetry-export-settings]

`telemetry.export.endpoint`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) URL of the endpoint that receives metrics and traces, in the form `http(s)://host:port` without a path. If it is not set, {{es}} logs a warning and does not export.

`telemetry.export.verify_server_cert`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Set to `false` to skip TLS certificate verification of the endpoint. Defaults to `true`.

`telemetry.export.interval`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) How often metrics are exported, and how long spans are batched before export. Must be greater than `telemetry.export.send_timeout`. Defaults to `60s`.

`telemetry.export.send_timeout`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Total time allowed for one export request, including retries. Accepts `100ms` to `60s`. Defaults to `5s`.

`telemetry.export.connect_timeout`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Timeout for opening a connection to the endpoint. Accepts `1ms` to `10s`. Defaults to `2s`.

## Authentication settings [telemetry-authentication-settings]

If both are set, `telemetry.api_key` is used.

`telemetry.secret_token`
:   ([Secure](docs-content://deploy-manage/security/secure-settings.md)) Secret token sent to the endpoint as a bearer token.

`telemetry.api_key`
:   ([Secure](docs-content://deploy-manage/security/secure-settings.md)) API key sent to the endpoint.

## Metrics settings [telemetry-metrics-settings]

`telemetry.metrics.enabled`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Set to `true` to export metrics. Defaults to `false`.

`telemetry.metrics.disabled`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Metric names that are not exported. Names can contain the wildcards `*` and `?`.

`telemetry.metrics.buffer.disk_size`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Maximum disk space used to buffer metrics while the endpoint is unreachable. Must be less than `2gb`. Set to `0b` to turn off buffering. Defaults to `512mb`.

`telemetry.metrics.buffer.ttl`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Buffered metrics older than this are dropped. Defaults to `48h`.

`telemetry.metrics.instrument_timing.enabled`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Set to `true` to also export how long each asynchronous metric takes to collect. Defaults to `false`.

## Tracing settings [telemetry-tracing-settings]

`telemetry.tracing.enabled`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Set to `true` to export traces. Defaults to `false`.

`telemetry.tracing.sample_rate`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Fraction of traces started by {{es}} that are exported, from `0.0` to `1.0`. Traces that arrive with a `traceparent` header follow the caller's sampling decision. Defaults to `0.001`.

`telemetry.tracing.max_depth`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Maximum depth of child spans that each node exports below the first span it creates for a request. `0` exports only that span. Defaults to `0`.

`telemetry.tracing.max_queue_size`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Maximum number of spans buffered for export. Spans beyond this limit are dropped. Defaults to `1024`.

`telemetry.tracing.max_batch_size`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Maximum number of spans sent in one export request. Values above `telemetry.tracing.max_queue_size` are lowered to it. Defaults to `512`.

`telemetry.tracing.names.include`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Span names to export. Names can contain the wildcard `*`. If empty, all spans are exported.

`telemetry.tracing.names.exclude`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Span names that are not exported. Names can contain the wildcard `*`. Takes precedence over `telemetry.tracing.names.include`.

`telemetry.tracing.sanitize_field_names`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Span attribute names whose values are replaced with `[REDACTED]`. Names can contain the wildcard `*`. Defaults to `password`, `passwd`, `pwd`, `secret`, `*key`, `*token*`, `*session*`, `*credit*`, `*card*`, `*auth*`, `*principal*`, `set-cookie`.

`telemetry.tracing.record_exception_stacks`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Set to `true` to attach full stack traces to exceptions recorded on spans. When `false`, only the exception type and message are recorded. Defaults to `false`.

## Resource settings [telemetry-resource-settings]

`telemetry.resource.*`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Resource attributes attached to every exported metric and span. For example, `telemetry.resource.deployment.environment: production` sets `deployment.environment`. They override the attributes {{es}} detects. By default, `service.name` is `self-managed-elasticsearch` and `service.instance.id` is the [node name](/reference/elasticsearch/configuration-reference/node-settings.md).

## Deprecated `telemetry.agent.*` settings [telemetry-agent-settings]
```{applies_to}
stack: deprecated 9.6
```

The `telemetry.agent.*` settings configured the Elastic APM Java agent, which {{es}} no longer includes. Nodes still start with them, and the [deprecation info API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-migration-deprecations) lists the ones in use.

Replace each setting with its replacement from the following table. Until you do, {{es}} keeps applying the deprecated setting, unless its notes say it is ignored. Any other `telemetry.agent.*` setting has no effect and can be removed. To remove a setting that was set with the cluster update settings API, set it to `null`.

| Deprecated setting | Replacement | Notes |
| --- | --- | --- |
| `telemetry.agent.server_url` | `telemetry.export.endpoint` | |
| `telemetry.agent.server_urls` | `telemetry.export.endpoint` | Applied only if it holds a single URL. |
| `telemetry.agent.verify_server_cert` | `telemetry.export.verify_server_cert` | |
| `telemetry.agent.metrics_interval` | `telemetry.export.interval` | |
| `telemetry.agent.server_timeout` | `telemetry.export.send_timeout` | Ignored. The replacement includes retries. |
| `telemetry.agent.transaction_sample_rate` | `telemetry.tracing.sample_rate` | The replacement is static. |
| `telemetry.agent.max_queue_size` | `telemetry.tracing.max_queue_size` | |
| `telemetry.agent.transaction_max_spans` | `telemetry.tracing.max_depth` | Ignored. The replacement limits the depth of child spans, not their number. |
| `telemetry.agent.disable_metrics` | `telemetry.metrics.disabled` | Ignored. The replacement matches {{es}} metric names. |
| `telemetry.agent.sanitize_field_names` | `telemetry.tracing.sanitize_field_names` | Ignored. |
| `telemetry.agent.service_name` | `telemetry.resource.service.name` | |
| `telemetry.agent.service_node_name` | `telemetry.resource.service.instance.id` | |
| `telemetry.agent.hostname` | `telemetry.resource.host.name` | |
| `telemetry.agent.environment` | `telemetry.resource.deployment.environment` | |
| `telemetry.agent.global_labels.<name>` | `telemetry.resource.<name>` | |

Versions before 9.6 use the Elastic APM Java agent, so keep the `telemetry.agent.*` settings until you upgrade.
