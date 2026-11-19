# esql-usage-reporting

Reports the CPU time of ES|QL queries over external sources (Data Federation) to the Elastic Usage API, so that it can be metered
on hosted (ECH) deployments. Serverless has its own collector; this plugin is `STATEFUL_ONLY` and is not loaded when stateless is
enabled, because two meters would count the same queries twice.

## Activation

Disabled by default: with default settings a node behaves exactly as without the plugin. Cloud enables it with
`xpack.esql.usage_reporting.enabled: true` and the settings below. If it is enabled but `url` is missing or is not an absolute
`https` URL without credentials, an identity value is missing, or (for an `https` url) there is no client certificate (`ssl.certificate` and `ssl.key`,
or `ssl.keystore.path`), the node still starts, logs one ERROR listing the problems, and the plugin stays inactive: a node that does
not start for a reporting reason is worse than a node that does not report. Snapshot builds also accept an `http` url, for tests.
An invalid value of a setting that only tests change (for example a period that does not divide an hour) fails the startup.

## Settings

All node scoped and not dynamic. Names and defaults are tentative.

| Setting (prefix `xpack.esql.usage_reporting.`) | Default | Meaning |
|---|---|---|
| `enabled` | `false` | master switch |
| `url` | none | Usage API endpoint, required when enabled |
| `period` | `5m` | reporting interval, must divide one hour; also the `period_seconds` of each record |
| `batch_size` | `100` | records per request, 1 to 1000 |
| `request_timeout` | `30s` | timeout of one request |
| `pending_max` | `1000` | bound of the records built but not yet accepted |
| `report_exempt` | `false` | also send non-success outcomes, as exempt records that are not counted |
| `deployment_id`, `cluster_id`, `provider`, `region` | none | identity injected by Cloud, required when enabled |
| `ssl.*` | none | client certificate profile for the mutual TLS connection (an `SslProfileExtension`) |

## Status

This is the skeleton: it creates the plugin, the settings, the listener and the interfaces of the collector, each with a no-op or
simple implementation. Nothing is reported yet. The package javadoc describes the flow. Each following change replaces one factory
method of `UsageComponents`:

| Piece | Added by |
|---|---|
| `UsageAccumulator` (atomic sums per outcome) | the accumulator change |
| `UsageReporter` (aligned schedule, single flight, final flush) | the reporter change |
| `UsageRecordBuilder`, `UsageRecordJson` (record ids, quantity in milliseconds, wire format) | the record change |
| `PendingStore` (bounded, oldest dropped first) | the store change |
| `UsagePublisher` (HTTP over mutual TLS, failure classes, bad record isolation) | the publisher change |
| fake Usage API and integration tests | the fake API change |
