% Section of prometheus-compatibility.md, also included in promql-http-api.md.
% Headings follow https://prometheus.io/docs/prometheus/latest/querying/api/.
% Rows are numbered across all snippets in _snippets/compatibility/, in page order. A row without a version badge
% applies to every version. "Not supported yet" columns: # | Construct | Example | Workaround | Issue.
% "Behaves differently" columns: # | Construct | Example | Elasticsearch | Prometheus | Issue.

Sections follow [HTTP API](https://prometheus.io/docs/prometheus/latest/querying/api/) in the {{prom}} documentation.
They apply only to the [{{prom}}-compatible HTTP API](/reference/query-languages/promql/promql-http-api.md).

### Format overview [prometheus-compatibility-format-overview]

**Supported**

`GET` requests, and responses in the {{prom}} JSON format.

**Behaves differently**

:::{table}
:widths: 1-3-2-3-2-1

| # | Construct | Example | {{es}} | {{prom}} | Issue |
|---|---|---|---|---|---|
| 58 | Form-encoded `POST` | `Content-Type: application/x-www-form-urlencoded` | `406 Not Acceptable`.<br>{applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` Accepted when [security](/reference/elasticsearch/configuration-reference/security-settings.md) is enabled, [`xpack.security.http.ssl.enabled`](/reference/elasticsearch/configuration-reference/security-settings.md) is `true`, and the request is authenticated. TLS must terminate at {{es}}. Otherwise, use `GET` | Accepted | - |
| 59 | `warnings` and `infos` | `quantile(2, x)` | Not returned. The only warning is `results truncated due to limit` | Returned | - |
:::

### Expression queries [prometheus-compatibility-expression-queries]

**Supported**

[Instant queries](/reference/query-languages/promql/promql-http-api.md#promql-http-api-query-instant) and [range queries](/reference/query-languages/promql/promql-http-api.md#promql-http-api-query-range).

**Not supported yet**

:::{table}
:widths: 1-4-3-3-1

| # | Construct | Example | Workaround | Issue |
|---|---|---|---|---|
| 60 | Query parameters that the [{{promql}} HTTP API](/reference/query-languages/promql/promql-http-api.md) doesn't list | `lookback_delta=1m`, `stats=all` | Configure clients to omit them. {{es}} returns `400 Bad Request` | [#149939]({{es-issue}}149939) |
:::

### Querying metric metadata [prometheus-compatibility-querying-metric-metadata]

**Supported**

{applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` The type and unit of each metric in [`/api/v1/metadata`](/reference/query-languages/promql/promql-http-api.md#promql-http-api-metadata-endpoint).

**Behaves differently**

:::{table}
:widths: 1-3-2-3-2-1

| # | Construct | Example | {{es}} | {{prom}} | Issue |
|---|---|---|---|---|---|
| 61 | `help` in [`/api/v1/metadata`](/reference/query-languages/promql/promql-http-api.md#promql-http-api-metadata-endpoint) {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` | `GET /api/v1/metadata` | Empty | The metric's `HELP` text | [#157230]({{es-issue}}157230) |
:::

### Querying exemplars [prometheus-compatibility-querying-exemplars]

**Supported**

{applies_to}`stack: preview 9.7` {applies_to}`serverless: preview` [Exemplar queries](/reference/query-languages/promql/promql-http-api.md#promql-http-api-query-exemplars) for the expressions that the query endpoints accept, such as `GET /api/v1/query_exemplars?query=rate(http_requests_total{job="api"}[5m])&start=...&end=...`.

**Behaves differently**

:::{table}
:widths: 1-3-2-3-2-1

| # | Construct | Example | {{es}} | {{prom}} | Issue |
|---|---|---|---|---|---|
| 62 | `start` and `end` of exemplar queries | `GET /api/v1/query_exemplars?query=up` | Required | Optional. Without them, all stored exemplars are considered | - |
| 63 | `offset` in exemplar queries | `rate(m[5m] offset 1h)` | Only the exemplars that influenced the returned metric values and fall into the requested time range. For example, with `offset 1h`, only exemplars older than one hour are returned, so a time range shorter than the offset returns no exemplars | The `offset` is ignored. The exemplars recorded in the requested time range are returned, even though they don't influence the metric values | - |
| 64 | Number of exemplars | `GET /api/v1/query_exemplars` | At most the 10,000 most recent exemplars. Exemplars are stored like other documents and follow the retention of their data stream | All exemplars still held in the fixed-size in-memory exemplar storage | - |
:::
