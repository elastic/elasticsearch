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

**Not supported yet**

:::{table}
:widths: 1-4-3-3-1

| # | Construct | Example | Workaround | Issue |
|---|---|---|---|---|
| 62 | Exemplar queries | `GET /api/v1/query_exemplars` | In Grafana, remove the links under **Data sources → Elasticsearch → Exemplars** | [#154786]({{es-issue}}154786) |
:::
