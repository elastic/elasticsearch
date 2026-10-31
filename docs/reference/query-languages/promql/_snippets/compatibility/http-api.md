% Section of prometheus-compatibility.md, also included in promql-http-api.md.
% Headings follow https://prometheus.io/docs/prometheus/latest/querying/api/.
% Rows are numbered across all snippets in _snippets/compatibility/. A row without a version badge applies to every
% version. Columns: # | Construct | Example | Elasticsearch | Prometheus | Issue.

Sections follow [HTTP API](https://prometheus.io/docs/prometheus/latest/querying/api/) in the Prometheus documentation.
They apply only to the [Prometheus-compatible HTTP API](/reference/query-languages/promql/promql-http-api.md).

### Format overview [prometheus-compatibility-format-overview]

:::{table}
:widths: 1-3-2-3-2-1

| # | Construct | Example | {{es}} | Prometheus | Issue |
|---|---|---|---|---|---|
| 55 | Form-encoded `POST` | `Content-Type: application/x-www-form-urlencoded` | **`406 Not Acceptable`**. {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` Accepted when [security](/reference/elasticsearch/configuration-reference/security-settings.md) is enabled, [`xpack.security.http.ssl.enabled`](/reference/elasticsearch/configuration-reference/security-settings.md) is `true`, and the request is authenticated. TLS must terminate at {{es}}. Otherwise, use `GET` | Accepted | - |
| 56 | `warnings` and `infos` | `quantile(2, x)` | **Not returned**. The only warning is `results truncated due to limit` | Returned | - |
:::

### Expression queries [prometheus-compatibility-expression-queries]

:::{table}
:widths: 1-3-2-3-2-1

| # | Construct | Example | {{es}} | Prometheus | Issue |
|---|---|---|---|---|---|
| 57 | Query parameters that the [PromQL HTTP API](/reference/query-languages/promql/promql-http-api.md) doesn't list | `lookback_delta=1m`, `stats=all` | **`400 Bad Request`**. Configure clients to omit them | Accepted | [#149939](https://github.com/elastic/elasticsearch/issues/149939) |
:::

### Querying metric metadata [prometheus-compatibility-querying-metric-metadata]

:::{table}
:widths: 1-3-2-3-2-1

| # | Construct | Example | {{es}} | Prometheus | Issue |
|---|---|---|---|---|---|
| 58 | `help` in [`/api/v1/metadata`](/reference/query-languages/promql/promql-http-api.md#promql-http-api-metadata-endpoint) {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` | `GET /api/v1/metadata` | **Empty** | The metric's `HELP` text | [#157230](https://github.com/elastic/elasticsearch/issues/157230) |
:::

### Querying exemplars [prometheus-compatibility-querying-exemplars]

:::{table}
:widths: 1-3-2-3-2-1

| # | Construct | Example | {{es}} | Prometheus | Issue |
|---|---|---|---|---|---|
| 59 | Exemplar queries | `GET /api/v1/query_exemplars` | **Unsupported**. In Grafana, remove the links under **Data sources** > **Elasticsearch** > **Exemplars** | Returns exemplars | [#154786](https://github.com/elastic/elasticsearch/issues/154786) |
:::
