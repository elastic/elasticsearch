% Section of prometheus-compatibility.md, also included in promql-http-api.md.
% Headings follow https://prometheus.io/docs/prometheus/latest/querying/api/. Rows are numbered across all snippets
% in _snippets/compatibility/. A row without a version badge applies to every version.

Sections follow [HTTP API](https://prometheus.io/docs/prometheus/latest/querying/api/) in the Prometheus documentation.
They apply only to the [Prometheus-compatible HTTP API](/reference/query-languages/promql/promql-http-api.md).

### Format overview [prometheus-compatibility-format-overview]

:::{table}
:widths: 1-4-3-3-1

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 60 | Form-encoded `POST` (`application/x-www-form-urlencoded`) | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` Accepted only for authenticated requests, with [security](/reference/elasticsearch/configuration-reference/security-settings.md) enabled and [`xpack.security.http.ssl.enabled`](/reference/elasticsearch/configuration-reference/security-settings.md) set to `true`. TLS must terminate at {{es}}. Otherwise `406 Not Acceptable`. Use `GET` instead | Accepted | - |
| 61 | `warnings` and `infos`, such as for a quantile outside 0 to 1 | Not returned. The only warning is `results truncated due to limit` | Returned | - |
:::

### Expression queries [prometheus-compatibility-expression-queries]

:::{table}
:widths: 1-4-3-3-1

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 62 | Query parameters that the [PromQL HTTP API](/reference/query-languages/promql/promql-http-api.md) doesn't list, such as `lookback_delta` and `stats` | `400 Bad Request`. Configure clients to omit them | Accepted | [#149939](https://github.com/elastic/elasticsearch/issues/149939) |
:::

### Querying metric metadata [prometheus-compatibility-querying-metric-metadata]

:::{table}
:widths: 1-4-3-3-1

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 63 | `help` in [`/api/v1/metadata`](/reference/query-languages/promql/promql-http-api.md#promql-http-api-metadata-endpoint) | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` Always empty | The metric's `HELP` text | [#157230](https://github.com/elastic/elasticsearch/issues/157230) |
:::

### Querying exemplars [prometheus-compatibility-querying-exemplars]

:::{table}
:widths: 1-4-3-3-1

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 64 | `/api/v1/query_exemplars` | Unsupported. In Grafana, remove the links under **Data sources** > **Elasticsearch** > **Exemplars** | Returns exemplars | [#154786](https://github.com/elastic/elasticsearch/issues/154786) |
:::
