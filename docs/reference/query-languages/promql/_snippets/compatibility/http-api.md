% Section of prometheus-compatibility.md, also included in promql-http-api.md.
% Headings follow https://prometheus.io/docs/prometheus/latest/querying/api/. Rows are numbered across all snippets
% in _snippets/compatibility/. A row without a version badge applies to every version.

These rows follow [HTTP API](https://prometheus.io/docs/prometheus/latest/querying/api/) in the Prometheus documentation.
They apply to the [Prometheus-compatible HTTP API](/reference/query-languages/promql/promql-http-api.md) only.

### Format overview [prometheus-compatibility-format-overview]

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 72 | Form-encoded POST: parameters in an `application/x-www-form-urlencoded` body | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` Accepted only when [security](/reference/elasticsearch/configuration-reference/security-settings.md) is enabled, [`xpack.security.http.ssl.enabled`](/reference/elasticsearch/configuration-reference/security-settings.md) is `true` on the {{es}} HTTP interface, and the request is authenticated. TLS that terminates before {{es}} doesn't count. Otherwise `406 Not Acceptable`. Use `GET` with query-string parameters instead | Accepted | - |
| 73 | `warnings` and `infos` annotations, for example for a quantile outside the range 0 to 1 | Not returned. The only warning is `results truncated due to limit` | Returned | - |

### Expression queries [prometheus-compatibility-expression-queries]

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 74 | Query parameters that the [PromQL HTTP API](/reference/query-languages/promql/promql-http-api.md) doesn't document, such as `lookback_delta` and `stats` | `400 Bad Request`. Configure clients to omit them | Accepted | [#149939](https://github.com/elastic/elasticsearch/issues/149939) |

### Querying metric metadata [prometheus-compatibility-querying-metric-metadata]

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 75 | Metric metadata `help` on [`/api/v1/metadata`](/reference/query-languages/promql/promql-http-api.md#promql-http-api-metadata-endpoint) | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` Always an empty string | The `HELP` text of the metric | [#157230](https://github.com/elastic/elasticsearch/issues/157230) |

### Querying exemplars [prometheus-compatibility-querying-exemplars]

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 76 | Exemplar queries: `/api/v1/query_exemplars` | Not implemented. Turn off exemplar queries in the client. In Grafana, go to **Data sources** > **Elasticsearch** > **Exemplars** and remove all exemplar links | Returns exemplars | [#154786](https://github.com/elastic/elasticsearch/issues/154786) |
