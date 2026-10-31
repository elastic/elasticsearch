# PromQL reference docs

## Keep the Prometheus compatibility page current

`prometheus-compatibility.md` is the single page of differences between PromQL in Elasticsearch and Prometheus. Each of its
sections is a snippet in `_snippets/compatibility/`, so other pages include the same rows instead of keeping their own
copies:

- The `PROMQL` command page includes every section except `http-api.md`.
- `promql-http-api.md` includes `http-api.md`.

The sections and headings mirror the Prometheus querying docs, so readers find a difference under the same heading
that documents the feature in Prometheus:

| Snippet | Prometheus page | Headings |
|---|---|---|
| `basics.md` | [Querying basics](https://prometheus.io/docs/prometheus/latest/querying/basics/) | Its section headings, such as `Instant vector selectors`, `@ modifier`, and `Staleness` |
| `operators.md` | [Operators](https://prometheus.io/docs/prometheus/latest/querying/operators/) | Its section headings, such as `Comparison binary operators` and `Vector matching` |
| `functions.md` | [Functions](https://prometheus.io/docs/prometheus/latest/querying/functions/) | None. One table, sorted by function name |
| `http-api.md` | [HTTP API](https://prometheus.io/docs/prometheus/latest/querying/api/) | Its section headings, such as `Expression queries` |

Put a new row under the Prometheus heading that documents its construct, and use that heading's name. Add a heading
only when Prometheus has it. A construct that spans pages, such as dropping `__name__`, gets one row per heading.

Agents rely on these rows as a concise, complete list of what to avoid. A stale row makes generated queries fail, or
steers them away from constructs that now work.

When you add, remove, or change the restrictions on a PromQL construct (selector, operator, modifier, aggregation,
subquery, function, and so on), or change a result so it matches Prometheus, update the matching row in the same PR.

- Each row compares what Elasticsearch does with what Prometheus does. Expected behavior comes from Prometheus, never
  from Elasticsearch tests.
- List features that aren't supported yet and differences by design, not bugs. Bugs are tracked and fixed separately.
- Write `Unsupported` in the Elasticsearch column for a construct that Elasticsearch rejects, followed by the
  workaround if there is one, and `Supported` in the Prometheus column unless Prometheus needs a feature flag.
- Keep every table inside `:::{table}` with `:widths: 1-4-3-3-1`, so all tables share the same column grid.
- The Issue column links the public `elastic/elasticsearch` issue that tracks the feature, or `-` when there is none.
- Rows are numbered across all sections, in page order. Keep the numbers sequential when you add or remove a row.
- Keep rows for versions that are about to leave maintenance, such as `=9.4`, at the bottom of their table.
- Check the version badges. A row without a badge applies to every version. A limitation that a change lifts gets
  `stack: ga <first>-<last>` for the versions that still have it. A difference that only exists from some version on
  gets `stack: ga <version>+` and `serverless: ga`.
- Unsupported functions are listed separately: their list is generated from `PromqlFunctionRegistry` by
  `PromqlKibanaDefinitionGeneratorTests`.

## Copies for agents

The `PROMQL` command page (`esql/_snippets/commands/layout/promql.md`) is copied verbatim into the context of
agents that generate PromQL. It includes the compatibility snippets, so the copies get the full list:

- Kibana's ES|QL generation tool syncs it to `esql_docs/esql-promql.txt`.
- The `elasticsearch-esql` agent skill in `elastic/agent-skills-sandbox` copies it with `make sync-docs`.

Neither copy needs a manual edit, but the update is not yet automated.
After a docs PR to `promql.md` or the pages it includes is merged, create a PR in Kibana and
`elastic/agent-skills-sandbox` to update the copy.
