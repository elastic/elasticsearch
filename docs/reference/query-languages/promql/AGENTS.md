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
| `basics.md` | [Querying basics](https://prometheus.io/docs/prometheus/latest/querying/basics/) | Its section headings, such as `Instant vector selectors`, `@ modifier`, and `Staleness`. The data types are split into `Histograms` and `Range vector` |
| `operators.md` | [Operators](https://prometheus.io/docs/prometheus/latest/querying/operators/) | Its section headings, such as `Comparison binary operators` and `Vector matching` |
| `functions.md` | [Functions](https://prometheus.io/docs/prometheus/latest/querying/functions/) | None. Rows are sorted by function name |
| `http-api.md` | [HTTP API](https://prometheus.io/docs/prometheus/latest/querying/api/) | Its section headings, such as `Expression queries` |

Put a new row under the Prometheus heading that documents its construct, and use that heading's name. Add a heading
only when Prometheus has it. A construct that spans pages, such as native histograms, gets one row per heading. Leave out a heading that has no rows.

Agents rely on these rows as a concise, complete list of what to avoid. A stale row makes generated queries fail, or
steers them away from constructs that now work.

When you add, remove, or change the restrictions on a PromQL construct (selector, operator, modifier, aggregation,
subquery, function, and so on), or change a result so it matches Prometheus, update the matching row in the same PR.

- Each row compares what Elasticsearch does with what Prometheus does. Expected behavior comes from Prometheus, never
  from Elasticsearch tests.
- List features that aren't supported yet and differences by design, not bugs. Bugs are tracked and fixed separately.
- Each heading starts with **Supported**: a short paragraph, with examples, of what runs and returns the same result as
  in Prometheus. Leave it out when nothing under the heading is supported. Update it when a row is added or removed.
- Under each heading, rows split into two tables. **Not supported yet** (Elasticsearch rejects the query) has columns
  `# | Construct | Example | Workaround | Issue` in `:::{table}` with `:widths: 1-4-3-3-1`. **Behaves differently** (the
  query runs, the result differs) has `# | Construct | Example | {{es}} | Prometheus | Issue` with
  `:widths: 1-3-2-3-2-1`. Prometheus supports everything in a Not supported yet table; name a Prometheus feature flag in
  the Construct cell when one is needed.
- Construct is a short name, with a version badge at its end when the row needs one. Example holds one or two queries in
  code spans. Escape `|` inside a cell, or pick an example without one.
- A difference that has no construct of its own, such as the `or` operand limit, is a sentence in the Supported
  paragraph, not a row.
- The Issue column links the public `elastic/elasticsearch` issue that tracks the feature, or `-` when there is none.
  Write the link with the `es-issue` substitution: `[#123]({{es-issue}}123)`.
- Rows are numbered across all sections, in page order. Keep the numbers sequential when you add or remove a row.
- Check the version badges. A row without a badge applies to every version. A difference that only exists from some
  version on gets `stack: ga <version>+` and `serverless: ga`. A difference that only exists in older versions gets
  those versions and no `serverless` badge, such as `stack: ga =9.6` or `stack: preview =9.4, ga =9.5`.
- When a change lifts a limitation completely, remove its row and badge the Supported line with the version that lifts
  it. When it lifts the limitation only in part, or when older versions return a different result without an error,
  keep a row for the older versions.
- Unsupported functions are listed separately: their list is generated from `PromqlFunctionRegistry` by
  `PromqlKibanaDefinitionGeneratorTests`.

## Function and operator entries

Each generated function and operator entry can end with one note, `stdDivergence`, set on its definition
(`PromqlFunctionDefinition`, or `OPERATOR_DEFS` in `PromqlDocsSupport`) and rendered as **Differences from Prometheus**.
It states first the uses Prometheus accepts and Elasticsearch rejects, with an example each, in one form:
"`<use>`, such as `<example>`, isn't supported yet." It then states the uses that run but return a different result.

What the function supports goes in its description or `extendedDescription`. Keep these notes in step with the
compatibility rows that name the function or operator.

## Writing style

Write `{{prom}}` instead of `Prometheus` and `{{promql}}` instead of `PromQL` in prose, as you write `{{es}}` instead
of `Elasticsearch`. Keep the plain word in front matter, `%` comments, code, URLs, and anchors. In Java, use these
substitutions only in `stdDivergence` notes: descriptions are also copied into the Kibana definitions, which don't
resolve substitutions.

## Copies for agents

The `PROMQL` command page (`esql/_snippets/commands/layout/promql.md`) is copied verbatim into the context of
agents that generate PromQL. It includes the compatibility snippets, so the copies get the full list:

- Kibana's ES|QL generation tool syncs it to `esql_docs/esql-promql.txt`.
- The `elasticsearch-esql` agent skill in `elastic/agent-skills-sandbox` copies it with `make sync-docs`.

Neither copy needs a manual edit, but the update is not yet automated.
After a docs PR to `promql.md` or the pages it includes is merged, create a PR in Kibana and
`elastic/agent-skills-sandbox` to update the copy.
