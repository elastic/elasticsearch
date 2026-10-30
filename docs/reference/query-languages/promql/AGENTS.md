# PromQL reference docs

## Keep the limitations current

`promql-limitations.md` is the single page of differences between PromQL in Elasticsearch and Prometheus. Each of its
sections is a snippet in `_snippets/limitations/`, so other pages include the same rows instead of keeping their own
copies:

- The `PROMQL` command page includes every section except `http-api.md`.
- `promql-http-api.md` includes `http-api.md`.

Agents rely on these rows as a concise, complete list of what to avoid. A stale row makes generated queries fail, or
steers them away from constructs that now work.

When you add, remove, or change the restrictions on a PromQL construct (selector, operator, modifier, aggregation,
subquery, function, and so on), or change a result so it matches Prometheus, update the matching row in the same PR.

- Each row compares what Elasticsearch does with what Prometheus does. Expected behavior comes from Prometheus, never
  from Elasticsearch tests.
- Rows are numbered across all sections. Keep the numbers sequential when you add or remove a row.
- Check the version badges. A row without a badge applies to every version. A limitation that a change lifts gets
  `stack: ga <first>-<last>` for the versions that still have it. A difference that only exists from some version on
  gets `stack: ga <version>+` and `serverless: ga`.
- Unsupported functions are listed separately: their list is generated from `PromqlFunctionRegistry` by
  `PromqlKibanaDefinitionGeneratorTests`.

## Copies for agents

The `PROMQL` command page (`esql/_snippets/commands/layout/promql.md`) is copied verbatim into the context of
agents that generate PromQL. It includes the limitations snippets, so the copies get the full list:

- Kibana's ES|QL generation tool syncs it to `esql_docs/esql-promql.txt`.
- The `elasticsearch-esql` agent skill in `elastic/agent-skills-sandbox` copies it with `make sync-docs`.

Neither copy needs a manual edit, but the update is not yet automated.
After a docs PR to `promql.md` or the pages it includes is merged, create a PR in Kibana and
`elastic/agent-skills-sandbox` to update the copy.
