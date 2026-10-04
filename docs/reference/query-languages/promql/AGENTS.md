# PromQL reference docs

## Keep the limitations list current

`_snippets/promql-unsupported-constructs.md` is the single list of PromQL constructs that Elasticsearch does not
evaluate yet. Both the limitations page and the `PROMQL` command page include it. Agents rely on it as a concise,
complete list of what to avoid. A stale entry makes generated queries fail, or steers them away from constructs that
now work.

When you add, remove, or change the restrictions on a PromQL construct (selector, operator, modifier, aggregation,
subquery, function, and so on), update that list in the same PR. Check the version badges too: an entry that only
applies to some versions needs an `{applies_to}` badge.

- State each restriction once, in the snippet. Other pages (operators, functions, Grafana) link to the
  `promql-limitations-unsupported-constructs` anchor of the limitations page instead of restating it, so there is
  nothing to drift.
- Unsupported functions are the exception: the "Not yet supported" function list is generated from
  `PromqlFunctionRegistry`. Regenerate it by running `PromqlKibanaDefinitionGeneratorTests`.

## Copies for agents

The `PROMQL` command page (`esql/_snippets/commands/layout/promql.md`) is copied verbatim into the context of
agents that generate PromQL. It includes the unsupported constructs snippet, so the copies get the full list:

- Kibana's ES|QL generation tool syncs it to `esql_docs/esql-promql.txt`.
- The `elasticsearch-esql` agent skill copies it into
  [`promql-command.md`](https://github.com/elastic/agent-skills/blob/main/skills/elasticsearch/elasticsearch-esql/references/promql-command.md)
  with `make sync-docs`.

Neither copy needs a manual edit, but changes only reach agents once the copies are refreshed. Agents only see
what is on the command page, so put guidance they need there, and keep the unsupported constructs on it by
including the snippet, not by linking to it. Guidance on other pages, such as the "Differences from Prometheus"
notes of the function docs, doesn't reach them. Kibana takes the list of supported functions from the synced
Kibana definitions.
