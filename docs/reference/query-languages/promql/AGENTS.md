# PromQL reference docs

## Keep the limitations list current

`_snippets/promql-unsupported-constructs.md` is the single list of PromQL constructs that Elasticsearch does not
evaluate yet. Both the limitations page and the `PROMQL` command page include it. Agents rely on it as a concise,
complete list of what to avoid. A stale entry makes generated queries fail, or steers them away from constructs that
now work.

When you add, remove, or change the restrictions on a PromQL construct (selector, operator, modifier, aggregation,
subquery, function, and so on), update that list in the same PR. Check the version badges too: an entry that only
applies to some versions needs an `{applies_to}` badge. Unsupported functions are the exception: their list is
generated from `PromqlFunctionRegistry` by `PromqlKibanaDefinitionGeneratorTests`.

## Copies for agents

The `PROMQL` command page (`esql/_snippets/commands/layout/promql.md`) is copied verbatim into the context of
agents that generate PromQL. It includes the unsupported constructs snippet, so the copies get the full list:

- Kibana's ES|QL generation tool syncs it to `esql_docs/esql-promql.txt`.
- The `elasticsearch-esql` agent skill in `elastic/agent-skills-sandbox` copies it with `make sync-docs`.

Neither copy needs a manual edit, but the update is not yet automated.
After a docs PR to `promql.md` or the pages it includes is merged, create a PR in Kibana and
`elastic/agent-skills-sandbox` to update the copy.
