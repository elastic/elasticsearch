# PromQL reference docs

## Keep the limitations list current

`promql-limitations.md#promql-limitations-unsupported-constructs` is the single list of PromQL constructs that
Elasticsearch does not evaluate yet. Agents rely on it as a concise, complete list of what to avoid. A stale entry
makes generated queries fail, or steers them away from constructs that now work.

When you add, remove, or change the restrictions on a PromQL construct (selector, operator, modifier, aggregation,
subquery, function, and so on), update that list in the same PR. Check the version badges too: an entry that only
applies to some versions needs an `{applies_to}` badge.

- State each restriction once, in the limitations page. Other pages (operators, functions, Grafana) link to the
  `promql-limitations-unsupported-constructs` anchor instead of restating it, so there is nothing to drift.
- Unsupported functions are the exception: the "Not yet supported" function list is generated from
  `PromqlFunctionRegistry`. Do not edit it by hand.

## Kibana copy

Kibana's ES|QL generation tool keeps a hand-maintained copy of the unsupported constructs, and of the function
restrictions under "Differences from Prometheus", in
[`promql_queries.md`](https://github.com/elastic/kibana/blob/main/x-pack/platform/packages/shared/agent-builder/agent-builder-genai-utils/tools/generate_esql/documentation/promql_queries.md).
When you change either, update that file too, or open a Kibana issue for it. The list of supported functions
comes from the synced Kibana definitions and needs no manual update.

## Generated files

Everything under `_snippets/generated/` and `kibana/generated/` is produced by `PromqlDocsSupport` and asserted
byte-for-byte in CI. Edit `PromqlDocsSupport` (or the function's definition), then regenerate with
`PromqlKibanaDefinitionGeneratorTests`. Never hand-edit those files.
