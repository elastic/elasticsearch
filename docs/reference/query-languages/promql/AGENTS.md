# PromQL reference docs

## Keep the limitations list current

`promql-limitations.md#promql-limitations-unsupported-constructs` is the single list of PromQL constructs that
{{es}} does not evaluate yet. It is also included in the Kibana PromQL query-generation instructions, so agents
rely on it as a concise, complete list of what to avoid. A stale entry makes generated queries fail, or steers
them away from constructs that now work.

When you add, remove, or change the restrictions on a PromQL construct (selector, operator, modifier, aggregation,
subquery, function, and so on), update that list in the same PR. Check the version badges too: an entry that only
applies to some versions needs an `{applies_to}` badge.

- State each restriction once, in the limitations page. Other pages (operators, functions, Grafana) link to the
  `promql-limitations-unsupported-constructs` anchor instead of restating it, so there is nothing to drift.
- Unsupported functions are the exception: the "Not yet supported" function list is generated from
  `PromqlFunctionRegistry`. Do not edit it by hand.

## Generated files

Everything under `_snippets/generated/` and `kibana/generated/` is produced by `PromqlDocsSupport` and asserted
byte-for-byte in CI. Edit `PromqlDocsSupport` (or the function's definition), then regenerate with
`PromqlKibanaDefinitionGeneratorTests`. Never hand-edit those files.
