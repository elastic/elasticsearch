---
applies_to:
  stack:
  serverless:
navigation_title: "Query directives"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-commands.html
---

# {{esql}} query directives [esql-query-directives]

An {{esql}} directive is an instruction that defines query settings and general behavior.

Directives are specified at the beginning of an {{esql}} query, before the source command,
and are separated by semicolons. Multiple directives can be included in a single query.

{{esql}} supports these directives:

:::{include} ../_snippets/lists/directives.md
:::

