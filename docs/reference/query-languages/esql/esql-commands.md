---
applies_to:
  stack:
  serverless:
navigation_title: "Commands"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-commands.html
---

# {{esql}} commands [esql-commands]

{{esql}} queries are built from the following building blocks:

- Optionally, start with [query directives](./commands/directives.md) to define query settings and general behavior.
- Every query must include a [source command](./commands/source-commands.md).
- Use [processing commands](./commands/processing-commands.md) to modify an input table by adding, removing, or transforming rows and columns.