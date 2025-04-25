---
navigation_title: "Commands"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-commands.html
---

# {{esql}} commands [esql-commands]

## Source commands [esql-source-commands]

An {{esql}} source command produces a table, typically with data from {{es}}. An {{esql}} query must start with a source command.

:::{image} ../images/source-command.svg
:alt: A source command producing a table from {{es}}
:::

{{esql}} supports these source commands:

:::{include} _snippets/lists/source-commands.md
:::

## Processing commands [esql-processing-commands]

{{esql}} processing commands change an input table by adding, removing, or changing rows and columns.

:::{image} ../images/processing-command.svg
:alt: A processing command changing an input table
:::

{{esql}} supports these processing commands:

:::{include} _snippets/lists/processing-commands.md
:::
