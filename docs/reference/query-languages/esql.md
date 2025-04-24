---
navigation_title: "{{esql}}"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-language.html
---

# {{esql}} reference [esql-language]

:::{note}
This section provides detailed **reference information** about the {{esql}} language, including syntax, functions, and operators.

For overview, conceptual, and getting started information, refer to the [{{esql}} language overview](docs-content://explore-analyze/query-filter/languages/esql.md) in the **Explore and analyze** section.
:::

{{esql}} is a SQL-like query language for querying and analyzing time series data in {{es}}. It is designed to be easy to use and understand, while also being powerful enough to handle complex queries.

This reference section provides detailed technical information about {{esql}} features, syntax, and behaviors:

* [Syntax reference](esql/esql-syntax-reference.md): Learn the basic syntax of commands, functions, and operators
* [Advanced commands](esql/esql-advanced-commands.md): Learn about structured text extraction with `DISSECT` and `GROK`, and combining data from multiple indices with `ENRICH` and `LOOKUP JOIN`
* [Types and fields](esql/esql-types-and-fields.md): Learn about how {{esql}} handles different data types and special fields
* [Limitations](esql/limitations.md): Learn about the current limitations of {{esql}}
* [Examples](esql/esql-examples.md): Explore some example queries