---
navigation_title: "{{esql}} reference"
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

* [Language fundamentals](esql/esql-language-fundamentals.md): Learn the basic syntax of commands, functions, and operators
* [Special fields](esql/esql-special-fields.md): Learn how to work with metadata and multivalued fields
* [Advanced operations](esql/esql-advanced-operations.md): Learn about structured text extraction with `DISSECT` and `GROK`, and combining data with `ENRICH` and `LOOKUP JOIN`
* [Limitations](esql/limitations.md): Learn about the current limitations of {{esql}}
* [Examples](esql/esql-examples.md): Explore some example queries