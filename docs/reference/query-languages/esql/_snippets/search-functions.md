## {{esql}} Full-text search functions [esql-search-functions]


Full text functions are used to search for text in fields. [Text analysis](docs-content://manage-data/data-store/text-analysis.md) is used to analyze the query before it is searched.

Full text functions can be used to match [multivalued fields](/reference/query-languages/esql/esql-multivalued-fields.md). A multivalued field that contains a value that matches a full text query is considered to match the query.

Full text functions are significantly more performant for text search use cases on large data sets than using pattern matching or regular expressions with `LIKE` or `RLIKE`

See [full text search limitations](/reference/query-languages/esql/limitations.md#esql-limitations-full-text-search) for information on the limitations of full text search.

{{esql}} supports these full-text search functions:

:::{include} lists/search-functions.md
:::


:::{include} functions/layout/kql.md
:::

:::{include} functions/layout/match.md
:::

:::{include} functions/layout/qstr.md
:::

