---
navigation_title: "Search functions"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-functions-operators.html#esql-search-functions
---

# {{esql}} Search functions [esql-search-functions]

Use these functions
for [full-text search](docs-content://solutions/search/full-text.md)
and [semantic search](docs-content://solutions/search/semantic-search/semantic-search-semantic-text.md).

Get started with {{esql}} for search use cases with
our [hands-on tutorial](docs-content://solutions/search/esql-search-tutorial.md).

Full text functions can be used to
match [multivalued fields](/reference/query-languages/esql/esql-multivalued-fields.md).
A multivalued field that contains a value that matches a full text query is
considered to match the query.

Full text functions are significantly more performant for text search use cases
on large data sets than using pattern matching or regular expressions with
`LIKE` or `RLIKE`

See [full text search limitations](/reference/query-languages/esql/limitations.md#esql-limitations-full-text-search)
for information on the limitations of full text search.

{{esql}} supports these full-text search functions:

:::{include} ../_snippets/lists/search-functions.md
:::

:::{include} ../_snippets/functions/layout/kql.md
:::

:::{include} ../_snippets/functions/layout/match.md
:::

:::{include} ../_snippets/functions/layout/match_phrase.md
:::

:::{include} ../_snippets/functions/layout/qstr.md
:::

% TERM is currently a hidden feature
% To make it visible again, uncomment this and the line in
lists/search-functions.md
% :::{include} ../_snippets/functions/layout/term.md
% :::

