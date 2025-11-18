---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/term-level-queries.html
---

# Term-level queries [term-level-queries]

You can use **term-level queries** to find documents based on precise values in structured data. Examples of structured data include date ranges, IP addresses, prices, or product IDs.

Unlike [full-text queries](/reference/query-languages/query-dsl/full-text-queries.md), term-level queries do not analyze search terms. Instead, term-level queries match the exact terms stored in a field.

::::{note}
Term-level queries still normalize search terms for `keyword` fields with the `normalizer` property. For more details, see [`normalizer`](/reference/elasticsearch/mapping-reference/normalizer.md).

::::



## Types of term-level queries [term-level-query-types]

[`exists` query](/reference/query-languages/query-dsl/query-dsl-exists-query.md)
:   Returns documents that contain any indexed value for a field.

[`fuzzy` query](/reference/query-languages/query-dsl/query-dsl-fuzzy-query.md)
:   Returns documents that contain terms similar to the search term. {{es}} measures similarity, or fuzziness, using a [Levenshtein edit distance](https://en.wikipedia.org/wiki/Levenshtein_distance).

[`ids` query](/reference/query-languages/query-dsl/query-dsl-ids-query.md)
:   Returns documents based on their [document IDs](/reference/elasticsearch/mapping-reference/mapping-id-field.md).

[`prefix` query](/reference/query-languages/query-dsl/query-dsl-prefix-query.md)
:   Returns documents that contain a specific prefix in a provided field.

[`range` query](/reference/query-languages/query-dsl/query-dsl-range-query.md)
:   Returns documents that contain terms within a provided range.

[`regexp` query](/reference/query-languages/query-dsl/query-dsl-regexp-query.md)
:   Returns documents that contain terms matching a [regular expression](https://en.wikipedia.org/wiki/Regular_expression).

[`term` query](/reference/query-languages/query-dsl/query-dsl-term-query.md)
:   Returns documents that contain an exact term in a provided field.

[`terms` query](/reference/query-languages/query-dsl/query-dsl-terms-query.md)
:   Returns documents that contain one or more exact terms in a provided field.

[`terms_set` query](/reference/query-languages/query-dsl/query-dsl-terms-set-query.md)
:   Returns documents that contain a minimum number of exact terms in a provided field. You can define the minimum number of matching terms using a field or script.

[`wildcard` query](/reference/query-languages/query-dsl/query-dsl-wildcard-query.md)
:   Returns documents that contain terms matching a wildcard pattern.
