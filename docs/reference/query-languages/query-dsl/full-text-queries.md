---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/full-text-queries.html
---

# Full text queries [full-text-queries]

The full text queries enable you to search [analyzed text fields](docs-content://manage-data/data-store/text-analysis.md) such as the body of an email. The query string is processed using the same analyzer that was applied to the field during indexing.

The queries in this group are:

[`intervals` query](/reference/query-languages/query-dsl/query-dsl-intervals-query.md)
:   A full text query that allows fine-grained control of the ordering and proximity of matching terms.

[`match` query](/reference/query-languages/query-dsl/query-dsl-match-query.md)
:   The standard query for performing full text queries, including fuzzy matching and phrase or proximity queries.

[`match_bool_prefix` query](/reference/query-languages/query-dsl/query-dsl-match-bool-prefix-query.md)
:   Creates a `bool` query that matches each term as a `term` query, except for the last term, which is matched as a `prefix` query

[`match_phrase` query](/reference/query-languages/query-dsl/query-dsl-match-query-phrase.md)
:   Like the `match` query but used for matching exact phrases or word proximity matches.

[`match_phrase_prefix` query](/reference/query-languages/query-dsl/query-dsl-match-query-phrase-prefix.md)
:   Like the `match_phrase` query, but does a wildcard search on the final word.

[`multi_match` query](/reference/query-languages/query-dsl/query-dsl-multi-match-query.md)
:   The multi-field version of the `match` query.

[`combined_fields` query](/reference/query-languages/query-dsl/query-dsl-combined-fields-query.md)
:   Matches over multiple fields as if they had been indexed into one combined field.

[`query_string` query](/reference/query-languages/query-dsl/query-dsl-query-string-query.md)
:   Supports the compact Lucene [query string syntax](/reference/query-languages/query-dsl/query-dsl-query-string-query.md#query-string-syntax), allowing you to specify AND|OR|NOT conditions and multi-field search within a single query string. For expert users only.

[`simple_query_string` query](/reference/query-languages/query-dsl/query-dsl-simple-query-string-query.md)
:   A simpler, more robust version of the `query_string` syntax suitable for exposing directly to users.










