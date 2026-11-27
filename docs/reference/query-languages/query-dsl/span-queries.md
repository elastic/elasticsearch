---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/span-queries.html
---

# Span queries [span-queries]

Span queries are low-level positional queries which provide expert control over the order and proximity of the specified terms. These are typically used to implement very specific queries on legal documents or patents.

:::{tip}
We recommend using the [`intervals` query](/reference/query-languages/query-dsl/query-dsl-intervals-query.md) instead of span queries. It gives similar control over the order and proximity of terms, but is simpler to use and more flexible. For example, its rules take analyzed text (including synonyms) instead of individual terms, support script filters on match positions, and offer more positional relations, such as `overlapping` and `not_contained_by`, all without wrapper queries.
:::

It is only allowed to set boost on an outer span query. Compound span queries, like span_near, only use the list of matching spans of inner span queries in order to find their own spans, which they then use to produce a score. Scores are never computed on inner span queries, which is the reason why boosts are not allowed: they only influence the way scores are computed, not spans.

Span queries cannot be mixed with non-span queries (with the exception of the `span_multi` query).

The queries in this group are:

[`span_containing` query](/reference/query-languages/query-dsl/query-dsl-span-containing-query.md)
:   Accepts a list of span queries, but only returns those spans which also match a second span query.

[`span_field_masking` query](/reference/query-languages/query-dsl/query-dsl-span-field-masking-query.md)
:   Allows queries like `span-near` or `span-or` across different fields.

[`span_first` query](/reference/query-languages/query-dsl/query-dsl-span-first-query.md)
:   Accepts another span query whose matches must appear within the first N positions of the field.

[`span_multi` query](/reference/query-languages/query-dsl/query-dsl-span-multi-term-query.md)
:   Wraps a [`term`](/reference/query-languages/query-dsl/query-dsl-term-query.md), [`range`](/reference/query-languages/query-dsl/query-dsl-range-query.md), [`prefix`](/reference/query-languages/query-dsl/query-dsl-prefix-query.md), [`wildcard`](/reference/query-languages/query-dsl/query-dsl-wildcard-query.md), [`regexp`](/reference/query-languages/query-dsl/query-dsl-regexp-query.md), or [`fuzzy`](/reference/query-languages/query-dsl/query-dsl-fuzzy-query.md) query.

[`span_near` query](/reference/query-languages/query-dsl/query-dsl-span-near-query.md)
:   Accepts multiple span queries whose matches must be within the specified distance of each other, and possibly in the same order.

[`span_not` query](/reference/query-languages/query-dsl/query-dsl-span-not-query.md)
:   Wraps another span query, and excludes any documents which match that query.

[`span_or` query](/reference/query-languages/query-dsl/query-dsl-span-query.md)
:   Combines multiple span queries — returns documents which match any of the specified queries.

[`span_term` query](/reference/query-languages/query-dsl/query-dsl-span-term-query.md)
:   The equivalent of the [`term` query](/reference/query-languages/query-dsl/query-dsl-term-query.md) but for use with other span queries.

[`span_within` query](/reference/query-languages/query-dsl/query-dsl-span-within-query.md)
:   The result from a single span query is returned as long is its span falls within the spans returned by a list of other span queries.










