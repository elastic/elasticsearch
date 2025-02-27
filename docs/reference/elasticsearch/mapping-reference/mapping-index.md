---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-index.html
---

# index [mapping-index]

The `index` option controls whether field values are indexed. It accepts `true` or `false` and defaults to `true`.

Indexing a field creates data structures that enable the field to be queried efficiently. [Numeric types](/reference/elasticsearch/mapping-reference/number.md), [date types](/reference/elasticsearch/mapping-reference/date.md), the [boolean type](/reference/elasticsearch/mapping-reference/boolean.md), [ip type](/reference/elasticsearch/mapping-reference/ip.md), [geo_point type](/reference/elasticsearch/mapping-reference/geo-point.md) and the [keyword type](/reference/elasticsearch/mapping-reference/keyword.md) can also be queried when they are not indexed but only have doc values enabled. Queries on these fields are slow as a full scan of the index has to be made. All other fields are not queryable.

