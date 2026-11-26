---
navigation_title: Query DSL
description: Learn how Query DSL organizes queries in Elasticsearch, including leaf and compound clauses, query groups, and the expensive queries you can block.
applies_to:
  serverless: ga
  stack: ga
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html
products:
  - id: elasticsearch
type: overview
---

# Query DSL reference [query-dsl]

:::{note}
This section provides detailed **reference information**.

Refer to the [Query DSL overview](docs-content://explore-analyze/query-filter/languages/querydsl.md) for overview and conceptual information about Query DSL.
:::

Elasticsearch provides a full Query DSL (Domain Specific Language) based on JSON to define queries. You can start with a single clause, such as a `match` query, and combine clauses into larger queries as your searches grow. Think of the Query DSL as an AST (Abstract Syntax Tree) of queries.

## Query clause types

Query clauses fall into two types:

Leaf query clauses
:   Leaf query clauses look for a particular value in a particular field, such as the [`match`](/reference/query-languages/query-dsl/query-dsl-match-query.md), [`term`](/reference/query-languages/query-dsl/query-dsl-term-query.md) or [`range`](/reference/query-languages/query-dsl/query-dsl-range-query.md) queries. These queries can be used by themselves.

Compound query clauses
:   Compound query clauses wrap other leaf **or** compound queries and are used to combine multiple queries in a logical fashion (such as the [`bool`](/reference/query-languages/query-dsl/query-dsl-bool-query.md) or [`dis_max`](/reference/query-languages/query-dsl/query-dsl-dis-max-query.md) query), or to alter their behaviour (such as the [`constant_score`](/reference/query-languages/query-dsl/query-dsl-constant-score-query.md) query).

Query clauses behave differently depending on whether they are used in [query context or filter context](/reference/query-languages/query-dsl/query-filter-context.md).

## Browse queries by group

Query DSL groups queries by the kind of search they run. Select a group to compare its queries:

* [Full-text queries](/reference/query-languages/query-dsl/full-text-queries.md): Search analyzed text fields, such as the body of an email.
* [Term-level queries](/reference/query-languages/query-dsl/term-level-queries.md): Find documents based on precise values in structured data, such as date ranges, IP addresses, prices, or product IDs.
* [Compound queries](/reference/query-languages/query-dsl/compound-queries.md): Combine other queries, change their behavior, or switch from query to filter context.
* [Vector queries](/reference/query-languages/query-dsl/vector-queries.md): Search vector fields to run semantic search.
* [Geo queries](/reference/query-languages/query-dsl/geo-queries.md): Search `geo_point` and `geo_shape` fields.
* [Shape queries](/reference/query-languages/query-dsl/shape-queries.md): Search `point` and `shape` fields that hold cartesian, non-geospatial data.
* [Joining queries](/reference/query-languages/query-dsl/joining-queries.md): Query related documents with `nested` and parent-child joins.
* [Span queries](/reference/query-languages/query-dsl/span-queries.md): Control the order and proximity of terms with low-level positional queries.
* [Specialized queries](/reference/query-languages/query-dsl/specialized-queries.md): Use queries that don't fit the other groups.
* [Match all query](/reference/query-languages/query-dsl/query-dsl-match-all-query.md): Match every document and give each a `_score` of `1.0`.

## Limit expensive queries [query-dsl-allow-expensive-queries]

Certain types of queries generally run slowly because of the way they are implemented, which can affect the stability of the cluster. These queries fall into the following categories:

* Queries that need to do linear scans to identify matches:
    * [`script` queries](/reference/query-languages/query-dsl/query-dsl-script-query.md)
    * queries on [numeric](/reference/elasticsearch/mapping-reference/number.md), [date](/reference/elasticsearch/mapping-reference/date.md), [boolean](/reference/elasticsearch/mapping-reference/boolean.md), [ip](/reference/elasticsearch/mapping-reference/ip.md), [geo_point](/reference/elasticsearch/mapping-reference/geo-point.md) or [keyword](/reference/elasticsearch/mapping-reference/keyword.md) fields that are not indexed but have [doc values](/reference/elasticsearch/mapping-reference/doc-values.md) enabled

* Queries that have a high up-front cost:
    * [`fuzzy` queries](/reference/query-languages/query-dsl/query-dsl-fuzzy-query.md) (except on [`wildcard`](/reference/elasticsearch/mapping-reference/keyword.md#wildcard-field-type) fields)
    * [`regexp` queries](/reference/query-languages/query-dsl/query-dsl-regexp-query.md) (except on [`wildcard`](/reference/elasticsearch/mapping-reference/keyword.md#wildcard-field-type) fields)
    * [`prefix` queries](/reference/query-languages/query-dsl/query-dsl-prefix-query.md) (except on [`wildcard`](/reference/elasticsearch/mapping-reference/keyword.md#wildcard-field-type) fields, or on [`text`](/reference/elasticsearch/mapping-reference/text.md) fields with [`index_prefixes`](/reference/elasticsearch/mapping-reference/index-prefixes.md) enabled for supported prefix lengths)
    * [`wildcard` queries](/reference/query-languages/query-dsl/query-dsl-wildcard-query.md) (except on [`wildcard`](/reference/elasticsearch/mapping-reference/keyword.md#wildcard-field-type) fields)
    * [`range` queries](/reference/query-languages/query-dsl/query-dsl-range-query.md) on [`text`](/reference/elasticsearch/mapping-reference/text.md) and [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) fields

* [Joining queries](/reference/query-languages/query-dsl/joining-queries.md)
* Queries on [runtime fields](docs-content://manage-data/data-store/mapping/runtime-fields.md)
* Queries that might have a high per-document cost:
    * [`script_score` queries](/reference/query-languages/query-dsl/query-dsl-script-score-query.md)
    * [`percolate` queries](/reference/query-languages/query-dsl/query-dsl-percolate-query.md)

{{es}} runs these queries by default. Whether you can block them depends on your deployment:

::::{applies-switch}

:::{applies-item} serverless: ga
You can't block these queries. The `search.allow_expensive_queries` setting isn't configurable.
:::

:::{applies-item} stack: ga
To block these queries, set `search.allow_expensive_queries` to `false` with the [cluster update settings API]({{es-apis}}operation/operation-cluster-put-settings). This requires the `manage` [cluster privilege](/reference/elasticsearch/security-privileges.md#privileges-list-cluster). The setting is dynamic, so the change applies without a restart. While it is `false`, a search that includes one of the listed queries returns an error that names the setting, instead of running.
:::

::::

## Next steps

To learn Query DSL by example, refer to [Get started with Query DSL search and filters](/reference/query-languages/query-dsl/full-text-filter-tutorial.md). To understand how query clauses score and filter documents, refer to [Query and filter context](/reference/query-languages/query-dsl/query-filter-context.md).
