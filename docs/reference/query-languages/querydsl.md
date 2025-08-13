---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html
---

# QueryDSL [query-dsl]

:::{note}
This section provides detailed **reference information**.

Refer to the [Query DSL overview](docs-content://explore-analyze/query-filter/languages/querydsl.md) in the **Explore and analyze** section for overview and conceptual information about Query DSL.
:::

Elasticsearch provides a full Query DSL (Domain Specific Language) based on JSON to define queries. Think of the Query DSL as an AST (Abstract Syntax Tree) of queries, consisting of two types of clauses:

Leaf query clauses
:   Leaf query clauses look for a particular value in a particular field, such as the [`match`](/reference/query-languages/query-dsl/query-dsl-match-query.md), [`term`](/reference/query-languages/query-dsl/query-dsl-term-query.md) or [`range`](/reference/query-languages/query-dsl/query-dsl-range-query.md) queries. These queries can be used by themselves.

Compound query clauses
:   Compound query clauses wrap other leaf **or** compound queries and are used to combine multiple queries in a logical fashion (such as the [`bool`](/reference/query-languages/query-dsl/query-dsl-bool-query.md) or [`dis_max`](/reference/query-languages/query-dsl/query-dsl-dis-max-query.md) query), or to alter their behaviour (such as the [`constant_score`](/reference/query-languages/query-dsl/query-dsl-constant-score-query.md) query).

Query clauses behave differently depending on whether they are used in [query context or filter context](/reference/query-languages/query-dsl/query-filter-context.md).

$$$query-dsl-allow-expensive-queries$$$

Allow expensive queries
:   Certain types of queries will generally execute slowly due to the way they are implemented, which can affect the stability of the cluster. Those queries can be categorised as follows:

    * Queries that need to do linear scans to identify matches:
        * [`script` queries](/reference/query-languages/query-dsl/query-dsl-script-query.md)
        * queries on [numeric](/reference/elasticsearch/mapping-reference/number.md), [date](/reference/elasticsearch/mapping-reference/date.md), [boolean](/reference/elasticsearch/mapping-reference/boolean.md), [ip](/reference/elasticsearch/mapping-reference/ip.md), [geo_point](/reference/elasticsearch/mapping-reference/geo-point.md) or [keyword](/reference/elasticsearch/mapping-reference/keyword.md) fields that are not indexed but have [doc values](/reference/elasticsearch/mapping-reference/doc-values.md) enabled

    * Queries that have a high up-front cost:
        * [`fuzzy` queries](/reference/query-languages/query-dsl/query-dsl-fuzzy-query.md) (except on [`wildcard`](/reference/elasticsearch/mapping-reference/keyword.md#wildcard-field-type) fields)
        * [`regexp` queries](/reference/query-languages/query-dsl/query-dsl-regexp-query.md) (except on [`wildcard`](/reference/elasticsearch/mapping-reference/keyword.md#wildcard-field-type) fields)
        * [`prefix` queries](/reference/query-languages/query-dsl/query-dsl-prefix-query.md)  (except on [`wildcard`](/reference/elasticsearch/mapping-reference/keyword.md#wildcard-field-type) fields or those without [`index_prefixes`](/reference/elasticsearch/mapping-reference/index-prefixes.md))
        * [`wildcard` queries](/reference/query-languages/query-dsl/query-dsl-wildcard-query.md) (except on [`wildcard`](/reference/elasticsearch/mapping-reference/keyword.md#wildcard-field-type) fields)
        * [`range` queries](/reference/query-languages/query-dsl/query-dsl-range-query.md) on [`text`](/reference/elasticsearch/mapping-reference/text.md) and [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) fields

    * [Joining queries](/reference/query-languages/query-dsl/joining-queries.md)
    * Queries that may have a high per-document cost:
        * [`script_score` queries](/reference/query-languages/query-dsl/query-dsl-script-score-query.md)
        * [`percolate` queries](/reference/query-languages/query-dsl/query-dsl-percolate-query.md)


The execution of such queries can be prevented by setting the value of the `search.allow_expensive_queries` setting to `false` (defaults to `true`).

