---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-filter-context.html
---

# Query and filter context [query-filter-context]


## Relevance scores [relevance-scores]

By default, Elasticsearch sorts matching search results by **relevance score**, which measures how well each document matches a query.

The relevance score is a positive floating point number, returned in the `_score` metadata field of the [search](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search) API. The higher the `_score`, the more relevant the document. While each query type can calculate relevance scores differently, score calculation also depends on whether the query clause is run in a **query** or **filter** context.


## Query context [query-context]

In the query context, a query clause answers the question *How well does this document match this query clause?* Besides deciding whether or not the document matches, the query clause also calculates a relevance score in the `_score` metadata field.

Query context is in effect whenever a query clause is passed to a `query` parameter, such as the `query` parameter in the [search](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search#request-body-search-query) API.


## Filter context [filter-context]

A filter answers the binary question “Does this document match this query clause?”. The answer is simply "yes" or "no". Filtering has several benefits:

1. **Simple binary logic**: In a filter context, a query clause determines document matches based on a yes/no criterion, without score calculation.
2. **Performance**: Because they don’t compute relevance scores, filters execute faster than queries.
3. **Caching**: {{es}} automatically caches frequently used filters, speeding up subsequent search performance.
4. **Resource efficiency**: Filters consume less CPU resources compared to full-text queries.
5. **Query combination**: Filters can be combined with scored queries to refine result sets efficiently.

Filters are particularly effective for querying structured data and implementing "must have" criteria in complex searches.

Structured data refers to information that is highly organized and formatted in a predefined manner. In the context of Elasticsearch, this typically includes:

* Numeric fields (integers, floating-point numbers)
* Dates and timestamps
* Boolean values
* Keyword fields (exact match strings)
* Geo-points and geo-shapes

Unlike full-text fields, structured data has a consistent, predictable format, making it ideal for precise filtering operations.

Common filter applications include:

* Date range checks: for example is the `timestamp` field between 2015 and 2016
* Specific field value checks: for example is the `status` field equal to "published" or is the `author` field equal to "John Doe"

Filter context applies when a query clause is passed to a `filter` parameter, such as:

* `filter` or `must_not` parameters in [`bool`](/reference/query-languages/query-dsl/query-dsl-bool-query.md)  queries
* `filter` parameter in [`constant_score`](/reference/query-languages/query-dsl/query-dsl-constant-score-query.md) queries
* [`filter`](/reference/aggregations/search-aggregations-bucket-filter-aggregation.md) aggregations

Filters optimize query performance and efficiency, especially for structured data queries and when combined with full-text searches.


## Example of query and filter contexts [query-filter-context-ex]

Below is an example of query clauses being used in query and filter context in the `search` API. This query will match documents where all of the following conditions are met:

* The `title` field contains the word `search`.
* The `content` field contains the word `elasticsearch`.
* The `status` field contains the exact word `published`.
* The `publish_date` field contains a date from 1 Jan 2015 onwards.

```console
GET /_search
{
  "query": { <1>
    "bool": { <2>
      "must": [
        { "match": { "title":   "Search"        }},
        { "match": { "content": "Elasticsearch" }}
      ],
      "filter": [ <3>
        { "term":  { "status": "published" }},
        { "range": { "publish_date": { "gte": "2015-01-01" }}}
      ]
    }
  }
}
```

1. The `query` parameter indicates query context.
2. The `bool` and two `match` clauses are used in query context, which means that they are used to score how well each document matches.
3. The `filter` parameter indicates filter context. Its `term` and `range` clauses are used in filter context. They will filter out documents which do not match, but they will not affect the score for matching documents.


::::{warning}
Scores calculated for queries in query context are represented as single precision floating point numbers; they have only 24 bits for significand’s precision. Score calculations that exceed the significand’s precision will be converted to floats with loss of precision.
::::


::::{tip}
Use query clauses in query context for conditions which should affect the score of matching documents (i.e. how well does the document match), and use all other query clauses in filter context.
::::
