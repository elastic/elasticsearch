---
navigation_title: "Adjacency matrix"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-adjacency-matrix-aggregation.html
---

# Adjacency matrix aggregation [search-aggregations-bucket-adjacency-matrix-aggregation]


A bucket aggregation returning a form of [adjacency matrix](https://en.wikipedia.org/wiki/Adjacency_matrix). The request provides a collection of named filter expressions, similar to the `filters` aggregation request. Each bucket in the response represents a non-empty cell in the matrix of intersecting filters.

Given filters named `A`, `B` and `C` the response would return buckets with the following names:

|  | A | B | C |
| --- | --- | --- | --- |
| A | A | A&B | A&C |
| B |  | B | B&C |
| C |  |  | C |

The intersecting buckets e.g `A&C` are labelled using a combination of the two filter names with a default separator of `&`. Note that the response does not also include a `C&A` bucket as this would be the same set of documents as `A&C`. The matrix is said to be *symmetric* so we only return half of it. To do this we sort the filter name strings and always use the lowest of a pair as the value to the left of the separator.

## Example [adjacency-matrix-agg-ex]

The following `interactions` aggregation uses `adjacency_matrix` to determine which groups of individuals exchanged emails.

$$$adjacency-matrix-aggregation-example$$$

```console
PUT emails/_bulk?refresh
{ "index" : { "_id" : 1 } }
{ "accounts" : ["hillary", "sidney"]}
{ "index" : { "_id" : 2 } }
{ "accounts" : ["hillary", "donald"]}
{ "index" : { "_id" : 3 } }
{ "accounts" : ["vladimir", "donald"]}

GET emails/_search
{
  "size": 0,
  "aggs" : {
    "interactions" : {
      "adjacency_matrix" : {
        "filters" : {
          "grpA" : { "terms" : { "accounts" : ["hillary", "sidney"] }},
          "grpB" : { "terms" : { "accounts" : ["donald", "mitt"] }},
          "grpC" : { "terms" : { "accounts" : ["vladimir", "nigel"] }}
        }
      }
    }
  }
}
```

The response contains buckets with document counts for each filter and combination of filters. Buckets with no matching documents are excluded from the response.

```console-result
{
  "took": 9,
  "timed_out": false,
  "_shards": ...,
  "hits": ...,
  "aggregations": {
    "interactions": {
      "buckets": [
        {
          "key":"grpA",
          "doc_count": 2
        },
        {
          "key":"grpA&grpB",
          "doc_count": 1
        },
        {
          "key":"grpB",
          "doc_count": 2
        },
        {
          "key":"grpB&grpC",
          "doc_count": 1
        },
        {
          "key":"grpC",
          "doc_count": 1
        }
      ]
    }
  }
}
```


## Parameters [adjacency-matrix-agg-params]

`filters`
:   (Required, object) Filters used to create buckets.

::::{dropdown} Properties of filters
`<filter>`
:   (Required, [Query DSL object](/reference/query-languages/querydsl.md)) Query used to filter documents. The key is the filter name.

    At least one filter is required. The total number of filters cannot exceed the [`indices.query.bool.max_clause_count`](/reference/elasticsearch/configuration-reference/search-settings.md#indices-query-bool-max-clause-count) setting. See [Filter limits](#adjacency-matrix-agg-filter-limits).
::::


`separator`
:   (Optional, string) Separator used to concatenate filter names. Defaults to `&`.


## Response body [adjacency-matrix-agg-response]

`key`
:   (string) Filters for the bucket. If the bucket uses multiple filters, filter names are concatenated using a `separator`.

`doc_count`
:   (integer) Number of documents matching the bucket’s filters.


## Usage [adjacency-matrix-agg-usage]

On its own this aggregation can provide all of the data required to create an undirected weighted graph. However, when used with child aggregations such as a `date_histogram` the results can provide the additional levels of data required to perform [dynamic network analysis](https://en.wikipedia.org/wiki/Dynamic_network_analysis) where examining interactions *over time* becomes important.


## Filter limits [adjacency-matrix-agg-filter-limits]

For N filters the matrix of buckets produced can be N²/2 which can be costly. The circuit breaker settings prevent results producing too many buckets and to avoid excessive disk seeks the `indices.query.bool.max_clause_count` setting is used to limit the number of filters.


