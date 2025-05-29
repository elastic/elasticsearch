---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/filter-search-results.html
applies_to:
  stack: all
---

# Filter search results [filter-search-results]

You can use two methods to filter search results:

* Use a boolean query with a `filter` clause. Search requests apply [boolean filters](/reference/query-languages/query-dsl/query-dsl-bool-query.md) to both search hits and [aggregations](/reference/aggregations/index.md).
* Use the search API’s `post_filter` parameter. Search requests apply [post filters](#post-filter) only to search hits, not aggregations. You can use a post filter to calculate aggregations based on a broader result set, and then further narrow the results.

    You can also [rescore](#rescore) hits after the post filter to improve relevance and reorder results.



## Post filter [post-filter]

When you use the `post_filter` parameter to filter search results, the search hits are filtered after the aggregations are calculated. A post filter has no impact on the aggregation results.

For example, you are selling shirts that have the following properties:

```console
PUT /shirts
{
  "mappings": {
    "properties": {
      "brand": { "type": "keyword"},
      "color": { "type": "keyword"},
      "model": { "type": "keyword"}
    }
  }
}

PUT /shirts/_doc/1?refresh
{
  "brand": "gucci",
  "color": "red",
  "model": "slim"
}
```
% TESTSETUP

Imagine a user has specified two filters:

`color:red` and `brand:gucci`. You only want to show them red shirts made by Gucci in the search results. Normally you would do this with a [`bool` query](/reference/query-languages/query-dsl/query-dsl-bool-query.md):

```console
GET /shirts/_search
{
  "query": {
    "bool": {
      "filter": [
        { "term": { "color": "red"   }},
        { "term": { "brand": "gucci" }}
      ]
    }
  }
}
```

However, you would also like to use *faceted navigation* to display a list of other options that the user could click on. Perhaps you have a `model` field that would allow the user to limit their search results to red Gucci `t-shirts` or `dress-shirts`.

This can be done with a [`terms` aggregation](/reference/aggregations/search-aggregations-bucket-terms-aggregation.md):

```console
GET /shirts/_search
{
  "query": {
    "bool": {
      "filter": [
        { "term": { "color": "red"   }},
        { "term": { "brand": "gucci" }}
      ]
    }
  },
  "aggs": {
    "models": {
      "terms": { "field": "model" } <1>
    }
  }
}
```

1. Returns the most popular models of red shirts by Gucci.


But perhaps you would also like to tell the user how many Gucci shirts are available in **other colors**. If you just add a `terms` aggregation on the `color` field, you will only get back the color `red`, because your query returns only red shirts by Gucci.

Instead, you want to include shirts of all colors during aggregation, then apply the `colors` filter only to the search results. This is the purpose of the `post_filter`:

```console
GET /shirts/_search
{
  "query": {
    "bool": {
      "filter": {
        "term": { "brand": "gucci" } <1>
      }
    }
  },
  "aggs": {
    "colors": {
      "terms": { "field": "color" } <2>
    },
    "color_red": {
      "filter": {
        "term": { "color": "red" } <3>
      },
      "aggs": {
        "models": {
          "terms": { "field": "model" } <3>
        }
      }
    }
  },
  "post_filter": { <4>
    "term": { "color": "red" }
  }
}
```

1. The main query now finds all shirts by Gucci, regardless of color.
2. The `colors` agg returns popular colors for shirts by Gucci.
3. The `color_red` agg limits the `models` sub-aggregation to **red** Gucci shirts.
4. Finally, the `post_filter` removes colors other than red from the search `hits`.



## Rescore filtered search results [rescore]

Rescoring can help to improve precision by reordering just the top (eg 100 - 500) documents returned by the [`query`](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search) and [`post_filter`](#post-filter) phases, using a secondary (usually more costly) algorithm, instead of applying the costly algorithm to all documents in the index.

A `rescore` request is executed on each shard before it returns its results to be sorted by the node handling the overall search request.

Currently the rescore API has only one implementation: the query rescorer, which uses a query to tweak the scoring. In the future, alternative rescorers may be made available, for example, a pair-wise rescorer.

::::{note}
An error will be thrown if an explicit [`sort`](/reference/elasticsearch/rest-apis/sort-search-results.md) (other than `_score` in descending order) is provided with a `rescore` query.
::::


::::{note}
when exposing pagination to your users, you should not change `window_size` as you step through each page (by passing different `from` values) since that can alter the top hits causing results to confusingly shift as the user steps through pages.
::::



### Query rescorer [query-rescorer]

The query rescorer executes a second query only on the Top-K results returned by the [`query`](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search) and [`post_filter`](#post-filter) phases. The number of docs which will be examined on each shard can be controlled by the `window_size` parameter, which defaults to 10.

By default the scores from the original query and the rescore query are combined linearly to produce the final `_score` for each document. The relative importance of the original query and of the rescore query can be controlled with the `query_weight` and `rescore_query_weight` respectively. Both default to `1`.

For example:

```console
POST /_search
{
   "query" : {
      "match" : {
         "message" : {
            "operator" : "or",
            "query" : "the quick brown"
         }
      }
   },
   "rescore" : {
      "window_size" : 50,
      "query" : {
         "rescore_query" : {
            "match_phrase" : {
               "message" : {
                  "query" : "the quick brown",
                  "slop" : 2
               }
            }
         },
         "query_weight" : 0.7,
         "rescore_query_weight" : 1.2
      }
   }
}
```
% TEST[setup:my_index]

The way the scores are combined can be controlled with the `score_mode`:

| Score Mode | Description |
| --- | --- |
| `total` | Add the original score and the rescore query score. The default. |
| `multiply` | Multiply the original score by the rescore query score. Usefulfor [`function query`](/reference/query-languages/query-dsl/query-dsl-function-score-query.md) rescores. |
| `avg` | Average the original score and the rescore query score. |
| `max` | Take the max of original score and the rescore query score. |
| `min` | Take the min of the original score and the rescore query score. |


### Multiple rescores [multiple-rescores]

It is also possible to execute multiple rescores in sequence:

```console
POST /_search
{
   "query" : {
      "match" : {
         "message" : {
            "operator" : "or",
            "query" : "the quick brown"
         }
      }
   },
   "rescore" : [ {
      "window_size" : 100,
      "query" : {
         "rescore_query" : {
            "match_phrase" : {
               "message" : {
                  "query" : "the quick brown",
                  "slop" : 2
               }
            }
         },
         "query_weight" : 0.7,
         "rescore_query_weight" : 1.2
      }
   }, {
      "window_size" : 10,
      "query" : {
         "score_mode": "multiply",
         "rescore_query" : {
            "function_score" : {
               "script_score": {
                  "script": {
                    "source": "Math.log10(doc.count.value + 2)"
                  }
               }
            }
         }
      }
   } ]
}
```
% TEST[setup:my_index]

The first one gets the results of the query then the second one gets the results of the first, etc. The second rescore will "see" the sorting done by the first rescore so it is possible to use a large window on the first rescore to pull documents into a smaller window for the second rescore.

