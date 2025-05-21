---
navigation_title: "Top hits"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-top-hits-aggregation.html
---

# Top hits aggregation [search-aggregations-metrics-top-hits-aggregation]


A `top_hits` metric aggregator keeps track of the most relevant document being aggregated. This aggregator is intended to be used as a sub aggregator, so that the top matching documents can be aggregated per bucket.

::::{tip}
We do not recommend using `top_hits` as a top-level aggregation. If you want to group search hits, use the [`collapse`](/reference/elasticsearch/rest-apis/collapse-search-results.md) parameter instead.
::::


The `top_hits` aggregator can effectively be used to group result sets by certain fields via a bucket aggregator. One or more bucket aggregators determines by which properties a result set get sliced into.

## Options [_options_6]

* `from` - The offset from the first result you want to fetch.
* `size` - The maximum number of top matching hits to return per bucket. By default the top three matching hits are returned.
* `sort` - How the top matching hits should be sorted. By default the hits are sorted by the score of the main query.


## Supported per hit features [_supported_per_hit_features]

The top_hits aggregation returns regular search hits, because of this many per hit features can be supported:

* [Highlighting](/reference/elasticsearch/rest-apis/highlighting.md)
* [Explain](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search)
* [Named queries](/reference/query-languages/query-dsl/query-dsl-bool-query.md#named-queries)
* [Search fields](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#search-fields-param)
* [Source filtering](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#source-filtering)
* [Stored fields](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#stored-fields)
* [Script fields](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#script-fields)
* [Doc value fields](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#docvalue-fields)
* [Include versions](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search)
* [Include Sequence Numbers and Primary Terms](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search)

::::{important}
If you **only** need `docvalue_fields`, `size`, and `sort` then [Top metrics](/reference/aggregations/search-aggregations-metrics-top-metrics.md) might be a more efficient choice than the Top Hits Aggregation.
::::


`top_hits` does not support the [`rescore`](/reference/elasticsearch/rest-apis/filter-search-results.md#rescore) parameter. Query rescoring applies only to search hits, not aggregation results. To change the scores used by aggregations, use a [`function_score`](/reference/query-languages/query-dsl/query-dsl-function-score-query.md) or [`script_score`](/reference/query-languages/query-dsl/query-dsl-script-score-query.md) query.


## Example [_example_6]

In the following example we group the sales by type and per type we show the last sale. For each sale only the date and price fields are being included in the source.

```console
POST /sales/_search?size=0
{
  "aggs": {
    "top_tags": {
      "terms": {
        "field": "type",
        "size": 3
      },
      "aggs": {
        "top_sales_hits": {
          "top_hits": {
            "sort": [
              {
                "date": {
                  "order": "desc"
                }
              }
            ],
            "_source": {
              "includes": [ "date", "price" ]
            },
            "size": 1
          }
        }
      }
    }
  }
}
```

Possible response:

```console-result
{
  ...
  "aggregations": {
    "top_tags": {
       "doc_count_error_upper_bound": 0,
       "sum_other_doc_count": 0,
       "buckets": [
          {
             "key": "hat",
             "doc_count": 3,
             "top_sales_hits": {
                "hits": {
                   "total" : {
                       "value": 3,
                       "relation": "eq"
                   },
                   "max_score": null,
                   "hits": [
                      {
                         "_index": "sales",
                         "_id": "AVnNBmauCQpcRyxw6ChK",
                         "_source": {
                            "date": "2015/03/01 00:00:00",
                            "price": 200
                         },
                         "sort": [
                            1425168000000
                         ],
                         "_score": null
                      }
                   ]
                }
             }
          },
          {
             "key": "t-shirt",
             "doc_count": 3,
             "top_sales_hits": {
                "hits": {
                   "total" : {
                       "value": 3,
                       "relation": "eq"
                   },
                   "max_score": null,
                   "hits": [
                      {
                         "_index": "sales",
                         "_id": "AVnNBmauCQpcRyxw6ChL",
                         "_source": {
                            "date": "2015/03/01 00:00:00",
                            "price": 175
                         },
                         "sort": [
                            1425168000000
                         ],
                         "_score": null
                      }
                   ]
                }
             }
          },
          {
             "key": "bag",
             "doc_count": 1,
             "top_sales_hits": {
                "hits": {
                   "total" : {
                       "value": 1,
                       "relation": "eq"
                   },
                   "max_score": null,
                   "hits": [
                      {
                         "_index": "sales",
                         "_id": "AVnNBmatCQpcRyxw6ChH",
                         "_source": {
                            "date": "2015/01/01 00:00:00",
                            "price": 150
                         },
                         "sort": [
                            1420070400000
                         ],
                         "_score": null
                      }
                   ]
                }
             }
          }
       ]
    }
  }
}
```


## Field collapse example [_field_collapse_example]

Field collapsing or result grouping is a feature that logically groups a result set into groups and per group returns top documents. The ordering of the groups is determined by the relevancy of the first document in a group. In Elasticsearch this can be implemented via a bucket aggregator that wraps a `top_hits` aggregator as sub-aggregator.

In the example below we search across crawled webpages. For each webpage we store the body and the domain the webpage belong to. By defining a `terms` aggregator on the `domain` field we group the result set of webpages by domain. The `top_hits` aggregator is then defined as sub-aggregator, so that the top matching hits are collected per bucket.

Also a `max` aggregator is defined which is used by the `terms` aggregator’s order feature to return the buckets by relevancy order of the most relevant document in a bucket.

```console
POST /sales/_search
{
  "query": {
    "match": {
      "body": "elections"
    }
  },
  "aggs": {
    "top_sites": {
      "terms": {
        "field": "domain",
        "order": {
          "top_hit": "desc"
        }
      },
      "aggs": {
        "top_tags_hits": {
          "top_hits": {}
        },
        "top_hit" : {
          "max": {
            "script": {
              "source": "_score"
            }
          }
        }
      }
    }
  }
}
```

At the moment the `max` (or `min`) aggregator is needed to make sure the buckets from the `terms` aggregator are ordered according to the score of the most relevant webpage per domain. Unfortunately the `top_hits` aggregator can’t be used in the `order` option of the `terms` aggregator yet.


## top_hits support in a nested or reverse_nested aggregator [_top_hits_support_in_a_nested_or_reverse_nested_aggregator]

If the `top_hits` aggregator is wrapped in a `nested` or `reverse_nested` aggregator then nested hits are being returned. Nested hits are in a sense hidden mini documents that are part of regular document where in the mapping a nested field type has been configured. The `top_hits` aggregator has the ability to un-hide these documents if it is wrapped in a `nested` or `reverse_nested` aggregator. Read more about nested in the [nested type mapping](/reference/elasticsearch/mapping-reference/nested.md).

If nested type has been configured a single document is actually indexed as multiple Lucene documents and they share the same id. In order to determine the identity of a nested hit there is more needed than just the id, so that is why nested hits also include their nested identity. The nested identity is kept under the `_nested` field in the search hit and includes the array field and the offset in the array field the nested hit belongs to. The offset is zero based.

Let’s see how it works with a real sample. Considering the following mapping:

```console
PUT /sales
{
  "mappings": {
    "properties": {
      "tags": { "type": "keyword" },
      "comments": {                           <1>
        "type": "nested",
        "properties": {
          "username": { "type": "keyword" },
          "comment": { "type": "text" }
        }
      }
    }
  }
}
```

1. The `comments` is an array that holds nested documents under the `product` object.


And some documents:

```console
PUT /sales/_doc/1?refresh
{
  "tags": [ "car", "auto" ],
  "comments": [
    { "username": "baddriver007", "comment": "This car could have better brakes" },
    { "username": "dr_who", "comment": "Where's the autopilot? Can't find it" },
    { "username": "ilovemotorbikes", "comment": "This car has two extra wheels" }
  ]
}
```

It’s now possible to execute the following `top_hits` aggregation (wrapped in a `nested` aggregation):

```console
POST /sales/_search
{
  "query": {
    "term": { "tags": "car" }
  },
  "aggs": {
    "by_sale": {
      "nested": {
        "path": "comments"
      },
      "aggs": {
        "by_user": {
          "terms": {
            "field": "comments.username",
            "size": 1
          },
          "aggs": {
            "by_nested": {
              "top_hits": {}
            }
          }
        }
      }
    }
  }
}
```

Top hits response snippet with a nested hit, which resides in the first slot of array field `comments`:

```console-result
{
  ...
  "aggregations": {
    "by_sale": {
      "by_user": {
        "buckets": [
          {
            "key": "baddriver007",
            "doc_count": 1,
            "by_nested": {
              "hits": {
                "total" : {
                   "value": 1,
                   "relation": "eq"
                },
                "max_score": 0.3616575,
                "hits": [
                  {
                    "_index": "sales",
                    "_id": "1",
                    "_nested": {
                      "field": "comments",  <1>
                      "offset": 0 <2>
                    },
                    "_score": 0.3616575,
                    "_source": {
                      "comment": "This car could have better brakes", <3>
                      "username": "baddriver007"
                    }
                  }
                ]
              }
            }
          }
          ...
        ]
      }
    }
  }
}
```

1. Name of the array field containing the nested hit
2. Position if the nested hit in the containing array
3. Source of the nested hit


If `_source` is requested then just the part of the source of the nested object is returned, not the entire source of the document. Also stored fields on the **nested** inner object level are accessible via `top_hits` aggregator residing in a `nested` or `reverse_nested` aggregator.

Only nested hits will have a `_nested` field in the hit, non nested (regular) hits will not have a `_nested` field.

The information in `_nested` can also be used to parse the original source somewhere else if `_source` isn’t enabled.

If there are multiple levels of nested object types defined in mappings then the `_nested` information can also be hierarchical in order to express the identity of nested hits that are two layers deep or more.

In the example below a nested hit resides in the first slot of the field `nested_grand_child_field` which then resides in the second slow of the `nested_child_field` field:

```js
...
"hits": {
 "total" : {
     "value": 2565,
     "relation": "eq"
 },
 "max_score": 1,
 "hits": [
   {
     "_index": "a",
     "_id": "1",
     "_score": 1,
     "_nested" : {
       "field" : "nested_child_field",
       "offset" : 1,
       "_nested" : {
         "field" : "nested_grand_child_field",
         "offset" : 0
       }
     }
     "_source": ...
   },
   ...
 ]
}
...
```


## Use in pipeline aggregations [_use_in_pipeline_aggregations]

`top_hits` can be used in pipeline aggregations that consume a single value per bucket, such as `bucket_selector` that applies per bucket filtering, similar to using a HAVING clause in SQL. This requires setting `size` to 1, and specifying the right path for the value to be passed to the wrapping aggregator. The latter can be a `_source`, a `_sort` or a `_score` value. For example:

```console
POST /sales/_search?size=0
{
  "aggs": {
    "top_tags": {
      "terms": {
        "field": "type",
        "size": 3
      },
      "aggs": {
        "top_sales_hits": {
          "top_hits": {
            "sort": [
              {
                "date": {
                  "order": "desc"
                }
              }
            ],
            "_source": {
              "includes": [ "date", "price" ]
            },
            "size": 1
          }
        },
        "having.top_salary": {
          "bucket_selector": {
            "buckets_path": {
              "tp": "top_sales_hits[_source.price]"
            },
            "script": "params.tp < 180"
          }
        }
      }
    }
  }
}
```

The `bucket_path` uses the `top_hits` name `top_sales_hits` and a keyword for the field providing the aggregate value, namely `_source` field `price` in the example above. Other options include `top_sales_hits[_sort]`, for filtering on the sort value `date` above, and `top_sales_hits[_score]`, for filtering on the score of the top hit.


