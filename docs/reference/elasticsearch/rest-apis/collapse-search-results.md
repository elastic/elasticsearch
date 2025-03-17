---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/collapse-search-results.html
applies_to:
  stack: all
navigation_title: Collapse search results
---

# Collapse search results [collapse-search-results]

You can use the `collapse` parameter to collapse search results based on field values. The collapsing is done by selecting only the top sorted document per collapse key.

For example, the following search collapses results by `user.id` and sorts them by `http.response.bytes`.

```console
GET my-index-000001/_search
{
  "query": {
    "match": {
      "message": "GET /search"
    }
  },
  "collapse": {
    "field": "user.id"         <1>
  },
  "sort": [
    {
      "http.response.bytes": { <2>
        "order": "desc"
      }
    }
  ],
  "from": 0                    <3>
}
```

1. Collapse the result set using the `user.id` field
2. Sort the results by `http.response.bytes`
3. Define the offset of the first collapsed result


::::{warning}
The total number of hits in the response indicates the number of matching documents without collapsing. The total number of distinct group is unknown.
::::


The field used for collapsing must be a single valued [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) or [`numeric`](/reference/elasticsearch/mapping-reference/number.md) field with [`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md) activated.

::::{note}
Collapsing is applied to the top hits only and does not affect aggregations.
::::



## Expand collapse results [expand-collapse-results]

It is also possible to expand each collapsed top hits with the [`inner hits`](/reference/elasticsearch/rest-apis/retrieve-inner-hits.md) option.

```console
GET /my-index-000001/_search
{
  "query": {
    "match": {
      "message": "GET /search"
    }
  },
  "collapse": {
    "field": "user.id",                       <1>
    "inner_hits": {
      "name": "most_recent",                  <2>
      "size": 5,                              <3>
      "sort": [ { "@timestamp": "desc" } ]    <4>
    },
    "max_concurrent_group_searches": 4        <5>
  },
  "sort": [
    {
      "http.response.bytes": {
        "order": "desc"
      }
    }
  ]
}
```

1. Collapse the result set using the `user.id` field
2. The name used for the inner hit section in the response
3. The number of `inner_hits` to retrieve per collapse key
4. How to sort the document inside each group
5. The number of concurrent requests allowed to retrieve the `inner_hits` per group


See [inner hits](/reference/elasticsearch/rest-apis/retrieve-inner-hits.md) for the complete list of supported options and the format of the response.

It is also possible to request multiple [`inner hits`](/reference/elasticsearch/rest-apis/retrieve-inner-hits.md) for each collapsed hit. This can be useful when you want to get multiple representations of the collapsed hits.

```console
GET /my-index-000001/_search
{
  "query": {
    "match": {
      "message": "GET /search"
    }
  },
  "collapse": {
    "field": "user.id",                   <1>
    "inner_hits": [
      {
        "name": "largest_responses",      <2>
        "size": 3,
        "sort": [
          {
            "http.response.bytes": {
              "order": "desc"
            }
          }
        ]
      },
      {
        "name": "most_recent",             <3>
        "size": 3,
        "sort": [
          {
            "@timestamp": {
              "order": "desc"
            }
          }
        ]
      }
    ]
  },
  "sort": [
    "http.response.bytes"
  ]
}
```

1. Collapse the result set using the `user.id` field
2. Return the three largest HTTP responses for the user
3. Return the three most recent HTTP responses for the user


The expansion of the group is done by sending an additional query for each `inner_hit` request for each collapsed hit returned in the response. This can significantly slow your search if you have too many groups or `inner_hit` requests.

The `max_concurrent_group_searches` request parameter can be used to control the maximum number of concurrent searches allowed in this phase. The default is based on the number of data nodes and the default search thread pool size.

::::{warning}
`collapse` cannot be used in conjunction with [scroll](/reference/elasticsearch/rest-apis/paginate-search-results.md#scroll-search-results).
::::



## Collapsing with `search_after` [collapsing-with-search-after]

Field collapsing can be used with the [`search_after`](/reference/elasticsearch/rest-apis/paginate-search-results.md#search-after) parameter. Using `search_after` is only supported when sorting and collapsing on the same field. Secondary sorts are also not allowed. For example, we can collapse and sort on `user.id`, while paging through the results using `search_after`:

```console
GET /my-index-000001/_search
{
  "query": {
    "match": {
      "message": "GET /search"
    }
  },
  "collapse": {
    "field": "user.id"
  },
  "sort": [ "user.id" ],
  "search_after": ["dd5ce1ad"]
}
```


## Rescore collapse results [rescore-collapse-results]

You can use field collapsing alongside the [`rescore`](/reference/elasticsearch/rest-apis/filter-search-results.md#rescore) search parameter. Rescorers run on every shard for the top-ranked document per collapsed field. To maintain a reliable order, it is recommended to cluster documents sharing the same collapse field value on one shard. This is achieved by assigning the collapse field value as the [routing key](/reference/elasticsearch/rest-apis/search-shard-routing.md#search-routing) during indexing:

```console
POST /my-index-000001/_doc?routing=xyz      <1>
{
  "@timestamp": "2099-11-15T13:12:00",
  "message": "You know for search!",
  "user.id": "xyz"
}
```

1. Assign routing with the collapse field value (`user.id`).


By doing this, you guarantee that only one top document per collapse key gets rescored globally.

The following request utilizes field collapsing on the `user.id` field and then rescores the top groups with a [query rescorer](/reference/elasticsearch/rest-apis/filter-search-results.md#query-rescorer):

```console
GET /my-index-000001/_search
{
  "query": {
    "match": {
      "message": "you know for search"
    }
  },
  "collapse": {
    "field": "user.id"
  },
  "rescore" : {
      "window_size" : 50,
      "query" : {
         "rescore_query" : {
            "match_phrase": {
                "message": "you know for search"
            }
         },
         "query_weight" : 0.3,
         "rescore_query_weight" : 1.4
      }
   }
}
```

::::{warning}
Rescorers are not applied to [`inner hits`](/reference/elasticsearch/rest-apis/retrieve-inner-hits.md).
::::



## Second level of collapsing [second-level-of-collapsing]

A second level of collapsing is also supported and is applied to `inner_hits`.

For example, the following search collapses results by `geo.country_name`. Within each `geo.country_name`, inner hits are collapsed by `user.id`.

::::{note}
Second level of collapsing doesn’t allow `inner_hits`.
::::


```console
GET /my-index-000001/_search
{
  "query": {
    "match": {
      "message": "GET /search"
    }
  },
  "collapse": {
    "field": "geo.country_name",
    "inner_hits": {
      "name": "by_location",
      "collapse": { "field": "user.id" },
      "size": 3
    }
  }
}
```

```console-result
{
  "hits" : {
    "hits" : [
      {
        "_index" : "my-index-000001",
        "_id" : "oX9uXXoB0da05OCR3adK",
        "_score" : 0.5753642,
        "_source" : {
          "@timestamp" : "2099-11-15T14:12:12",
          "geo" : {
            "country_name" : "Amsterdam"
          },
          "http" : {
            "request" : {
              "method" : "get"
            },
            "response" : {
              "bytes" : 1070000,
              "status_code" : 200
            },
            "version" : "1.1"
          },
          "message" : "GET /search HTTP/1.1 200 1070000",
          "source" : {
            "ip" : "127.0.0.1"
          },
          "user" : {
            "id" : "kimchy"
          }
        },
        "fields" : {
          "geo.country_name" : [
            "Amsterdam"
          ]
        },
        "inner_hits" : {
          "by_location" : {
            "hits" : {
              "total" : {
                "value" : 1,
                "relation" : "eq"
              },
              "max_score" : 0.5753642,
              "hits" : [
                {
                  "_index" : "my-index-000001",
                  "_id" : "oX9uXXoB0da05OCR3adK",
                  "_score" : 0.5753642,
                  "_source" : {
                    "@timestamp" : "2099-11-15T14:12:12",
                    "geo" : {
                      "country_name" : "Amsterdam"
                    },
                    "http" : {
                      "request" : {
                        "method" : "get"
                      },
                      "response" : {
                        "bytes" : 1070000,
                        "status_code" : 200
                      },
                      "version" : "1.1"
                    },
                    "message" : "GET /search HTTP/1.1 200 1070000",
                    "source" : {
                      "ip" : "127.0.0.1"
                    },
                    "user" : {
                      "id" : "kimchy"
                    }
                  },
                  "fields" : {
                    "user.id" : [
                      "kimchy"
                    ]
                  }
                }
              ]
            }
          }
        }
      }
    ]
  }
}
```


## Track Scores [_track_scores_2]

When `collapse` is used with `sort` on a field, scores are not computed. Setting `track_scores` to true instructs {{es}} to compute and track scores.

