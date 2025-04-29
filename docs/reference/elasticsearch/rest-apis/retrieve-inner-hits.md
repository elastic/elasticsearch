---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/inner-hits.html
applies_to:
  stack: all
---

# Retrieve inner hits [inner-hits]

The [parent-join](/reference/elasticsearch/mapping-reference/parent-join.md) and [nested](/reference/elasticsearch/mapping-reference/nested.md) features allow the return of documents that have matches in a different scope. In the parent/child case, parent documents are returned based on matches in child documents or child documents are returned based on matches in parent documents. In the nested case, documents are returned based on matches in nested inner objects.

In both cases, the actual matches in the different scopes that caused a document to be returned are hidden. In many cases, it’s very useful to know which inner nested objects (in the case of nested) or children/parent documents (in the case of parent/child) caused certain information to be returned. The inner hits feature can be used for this. This feature returns per search hit in the search response additional nested hits that caused a search hit to match in a different scope.

Inner hits can be used by defining an `inner_hits` definition on a `nested`, `has_child` or `has_parent` query and filter. The structure looks like this:

```js
"<query>" : {
    "inner_hits" : {
        <inner_hits_options>
    }
}
```
% NOTCONSOLE

If `inner_hits` is defined on a query that supports it then each search hit will contain an `inner_hits` json object with the following structure:

```js
"hits": [
     {
        "_index": ...,
        "_type": ...,
        "_id": ...,
        "inner_hits": {
           "<inner_hits_name>": {
              "hits": {
                 "total": ...,
                 "hits": [
                    {
                       "_id": ...,
                       ...
                    },
                    ...
                 ]
              }
           }
        },
        ...
     },
     ...
]
```
% NOTCONSOLE


## Options [inner-hits-options]

Inner hits support the following options:

`from`
:   The offset from where the first hit to fetch for each `inner_hits` in the returned regular search hits.

`size`
:   The maximum number of hits to return per `inner_hits`. By default the top three matching hits are returned.

`sort`
:   How the inner hits should be sorted per `inner_hits`. By default the hits are sorted by the score.

`name`
:   The name to be used for the particular inner hit definition in the response. Useful when multiple inner hits have been defined in a single search request. The default depends in which query the inner hit is defined. For `has_child` query and filter this is the child type, `has_parent` query and filter this is the parent type and the nested query and filter this is the nested path.

Inner hits also supports the following per document features:

* [Highlighting](/reference/elasticsearch/rest-apis/highlighting.md)
* [Explain](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search)
* [Search fields](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#search-fields-param)
* [Source filtering](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#source-filtering)
* [Script fields](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#script-fields)
* [Doc value fields](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#docvalue-fields)
* [Include versions](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search)
* [Include Sequence Numbers and Primary Terms](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search)


## Nested inner hits [nested-inner-hits]

The nested `inner_hits` can be used to include nested inner objects as inner hits to a search hit.

```console
PUT test
{
  "mappings": {
    "properties": {
      "comments": {
        "type": "nested"
      }
    }
  }
}

PUT test/_doc/1?refresh
{
  "title": "Test title",
  "comments": [
    {
      "author": "kimchy",
      "number": 1
    },
    {
      "author": "nik9000",
      "number": 2
    }
  ]
}

POST test/_search
{
  "query": {
    "nested": {
      "path": "comments",
      "query": {
        "match": {"comments.number" : 2}
      },
      "inner_hits": {} <1>
    }
  }
}
```

1. The inner hit definition in the nested query. No other options need to be defined.


An example of a response snippet that could be generated from the above search request:

```console-result
{
  ...,
  "hits": {
    "total" : {
        "value": 1,
        "relation": "eq"
    },
    "max_score": 1.0,
    "hits": [
      {
        "_index": "test",
        "_id": "1",
        "_score": 1.0,
        "_source": ...,
        "inner_hits": {
          "comments": { <1>
            "hits": {
              "total" : {
                  "value": 1,
                  "relation": "eq"
              },
              "max_score": 1.0,
              "hits": [
                {
                  "_index": "test",
                  "_id": "1",
                  "_nested": {
                    "field": "comments",
                    "offset": 1
                  },
                  "_score": 1.0,
                  "_source": {
                    "author": "nik9000",
                    "number": 2
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
% TESTRESPONSE[s/"_source": \.\.\./"_source": $body.hits.hits.0._source/]
% TESTRESPONSE[s/\.\.\./"timed_out": false, "took": $body.took, "_shards": $body._shards/]

1. The name used in the inner hit definition in the search request. A custom key can be used via the `name` option.


The `_nested` metadata is crucial in the above example, because it defines from what inner nested object this inner hit came from. The `field` defines the object array field the nested hit is from and the `offset` relative to its location in the `_source`. Due to sorting and scoring the actual location of the hit objects in the `inner_hits` is usually different than the location a nested inner object was defined.

By default the `_source` is returned also for the hit objects in `inner_hits`, but this can be changed. Either via `_source` filtering feature part of the source can be returned or be disabled. If stored fields are defined on the nested level these can also be returned via the `fields` feature.

An important default is that the `_source` returned in hits inside `inner_hits` is relative to the `_nested` metadata. So in the above example only the comment part is returned per nested hit and not the entire source of the top level document that contained the comment.


### Nested inner hits and `_source` [nested-inner-hits-source]

Nested document don’t have a `_source` field, because the entire source of document is stored with the root document under its `_source` field. To include the source of just the nested document, the source of the root document is parsed and just the relevant bit for the nested document is included as source in the inner hit. Doing this for each matching nested document has an impact on the time it takes to execute the entire search request, especially when `size` and the inner hits' `size` are set higher than the default. To avoid the relatively expensive source extraction for nested inner hits, one can disable including the source and solely rely on doc values fields. Like this:

```console
PUT test
{
  "mappings": {
    "properties": {
      "comments": {
        "type": "nested"
      }
    }
  }
}

PUT test/_doc/1?refresh
{
  "title": "Test title",
  "comments": [
    {
      "author": "kimchy",
      "text": "comment text"
    },
    {
      "author": "nik9000",
      "text": "words words words"
    }
  ]
}

POST test/_search
{
  "query": {
    "nested": {
      "path": "comments",
      "query": {
        "match": {"comments.text" : "words"}
      },
      "inner_hits": {
        "_source" : false,
        "docvalue_fields" : [
          "comments.text.keyword"
        ]
      }
    }
  }
}
```


## Hierarchical levels of nested object fields and inner hits. [hierarchical-nested-inner-hits]

If a mapping has multiple levels of hierarchical nested object fields each level can be accessed via dot notated path. For example if there is a `comments` nested field that contains a `votes` nested field and votes should directly be returned with the root hits then the following path can be defined:

```console
PUT test
{
  "mappings": {
    "properties": {
      "comments": {
        "type": "nested",
        "properties": {
          "votes": {
            "type": "nested"
          }
        }
      }
    }
  }
}

PUT test/_doc/1?refresh
{
  "title": "Test title",
  "comments": [
    {
      "author": "kimchy",
      "text": "comment text",
      "votes": []
    },
    {
      "author": "nik9000",
      "text": "words words words",
      "votes": [
        {"value": 1 , "voter": "kimchy"},
        {"value": -1, "voter": "other"}
      ]
    }
  ]
}

POST test/_search
{
  "query": {
    "nested": {
      "path": "comments.votes",
        "query": {
          "match": {
            "comments.votes.voter": "kimchy"
          }
        },
        "inner_hits" : {}
    }
  }
}
```

Which would look like:

```console-result
{
  ...,
  "hits": {
    "total" : {
        "value": 1,
        "relation": "eq"
    },
    "max_score": 0.6931471,
    "hits": [
      {
        "_index": "test",
        "_id": "1",
        "_score": 0.6931471,
        "_source": ...,
        "inner_hits": {
          "comments.votes": {
            "hits": {
              "total" : {
                  "value": 1,
                  "relation": "eq"
              },
              "max_score": 0.6931471,
              "hits": [
                {
                  "_index": "test",
                  "_id": "1",
                  "_nested": {
                    "field": "comments",
                    "offset": 1,
                    "_nested": {
                      "field": "votes",
                      "offset": 0
                    }
                  },
                  "_score": 0.6931471,
                  "_source": {
                    "value": 1,
                    "voter": "kimchy"
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

This indirect referencing is only supported for nested inner hits.


## Parent/child inner hits [parent-child-inner-hits]

The parent/child `inner_hits` can be used to include parent or child:

```console
PUT test
{
  "mappings": {
    "properties": {
      "my_join_field": {
        "type": "join",
        "relations": {
          "my_parent": "my_child"
        }
      }
    }
  }
}

PUT test/_doc/1?refresh
{
  "number": 1,
  "my_join_field": "my_parent"
}

PUT test/_doc/2?routing=1&refresh
{
  "number": 1,
  "my_join_field": {
    "name": "my_child",
    "parent": "1"
  }
}

POST test/_search
{
  "query": {
    "has_child": {
      "type": "my_child",
      "query": {
        "match": {
          "number": 1
        }
      },
      "inner_hits": {}    <1>
    }
  }
}
```

1. The inner hit definition like in the nested example.


An example of a response snippet that could be generated from the above search request:

```console-result
{
  ...,
  "hits": {
    "total": {
      "value": 1,
      "relation": "eq"
    },
    "max_score": 1.0,
    "hits": [
      {
        "_index": "test",
        "_id": "1",
        "_score": 1.0,
        "_source": {
          "number": 1,
          "my_join_field": "my_parent"
        },
        "inner_hits": {
          "my_child": {
            "hits": {
              "total": {
                "value": 1,
                "relation": "eq"
              },
              "max_score": 1.0,
              "hits": [
                {
                  "_index": "test",
                  "_id": "2",
                  "_score": 1.0,
                  "_routing": "1",
                  "_source": {
                    "number": 1,
                    "my_join_field": {
                      "name": "my_child",
                      "parent": "1"
                    }
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
% TESTRESPONSE[s/"_source": \.\.\./"_source": $body.hits.hits.0._source/]
% TESTRESPONSE[s/\.\.\./"timed_out": false, "took": $body.took, "_shards": $body._shards/]

