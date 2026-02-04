---
navigation_title: "Percolate"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-percolate-query.html
---

# Percolate query [query-dsl-percolate-query]


The `percolate` query can be used to match queries stored in an index. The `percolate` query itself contains the document that will be used as query to match with the stored queries.

## Sample usage [_sample_usage]

::::{tip}
To provide a simple example, this documentation uses one index, `my-index-000001`, for both the percolate queries and documents. This setup can work well when there are just a few percolate queries registered. For heavier usage, we recommend you store queries and documents in separate indices. For more details, refer to [How it Works Under the Hood](#how-it-works).
::::


Create an index with two fields:

```console
PUT /my-index-000001
{
  "mappings": {
    "properties": {
      "message": {
        "type": "text"
      },
      "query": {
        "type": "percolator"
      }
    }
  }
}
```

The `message` field is the field used to preprocess the document defined in the `percolator` query before it gets indexed into a temporary index.

The `query` field is used for indexing the query documents. It will hold a json object that represents an actual Elasticsearch query. The `query` field has been configured to use the [percolator field type](/reference/elasticsearch/mapping-reference/percolator.md). This field type understands the query dsl and stores the query in such a way that it can be used later on to match documents defined on the `percolate` query.

Register a query in the percolator:

```console
PUT /my-index-000001/_doc/1?refresh
{
  "query": {
    "match": {
      "message": "bonsai tree"
    }
  }
}
```
% TEST[continued]

Match a document to the registered percolator queries:

```console
GET /my-index-000001/_search
{
  "query": {
    "percolate": {
      "field": "query",
      "document": {
        "message": "A new bonsai tree in the office"
      }
    }
  }
}
```
% TEST[continued]

The above request will yield the following response:

```console-result
{
  "took": 13,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped" : 0,
    "failed": 0
  },
  "hits": {
    "total" : {
        "value": 1,
        "relation": "eq"
    },
    "max_score": 0.26152915,
    "hits": [
      { <1>
        "_index": "my-index-000001",
        "_id": "1",
        "_score": 0.26152915,
        "_source": {
          "query": {
            "match": {
              "message": "bonsai tree"
            }
          }
        },
        "fields" : {
          "_percolator_document_slot" : [0] <2>
        }
      }
    ]
  }
}
```
% TESTRESPONSE[s/"took": 13,/"took": "$body.took",/]

1. The query with id `1` matches our document.
2. The `_percolator_document_slot` field indicates which document has matched with this query. Useful when percolating multiple document simultaneously.



## Parameters [_parameters_3]

The following parameters are required when percolating a document:

`field`
:   The field of type `percolator` that holds the indexed queries. This is a required parameter.

`name`
:   The suffix to be used for the `_percolator_document_slot` field in case multiple `percolate` queries have been specified. This is an optional parameter.

`document`
:   The source of the document being percolated.

`documents`
:   Like the `document` parameter, but accepts multiple documents via a json array.

`document_type`
:   The type / mapping of the document being percolated. This parameter is deprecated and will be removed in Elasticsearch 8.0.

Instead of specifying the source of the document being percolated, the source can also be retrieved from an already stored document. The `percolate` query will then internally execute a get request to fetch that document.

In that case the `document` parameter can be substituted with the following parameters:

`index`
:   The index the document resides in. This is a required parameter.

`type`
:   The type of the document to fetch. This parameter is deprecated and will be removed in Elasticsearch 8.0.

`id`
:   The id of the document to fetch. This is a required parameter.

`routing`
:   Optionally, routing to be used to fetch document to percolate.

`preference`
:   Optionally, preference to be used to fetch document to percolate.

`version`
:   Optionally, the expected version of the document to be fetched.


## Percolating in a filter context [_percolating_in_a_filter_context]

In case you are not interested in the score, better performance can be expected by wrapping the percolator query in a `bool` query’s filter clause or in a `constant_score` query:

```console
GET /my-index-000001/_search
{
  "query": {
    "constant_score": {
      "filter": {
        "percolate": {
          "field": "query",
          "document": {
            "message": "A new bonsai tree in the office"
          }
        }
      }
    }
  }
}
```
% TEST[continued]

At index time terms are extracted from the percolator query and the percolator can often determine whether a query matches just by looking at those extracted terms. However, computing scores requires to deserialize each matching query and run it against the percolated document, which is a much more expensive operation. Hence if computing scores is not required the `percolate` query should be wrapped in a `constant_score` query or a `bool` query’s filter clause.

Note that the `percolate` query never gets cached by the query cache.


## Percolating multiple documents [_percolating_multiple_documents]

The `percolate` query can match multiple documents simultaneously with the indexed percolator queries. Percolating multiple documents in a single request can improve performance as queries only need to be parsed and matched once instead of multiple times.

The `_percolator_document_slot` field that is being returned with each matched percolator query is important when percolating multiple documents simultaneously. It indicates which documents matched with a particular percolator query. The numbers correlate with the slot in the `documents` array specified in the `percolate` query.

```console
GET /my-index-000001/_search
{
  "query": {
    "percolate": {
      "field": "query",
      "documents": [ <1>
        {
          "message": "bonsai tree"
        },
        {
          "message": "new tree"
        },
        {
          "message": "the office"
        },
        {
          "message": "office tree"
        }
      ]
    }
  }
}
```
% TEST[continued]

1. The documents array contains 4 documents that are going to be percolated at the same time.


```console-result
{
  "took": 13,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped" : 0,
    "failed": 0
  },
  "hits": {
    "total" : {
        "value": 1,
        "relation": "eq"
    },
    "max_score": 0.7093853,
    "hits": [
      {
        "_index": "my-index-000001",
        "_id": "1",
        "_score": 0.7093853,
        "_source": {
          "query": {
            "match": {
              "message": "bonsai tree"
            }
          }
        },
        "fields" : {
          "_percolator_document_slot" : [0, 1, 3] <1>
        }
      }
    ]
  }
}
```
% TESTRESPONSE[s/"took": 13,/"took": "$body.took",/]

1. The `_percolator_document_slot` indicates that the first, second and last documents specified in the `percolate` query are matching with this query.



## Percolating an existing document [_percolating_an_existing_document]

In order to percolate a newly indexed document, the `percolate` query can be used. Based on the response from an index request, the `_id` and other meta information can be used to immediately percolate the newly added document.

### Example [_example_3]

Based on the previous example.

Index the document we want to percolate:

```console
PUT /my-index-000001/_doc/2
{
  "message" : "A new bonsai tree in the office"
}
```
% TEST[continued]

Index response:

```console-result
{
  "_index": "my-index-000001",
  "_id": "2",
  "_version": 1,
  "_shards": {
    "total": 2,
    "successful": 1,
    "failed": 0
  },
  "result": "created",
  "_seq_no" : 1,
  "_primary_term" : 1
}
```

Percolating an existing document, using the index response as basis to build to new search request:

```console
GET /my-index-000001/_search
{
  "query": {
    "percolate": {
      "field": "query",
      "index": "my-index-000001",
      "id": "2",
      "version": 1 <1>
    }
  }
}
```
% TEST[continued]

1. The version is optional, but useful in certain cases. We can ensure that we are trying to percolate the document we just have indexed. A change may be made after we have indexed, and if that is the case the search request would fail with a version conflict error.


The search response returned is identical as in the previous example.



## Percolate query and highlighting [_percolate_query_and_highlighting]

The `percolate` query is handled in a special way when it comes to highlighting. The queries hits are used to highlight the document that is provided in the `percolate` query. Whereas with regular highlighting the query in the search request is used to highlight the hits.

### Example [_example_4]

This example is based on the mapping of the first example.

Save a query:

```console
PUT /my-index-000001/_doc/3?refresh
{
  "query": {
    "match": {
      "message": "brown fox"
    }
  }
}
```
% TEST[continued]

Save another query:

```console
PUT /my-index-000001/_doc/4?refresh
{
  "query": {
    "match": {
      "message": "lazy dog"
    }
  }
}
```
% TEST[continued]

Execute a search request with the `percolate` query and highlighting enabled:

```console
GET /my-index-000001/_search
{
  "query": {
    "percolate": {
      "field": "query",
      "document": {
        "message": "The quick brown fox jumps over the lazy dog"
      }
    }
  },
  "highlight": {
    "fields": {
      "message": {}
    }
  }
}
```
% TEST[continued]

This will yield the following response.

```console-result
{
  "took": 7,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped" : 0,
    "failed": 0
  },
  "hits": {
    "total" : {
        "value": 2,
        "relation": "eq"
    },
    "max_score": 0.26152915,
    "hits": [
      {
        "_index": "my-index-000001",
        "_id": "3",
        "_score": 0.26152915,
        "_source": {
          "query": {
            "match": {
              "message": "brown fox"
            }
          }
        },
        "highlight": {
          "message": [
            "The quick <em>brown</em> <em>fox</em> jumps over the lazy dog" <1>
          ]
        },
        "fields" : {
          "_percolator_document_slot" : [0]
        }
      },
      {
        "_index": "my-index-000001",
        "_id": "4",
        "_score": 0.26152915,
        "_source": {
          "query": {
            "match": {
              "message": "lazy dog"
            }
          }
        },
        "highlight": {
          "message": [
            "The quick brown fox jumps over the <em>lazy</em> <em>dog</em>" <1>
          ]
        },
        "fields" : {
          "_percolator_document_slot" : [0]
        }
      }
    ]
  }
}
```
% TESTRESPONSE[s/"took": 7,/"took": "$body.took",/]

1. The terms from each query have been highlighted in the document.


Instead of the query in the search request highlighting the percolator hits, the percolator queries are highlighting the document defined in the `percolate` query.

When percolating multiple documents at the same time like the request below then the highlight response is different:

```console
GET /my-index-000001/_search
{
  "query": {
    "percolate": {
      "field": "query",
      "documents": [
        {
          "message": "bonsai tree"
        },
        {
          "message": "new tree"
        },
        {
          "message": "the office"
        },
        {
          "message": "office tree"
        }
      ]
    }
  },
  "highlight": {
    "fields": {
      "message": {}
    }
  }
}
```
% TEST[continued]

The slightly different response:

```console-result
{
  "took": 13,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped" : 0,
    "failed": 0
  },
  "hits": {
    "total" : {
        "value": 1,
        "relation": "eq"
    },
    "max_score": 0.7093853,
    "hits": [
      {
        "_index": "my-index-000001",
        "_id": "1",
        "_score": 0.7093853,
        "_source": {
          "query": {
            "match": {
              "message": "bonsai tree"
            }
          }
        },
        "fields" : {
          "_percolator_document_slot" : [0, 1, 3]
        },
        "highlight" : { <1>
          "0_message" : [
              "<em>bonsai</em> <em>tree</em>"
          ],
          "3_message" : [
              "office <em>tree</em>"
          ],
          "1_message" : [
              "new <em>tree</em>"
          ]
        }
      }
    ]
  }
}
```
% TESTRESPONSE[s/"took": 13,/"took": "$body.took",/]

1. The highlight fields have been prefixed with the document slot they belong to, in order to know which highlight field belongs to what document.




## Named queries within percolator queries [_named_queries_within_percolator_queries]

If a stored percolator query is a complex query, and you want to track which its sub-queries matched a percolated document, then you can use the `\_name` parameter for its sub-queries. In this case, in a response, each hit together with a `_percolator_document_slot` field contains `_percolator_document_slot_<slotNumber>_matched_queries` fields that show which sub-queries matched each percolated document.

For example:

```console
PUT /my-index-000001/_doc/5?refresh
{
  "query": {
    "bool": {
      "should": [
        {
          "match": {
            "message": {
              "query": "Japanese art",
              "_name": "query1"
            }
          }
        },
        {
          "match": {
            "message": {
              "query": "Holand culture",
              "_name": "query2"
            }
          }
        }
      ]
    }
  }
}
```
% TEST[continued]

```console
GET /my-index-000001/_search
{
  "query": {
    "percolate": {
      "field": "query",
      "documents": [
        {
          "message": "Japanse art"
        },
        {
          "message": "Holand culture"
        },
        {
          "message": "Japanese art and Holand culture"
        },
        {
          "message": "no-match"
        }
      ]
    }
  }
}
```
% TEST[continued]

```console-result
{
  "took": 55,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped" : 0,
    "failed": 0
  },
  "hits": {
    "total" : {
        "value": 1,
        "relation": "eq"
    },
    "max_score": 1.1181908,
    "hits": [
      {
        "_index": "my-index-000001",
        "_id": "5",
        "_score": 1.1181908,
        "_source": {
          "query": {
            "bool": {
              "should": [
                {
                  "match": {
                    "message": {
                      "query": "Japanese art",
                      "_name": "query1"
                    }
                  }
                },
                {
                  "match": {
                    "message": {
                      "query": "Holand culture",
                      "_name": "query2"
                    }
                  }
                }
              ]
            }
          }
        },
        "fields" : {
          "_percolator_document_slot" : [0, 1, 2],
          "_percolator_document_slot_0_matched_queries" : ["query1"], <1>
          "_percolator_document_slot_1_matched_queries" : ["query2"], <2>
          "_percolator_document_slot_2_matched_queries" : ["query1", "query2"] <3>
        }
      }
    ]
  }
}
```
% TESTRESPONSE[s/"took": 55,/"took": "$body.took",/]

1. The first document matched only the first sub-query.
2. The second document matched only the second sub-query.
3. The third document matched both sub-queries.



## Specifying multiple percolate queries [_specifying_multiple_percolate_queries]

It is possible to specify multiple `percolate` queries in a single search request:

```console
GET /my-index-000001/_search
{
  "query": {
    "bool": {
      "should": [
        {
          "percolate": {
            "field": "query",
            "document": {
              "message": "bonsai tree"
            },
            "name": "query1" <1>
          }
        },
        {
          "percolate": {
            "field": "query",
            "document": {
              "message": "tulip flower"
            },
            "name": "query2" <1>
          }
        }
      ]
    }
  }
}
```
% TEST[continued]

1. The `name` parameter will be used to identify which percolator document slots belong to what `percolate` query.


The `_percolator_document_slot` field name will be suffixed with what is specified in the `_name` parameter. If that isn’t specified then the `field` parameter will be used, which in this case will result in ambiguity.

The above search request returns a response similar to this:

```console-result
{
  "took": 13,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped" : 0,
    "failed": 0
  },
  "hits": {
    "total" : {
        "value": 1,
        "relation": "eq"
    },
    "max_score": 0.26152915,
    "hits": [
      {
        "_index": "my-index-000001",
        "_id": "1",
        "_score": 0.26152915,
        "_source": {
          "query": {
            "match": {
              "message": "bonsai tree"
            }
          }
        },
        "fields" : {
          "_percolator_document_slot_query1" : [0] <1>
        }
      }
    ]
  }
}
```
% TESTRESPONSE[s/"took": 13,/"took": "$body.took",/]

1. The `_percolator_document_slot_query1` percolator slot field indicates that these matched slots are from the `percolate` query with `_name` parameter set to `query1`.



## How it works under the hood [how-it-works]

When indexing a document into an index that has the [percolator field type](/reference/elasticsearch/mapping-reference/percolator.md) mapping configured, the query part of the document gets parsed into a Lucene query and is stored into the Lucene index. A binary representation of the query gets stored, but also the query’s terms are analyzed and stored into an indexed field.

At search time, the document specified in the request gets parsed into a Lucene document and is stored in a in-memory temporary Lucene index. This in-memory index can just hold this one document and it is optimized for that. After this a special query is built based on the terms in the in-memory index that select candidate percolator queries based on their indexed query terms. These queries are then evaluated by the in-memory index if they actually match.

The selecting of candidate percolator queries matches is an important performance optimization during the execution of the `percolate` query as it can significantly reduce the number of candidate matches the in-memory index needs to evaluate. The reason the `percolate` query can do this is because during indexing of the percolator queries the query terms are being extracted and indexed with the percolator query. Unfortunately the percolator cannot extract terms from all queries (for example the `wildcard` or `geo_shape` query) and as a result of that in certain cases the percolator can’t do the selecting optimization (for example if an unsupported query is defined in a required clause of a boolean query or the unsupported query is the only query in the percolator document). These queries are marked by the percolator and can be found by running the following search:

```console
GET /_search
{
  "query": {
    "term" : {
      "query.extraction_result" : "failed"
    }
  }
}
```

::::{note}
The above example assumes that there is a `query` field of type `percolator` in the mappings.
::::


Given the design of percolation, it often makes sense to use separate indices for the percolate queries and documents being percolated, as opposed to a single index as we do in examples. There are a few benefits to this approach:

* Because percolate queries contain a different set of fields from the percolated documents, using two separate indices allows for fields to be stored in a denser, more efficient way.
* Percolate queries do not scale in the same way as other queries, so percolation performance may benefit from using a different index configuration, like the number of primary shards.


## Notes [percolate-query-notes]

### Allow expensive queries [_allow_expensive_queries_3]

Percolate queries will not be executed if [`search.allow_expensive_queries`](/reference/query-languages/querydsl.md#query-dsl-allow-expensive-queries) is set to false.


### Using custom similarities [_using_custom_similarities]

Percolate queries will not respect any configured [custom similarity](/reference/elasticsearch/index-settings/similarity.md). They always use the default Lucene similarity.



