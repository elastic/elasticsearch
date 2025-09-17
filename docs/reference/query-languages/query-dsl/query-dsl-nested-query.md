---
navigation_title: "Nested"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-nested-query.html
---

# Nested query [query-dsl-nested-query]


Wraps another query to search [nested](/reference/elasticsearch/mapping-reference/nested.md) fields.

The `nested` query searches nested field objects as if they were indexed as separate documents. If an object matches the search, the `nested` query returns the root parent document.

## Example request [nested-query-ex-request]

### Index setup [nested-query-index-setup]

To use the `nested` query, your index must include a [nested](/reference/elasticsearch/mapping-reference/nested.md) field mapping. For example:

```console
PUT /my-index-000001
{
  "mappings": {
    "properties": {
      "obj1": {
        "type": "nested"
      }
    }
  }
}
```


### Example query [nested-query-ex-query]

```console
GET /my-index-000001/_search
{
  "query": {
    "nested": {
      "path": "obj1",
      "query": {
        "bool": {
          "must": [
            { "match": { "obj1.name": "blue" } },
            { "range": { "obj1.count": { "gt": 5 } } }
          ]
        }
      },
      "score_mode": "avg"
    }
  }
}
```



## Top-level parameters for `nested` [nested-top-level-params]

`path`
:   (Required, string) Path to the nested object you wish to search.

`query`
:   (Required, query object) Query you wish to run on nested objects in the `path`. If an object matches the search, the `nested` query returns the root parent document.

You can search nested fields using dot notation that includes the complete path, such as `obj1.name`.

Multi-level nesting is automatically supported, and detected, resulting in an inner nested query to automatically match the relevant nesting level, rather than root, if it exists within another nested query.

See [Multi-level nested queries](#multi-level-nested-query-ex) for an example.


`score_mode`
:   (Optional, string) Indicates how scores for matching child objects affect the root parent document’s [relevance score](/reference/query-languages/query-dsl/query-filter-context.md#relevance-scores). Default is `avg`, but nested `knn` queries only support `score_mode=max`. Valid values are:

`avg` (Default)
:   Use the mean relevance score of all matching child objects.

`max`
:   Uses the highest relevance score of all matching child objects.

`min`
:   Uses the lowest relevance score of all matching child objects.

`none`
:   Do not use the relevance scores of matching child objects. The query assigns parent documents a score of `0`.

`sum`
:   Add together the relevance scores of all matching child objects.


`ignore_unmapped`
:   (Optional, Boolean) Indicates whether to ignore an unmapped `path` and not return any documents instead of an error. Defaults to `false`.

If `false`, {{es}} returns an error if the `path` is an unmapped field.

You can use this parameter to query multiple indices that may not contain the field `path`.



## Notes [nested-query-notes]

### Context of script queries [nested-query-script-notes]

If you run a [`script` query](/reference/query-languages/query-dsl/query-dsl-script-query.md) within a nested query, you can only access doc values from the nested document, not the parent or root document.


### Multi-level nested queries [multi-level-nested-query-ex]

To see how multi-level nested queries work, first you need an index that has nested fields. The following request defines mappings for the `drivers` index with nested `make` and `model` fields.

```console
PUT /drivers
{
  "mappings": {
    "properties": {
      "driver": {
        "type": "nested",
        "properties": {
          "last_name": {
            "type": "text"
          },
          "vehicle": {
            "type": "nested",
            "properties": {
              "make": {
                "type": "text"
              },
              "model": {
                "type": "text"
              }
            }
          }
        }
      }
    }
  }
}
```

Next, index some documents to the `drivers` index.

```console
PUT /drivers/_doc/1
{
  "driver" : {
        "last_name" : "McQueen",
        "vehicle" : [
            {
                "make" : "Powell Motors",
                "model" : "Canyonero"
            },
            {
                "make" : "Miller-Meteor",
                "model" : "Ecto-1"
            }
        ]
    }
}

PUT /drivers/_doc/2?refresh
{
  "driver" : {
        "last_name" : "Hudson",
        "vehicle" : [
            {
                "make" : "Mifune",
                "model" : "Mach Five"
            },
            {
                "make" : "Miller-Meteor",
                "model" : "Ecto-1"
            }
        ]
    }
}
```

You can now use a multi-level nested query to match documents based on the `make` and `model` fields.

```console
GET /drivers/_search
{
  "query": {
    "nested": {
      "path": "driver",
      "query": {
        "nested": {
          "path": "driver.vehicle",
          "query": {
            "bool": {
              "must": [
                { "match": { "driver.vehicle.make": "Powell Motors" } },
                { "match": { "driver.vehicle.model": "Canyonero" } }
              ]
            }
          }
        }
      }
    }
  }
}
```

The search request returns the following response:

```console-result
{
  "took" : 5,
  "timed_out" : false,
  "_shards" : {
    "total" : 1,
    "successful" : 1,
    "skipped" : 0,
    "failed" : 0
  },
  "hits" : {
    "total" : {
      "value" : 1,
      "relation" : "eq"
    },
    "max_score" : 3.7349272,
    "hits" : [
      {
        "_index" : "drivers",
        "_id" : "1",
        "_score" : 3.7349272,
        "_source" : {
          "driver" : {
            "last_name" : "McQueen",
            "vehicle" : [
              {
                "make" : "Powell Motors",
                "model" : "Canyonero"
              },
              {
                "make" : "Miller-Meteor",
                "model" : "Ecto-1"
              }
            ]
          }
        }
      }
    ]
  }
}
```


### `must_not` clauses and `nested` queries [must-not-clauses-and-nested-queries]

If a `nested` query matches one or more nested objects in a document, it returns the document as a hit. This applies even if other nested objects in the document don’t match the query. Keep this in mind when using a `nested` query that contains an inner [`must_not` clause](/reference/query-languages/query-dsl/query-dsl-bool-query.md).

::::{tip}
Use the [`inner_hits`](/reference/elasticsearch/rest-apis/retrieve-inner-hits.md) parameter to see which nested objects matched a `nested` query.
::::


For example, the following search uses an outer `nested` query with an inner `must_not` clause.

```console
PUT my-index
{
  "mappings": {
    "properties": {
      "comments": {
        "type": "nested"
      }
    }
  }
}

PUT my-index/_doc/1?refresh
{
  "comments": [
    {
      "author": "kimchy"
    }
  ]
}

PUT my-index/_doc/2?refresh
{
  "comments": [
    {
      "author": "kimchy"
    },
    {
      "author": "nik9000"
    }
  ]
}

PUT my-index/_doc/3?refresh
{
  "comments": [
    {
      "author": "nik9000"
    }
  ]
}

POST my-index/_search
{
  "query": {
    "nested": {
      "path": "comments",
      "query": {
        "bool": {
          "must_not": [
            {
              "term": {
                "comments.author": "nik9000"
              }
            }
          ]
        }
      }
    }
  }
}
```

The search returns:

```console
{
  ...
  "hits" : {
    ...
    "hits" : [
      {
        "_index" : "my-index",
        "_id" : "1",
        "_score" : 0.0,
        "_source" : {
          "comments" : [
            {
              "author" : "kimchy"
            }
          ]
        }
      },
      {
        "_index" : "my-index",
        "_id" : "2",
        "_score" : 0.0,
        "_source" : {
          "comments" : [
            {
              "author" : "kimchy"              <1>
            },
            {
              "author" : "nik9000"             <2>
            }
          ]
        }
      }
    ]
  }
}
```

1. This nested object matches the query. As a result, the search returns the object’s parent document as a hit.
2. This nested object doesn’t match the query. Since another nested object in the same document does match the query, the search still returns the parent document as a hit.


To exclude documents with any nested objects that match the `nested` query, use an outer `must_not` clause.

```console
POST my-index/_search
{
  "query": {
    "bool": {
      "must_not": [
        {
          "nested": {
            "path": "comments",
            "query": {
              "term": {
                "comments.author": "nik9000"
              }
            }
          }
        }
      ]
    }
  }
}
```

The search returns:

```console
{
  ...
  "hits" : {
    ...
    "hits" : [
      {
        "_index" : "my-index",
        "_id" : "1",
        "_score" : 0.0,
        "_source" : {
          "comments" : [
            {
              "author" : "kimchy"
            }
          ]
        }
      }
    ]
  }
}
```



