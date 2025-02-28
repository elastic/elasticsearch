---
navigation_title: "Percolator"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/percolator.html
---

# Percolator field type [percolator]


The `percolator` field type parses a json structure into a native query and stores that query, so that the [percolate query](/reference/query-languages/query-dsl-percolate-query.md) can use it to match provided documents.

Any field that contains a json object can be configured to be a percolator field. The percolator field type has no settings. Just configuring the `percolator` field type is sufficient to instruct Elasticsearch to treat a field as a query.

If the following mapping configures the `percolator` field type for the `query` field:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "query": {
        "type": "percolator"
      },
      "field": {
        "type": "text"
      }
    }
  }
}
```

Then you can index a query:

```console
PUT my-index-000001/_doc/match_value
{
  "query": {
    "match": {
      "field": "value"
    }
  }
}
```

::::{important}
Fields referred to in a percolator query must **already** exist in the mapping associated with the index used for percolation. In order to make sure these fields exist, add or update a mapping via the [create index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) or [update mapping](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping) APIs.

::::



## Reindexing your percolator queries [_reindexing_your_percolator_queries]

Reindexing percolator queries is sometimes required to benefit from improvements made to the `percolator` field type in new releases.

Reindexing percolator queries can be reindexed by using the [reindex api](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-reindex). Lets take a look at the following index with a percolator field type:

```console
PUT index
{
  "mappings": {
    "properties": {
      "query" : {
        "type" : "percolator"
      },
      "body" : {
        "type": "text"
      }
    }
  }
}

POST _aliases
{
  "actions": [
    {
      "add": {
        "index": "index",
        "alias": "queries" <1>
      }
    }
  ]
}

PUT queries/_doc/1?refresh
{
  "query" : {
    "match" : {
      "body" : "quick brown fox"
    }
  }
}
```

1. It is always recommended to define an alias for your index, so that in case of a reindex systems / applications don’t need to be changed to know that the percolator queries are now in a different index.


Lets say you’re going to upgrade to a new major version and in order for the new Elasticsearch version to still be able to read your queries you need to reindex your queries into a new index on the current Elasticsearch version:

```console
PUT new_index
{
  "mappings": {
    "properties": {
      "query" : {
        "type" : "percolator"
      },
      "body" : {
        "type": "text"
      }
    }
  }
}

POST /_reindex?refresh
{
  "source": {
    "index": "index"
  },
  "dest": {
    "index": "new_index"
  }
}

POST _aliases
{
  "actions": [ <1>
    {
      "remove": {
        "index" : "index",
        "alias": "queries"
      }
    },
    {
      "add": {
        "index": "new_index",
        "alias": "queries"
      }
    }
  ]
}
```

1. If you have an alias don’t forget to point it to the new index.


Executing the `percolate` query via the `queries` alias:

```console
GET /queries/_search
{
  "query": {
    "percolate" : {
      "field" : "query",
      "document" : {
        "body" : "fox jumps over the lazy dog"
      }
    }
  }
}
```

now returns matches from the new index:

```console-result
{
  "took": 3,
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
    "max_score": 0.13076457,
    "hits": [
      {
        "_index": "new_index", <1>
        "_id": "1",
        "_score": 0.13076457,
        "_source": {
          "query": {
            "match": {
              "body": "quick brown fox"
            }
          }
        },
        "fields" : {
          "_percolator_document_slot" : [0]
        }
      }
    ]
  }
}
```

1. Percolator query hit is now being presented from the new index.



## Optimizing query time text analysis [_optimizing_query_time_text_analysis]

When the percolator verifies a percolator candidate match it is going to parse, perform query time text analysis and actually run the percolator query on the document being percolated. This is done for each candidate match and every time the `percolate` query executes. If your query time text analysis is relatively expensive part of query parsing then text analysis can become the dominating factor time is being spent on when percolating. This query parsing overhead can become noticeable when the percolator ends up verifying many candidate percolator query matches.

To avoid the most expensive part of text analysis at percolate time. One can choose to do the expensive part of text analysis when indexing the percolator query. This requires using two different analyzers. The first analyzer actually performs text analysis that needs be performed (expensive part). The second analyzer (usually whitespace) just splits the generated tokens that the first analyzer has produced. Then before indexing a percolator query, the analyze api should be used to analyze the query text with the more expensive analyzer. The result of the analyze api, the tokens, should be used to substitute the original query text in the percolator query. It is important that the query should now be configured to override the analyzer from the mapping and just the second analyzer. Most text based queries support an `analyzer` option (`match`, `query_string`, `simple_query_string`). Using this approach the expensive text analysis is performed once instead of many times.

Lets demonstrate this workflow via a simplified example.

Lets say we want to index the following percolator query:

```js
{
  "query" : {
    "match" : {
      "body" : {
        "query" : "missing bicycles"
      }
    }
  }
}
```

with these settings and mapping:

```console
PUT /test_index
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_analyzer" : {
          "tokenizer": "standard",
          "filter" : ["lowercase", "porter_stem"]
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "query" : {
        "type": "percolator"
      },
      "body" : {
        "type": "text",
        "analyzer": "my_analyzer" <1>
      }
    }
  }
}
```

1. For the purpose of this example, this analyzer is considered expensive.


First we need to use the analyze api to perform the text analysis prior to indexing:

```console
POST /test_index/_analyze
{
  "analyzer" : "my_analyzer",
  "text" : "missing bicycles"
}
```

This results the following response:

```console-result
{
  "tokens": [
    {
      "token": "miss",
      "start_offset": 0,
      "end_offset": 7,
      "type": "<ALPHANUM>",
      "position": 0
    },
    {
      "token": "bicycl",
      "start_offset": 8,
      "end_offset": 16,
      "type": "<ALPHANUM>",
      "position": 1
    }
  ]
}
```

All the tokens in the returned order need to replace the query text in the percolator query:

```console
PUT /test_index/_doc/1?refresh
{
  "query" : {
    "match" : {
      "body" : {
        "query" : "miss bicycl",
        "analyzer" : "whitespace" <1>
      }
    }
  }
}
```

1. It is important to select a whitespace analyzer here, otherwise the analyzer defined in the mapping will be used, which defeats the point of using this workflow. Note that `whitespace` is a built-in analyzer, if a different analyzer needs to be used, it needs to be configured first in the index’s settings.


The analyze api prior to the indexing the percolator flow should be done for each percolator query.

At percolate time nothing changes and the `percolate` query can be defined normally:

```console
GET /test_index/_search
{
  "query": {
    "percolate" : {
      "field" : "query",
      "document" : {
        "body" : "Bycicles are missing"
      }
    }
  }
}
```

This results in a response like this:

```console-result
{
  "took": 6,
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
    "max_score": 0.13076457,
    "hits": [
      {
        "_index": "test_index",
        "_id": "1",
        "_score": 0.13076457,
        "_source": {
          "query": {
            "match": {
              "body": {
                "query": "miss bicycl",
                "analyzer": "whitespace"
              }
            }
          }
        },
        "fields" : {
          "_percolator_document_slot" : [0]
        }
      }
    ]
  }
}
```


## Optimizing wildcard queries. [_optimizing_wildcard_queries]

Wildcard queries are more expensive than other queries for the percolator, especially if the wildcard expressions are large.

In the case of `wildcard` queries with prefix wildcard expressions or just the `prefix` query, the `edge_ngram` token filter can be used to replace these queries with regular `term` query on a field where the `edge_ngram` token filter is configured.

Creating an index with custom analysis settings:

```console
PUT my_queries1
{
  "settings": {
    "analysis": {
      "analyzer": {
        "wildcard_prefix": { <1>
          "type": "custom",
          "tokenizer": "standard",
          "filter": [
            "lowercase",
            "wildcard_edge_ngram"
          ]
        }
      },
      "filter": {
        "wildcard_edge_ngram": { <2>
          "type": "edge_ngram",
          "min_gram": 1,
          "max_gram": 32
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "query": {
        "type": "percolator"
      },
      "my_field": {
        "type": "text",
        "fields": {
          "prefix": { <3>
            "type": "text",
            "analyzer": "wildcard_prefix",
            "search_analyzer": "standard"
          }
        }
      }
    }
  }
}
```

1. The analyzer that generates the prefix tokens to be used at index time only.
2. Increase the `min_gram` and decrease `max_gram` settings based on your prefix search needs.
3. This multifield should be used to do the prefix search with a `term` or `match` query instead of a `prefix` or `wildcard` query.


Then instead of indexing the following query:

```js
{
  "query": {
    "wildcard": {
      "my_field": "abc*"
    }
  }
}
```

this query below should be indexed:

```console
PUT /my_queries1/_doc/1?refresh
{
  "query": {
    "term": {
      "my_field.prefix": "abc"
    }
  }
}
```

This way can handle the second query more efficiently than the first query.

The following search request will match with the previously indexed percolator query:

```console
GET /my_queries1/_search
{
  "query": {
    "percolate": {
      "field": "query",
      "document": {
        "my_field": "abcd"
      }
    }
  }
}
```

```console-result
{
  "took": 6,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total" : {
        "value": 1,
        "relation": "eq"
    },
    "max_score": 0.18864399,
    "hits": [
      {
        "_index": "my_queries1",
        "_id": "1",
        "_score": 0.18864399,
        "_source": {
          "query": {
            "term": {
              "my_field.prefix": "abc"
            }
          }
        },
        "fields": {
          "_percolator_document_slot": [
            0
          ]
        }
      }
    ]
  }
}
```

The same technique can also be used to speed up suffix wildcard searches. By using the `reverse` token filter before the `edge_ngram` token filter.

```console
PUT my_queries2
{
  "settings": {
    "analysis": {
      "analyzer": {
        "wildcard_suffix": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": [
            "lowercase",
            "reverse",
            "wildcard_edge_ngram"
          ]
        },
        "wildcard_suffix_search_time": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": [
            "lowercase",
            "reverse"
          ]
        }
      },
      "filter": {
        "wildcard_edge_ngram": {
          "type": "edge_ngram",
          "min_gram": 1,
          "max_gram": 32
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "query": {
        "type": "percolator"
      },
      "my_field": {
        "type": "text",
        "fields": {
          "suffix": {
            "type": "text",
            "analyzer": "wildcard_suffix",
            "search_analyzer": "wildcard_suffix_search_time" <1>
          }
        }
      }
    }
  }
}
```

1. A custom analyzer is needed at search time too, because otherwise the query terms are not being reversed and would otherwise not match with the reserved suffix tokens.


Then instead of indexing the following query:

```js
{
  "query": {
    "wildcard": {
      "my_field": "*xyz"
    }
  }
}
```

the following query below should be indexed:

```console
PUT /my_queries2/_doc/2?refresh
{
  "query": {
    "match": { <1>
      "my_field.suffix": "xyz"
    }
  }
}
```

1. The `match` query should be used instead of the `term` query, because text analysis needs to reverse the query terms.


The following search request will match with the previously indexed percolator query:

```console
GET /my_queries2/_search
{
  "query": {
    "percolate": {
      "field": "query",
      "document": {
        "my_field": "wxyz"
      }
    }
  }
}
```


## Dedicated Percolator Index [_dedicated_percolator_index]

Percolate queries can be added to any index. Instead of adding percolate queries to the index the data resides in, these queries can also be added to a dedicated index. The advantage of this is that this dedicated percolator index can have its own index settings (For example the number of primary and replica shards). If you choose to have a dedicated percolate index, you need to make sure that the mappings from the normal index are also available on the percolate index. Otherwise percolate queries can be parsed incorrectly.


## Forcing Unmapped Fields to be Handled as Strings [_forcing_unmapped_fields_to_be_handled_as_strings]

In certain cases it is unknown what kind of percolator queries do get registered, and if no field mapping exists for fields that are referred by percolator queries then adding a percolator query fails. This means the mapping needs to be updated to have the field with the appropriate settings, and then the percolator query can be added. But sometimes it is sufficient if all unmapped fields are handled as if these were default text fields. In those cases one can configure the `index.percolator.map_unmapped_fields_as_text` setting to `true` (default to `false`) and then if a field referred in a percolator query does not exist, it will be handled as a default text field so that adding the percolator query doesn’t fail.


## Limitations [_limitations_2]


### Parent/child [parent-child]

Because the `percolate` query is processing one document at a time, it doesn’t support queries and filters that run against child documents such as `has_child` and `has_parent`.


### Fetching queries [_fetching_queries]

There are a number of queries that fetch data via a get call during query parsing. For example the `terms` query when using terms lookup, `template` query when using indexed scripts and `geo_shape` when using pre-indexed shapes. When these queries are indexed by the `percolator` field type then the get call is executed once. So each time the `percolator` query evaluates these queries, the fetches terms, shapes etc. as the were upon index time will be used. Important to note is that fetching of terms that these queries do, happens both each time the percolator query gets indexed on both primary and replica shards, so the terms that are actually indexed can be different between shard copies, if the source index changed while indexing.


### Script query [_script_query]

The script inside a `script` query can only access doc values fields. The `percolate` query indexes the provided document into an in-memory index. This in-memory index doesn’t support stored fields and because of that the `_source` field and other stored fields are not stored. This is the reason why in the `script` query the `_source` and other stored fields aren’t available.


### Field aliases [_field_aliases]

Percolator queries that contain [field aliases](/reference/elasticsearch/mapping-reference/field-alias.md) may not always behave as expected. In particular, if a percolator query is registered that contains a field alias, and then that alias is updated in the mappings to refer to a different field, the stored query will still refer to the original target field. To pick up the change to the field alias, the percolator query must be explicitly reindexed.

