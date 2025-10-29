---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/highlighting.html
applies_to:
  stack: all
  serverless: all
---

This page provides practical examples of how to configure highlighting in {{es}}.

# Highlighting examples

* [Override global settings](#override-global-settings)
* [Specify a highlight query](#specify-highlight-query)
* [Set highlighter type](#set-highlighter-type)
* [Configure highlighting tags](#configure-tags)
* [Highlight all fields](#highlight-all)
* [Combine matches on multiple fields](#matched-fields)
* [Explicitly order highlighted fields](#explicit-field-order)
* [Control highlighted fragments](#control-highlighted-frags)
* [Highlight using the postings list](#highlight-postings-list)
* [Specify a fragmenter for the plain highlighter](#specify-fragmenter)

## Override global settings [override-global-settings]

You can specify highlighter settings globally and selectively override them for individual fields.

```console
GET /_search
{
  "query" : {
    "match": { "user.id": "kimchy" }
  },
  "highlight" : {
    "number_of_fragments" : 3,
    "fragment_size" : 150,
    "fields" : {
      "body" : { "pre_tags" : ["<em>"], "post_tags" : ["</em>"] },
      "blog.title" : { "number_of_fragments" : 0 },
      "blog.author" : { "number_of_fragments" : 0 },
      "blog.comment" : { "number_of_fragments" : 5, "order" : "score" }
    }
  }
}
```
% TEST[setup:my_index]

## Specify a highlight query [specify-highlight-query]

You can specify a `highlight_query` to take additional information into account when highlighting. For example, the following query includes both the search query and rescore query in the `highlight_query`. Without the `highlight_query`, highlighting would only take the search query into account.

```console
GET /_search
{
  "query": {
    "match": {
      "comment": {
        "query": "foo bar"
      }
    }
  },
  "rescore": {
    "window_size": 50,
    "query": {
      "rescore_query": {
        "match_phrase": {
          "comment": {
            "query": "foo bar",
            "slop": 1
          }
        }
      },
      "rescore_query_weight": 10
    }
  },
  "_source": false,
  "highlight": {
    "order": "score",
    "fields": {
      "comment": {
        "fragment_size": 150,
        "number_of_fragments": 3,
        "highlight_query": {
          "bool": {
            "must": {
              "match": {
                "comment": {
                  "query": "foo bar"
                }
              }
            },
            "should": {
              "match_phrase": {
                "comment": {
                  "query": "foo bar",
                  "slop": 1,
                  "boost": 10.0
                }
              }
            },
            "minimum_should_match": 0
          }
        }
      }
    }
  }
}
```
% TEST[setup:my_index]

## Set highlighter type [set-highlighter-type]

The `type` field allows to force a specific highlighter type. The allowed values are: `unified`, `plain` and `fvh`. The following is an example that forces the use of the plain highlighter:

```console
GET /_search
{
  "query": {
    "match": { "user.id": "kimchy" }
  },
  "highlight": {
    "fields": {
      "comment": { "type": "plain" }
    }
  }
}
```
% TEST[setup:my_index]

## Configure highlighting tags [configure-tags]

By default, the highlighting will wrap highlighted text in `<em>` and `</em>`. This can be controlled by setting `pre_tags` and `post_tags`, for example:

```console
GET /_search
{
  "query" : {
    "match": { "user.id": "kimchy" }
  },
  "highlight" : {
    "pre_tags" : ["<tag1>"],
    "post_tags" : ["</tag1>"],
    "fields" : {
      "body" : {}
    }
  }
}
```
% TEST[setup:my_index]

When using the fast vector highlighter, you can specify additional tags and the "importance" is ordered.

```console
GET /_search
{
  "query" : {
    "match": { "user.id": "kimchy" }
  },
  "highlight" : {
    "pre_tags" : ["<tag1>", "<tag2>"],
    "post_tags" : ["</tag1>", "</tag2>"],
    "fields" : {
      "body" : {}
    }
  }
}
```
% TEST[setup:my_index]

You can also use the built-in `styled` tag schema:

```console
GET /_search
{
  "query" : {
    "match": { "user.id": "kimchy" }
  },
  "highlight" : {
    "tags_schema" : "styled",
    "fields" : {
      "comment" : {}
    }
  }
}
```
% TEST[setup:my_index]

## Highlight in all fields [highlight-all]

By default, only fields that contains a query match are highlighted. Set `require_field_match` to `false` to highlight all fields.

```console
GET /_search
{
  "query" : {
    "match": { "user.id": "kimchy" }
  },
  "highlight" : {
    "require_field_match": false,
    "fields": {
      "body" : { "pre_tags" : ["<em>"], "post_tags" : ["</em>"] }
    }
  }
}
```
% TEST[setup:my_index]

## Combine matches on multiple fields [matched-fields]

::::{note}
Supported by the `unified` and `fvh` highlighters.
::::


The Unified and Fast Vector Highlighter can combine matches on multiple fields to highlight a single field. This is most intuitive for multifields that analyze the same string in different ways.

:::::::{tab-set}

::::::{tab-item} Unified
In the following examples, `comment` is analyzed by the `standard` analyzer and `comment.english` is analyzed by the `english` analyzer.

```console
PUT index1
{
  "mappings": {
    "properties": {
      "comment": {
        "type": "text",
        "analyzer": "standard",
        "fields": {
          "english": {
            "type": "text",
            "analyzer": "english"
          }
        }
      }
    }
  }
}
```
% TEST[continued]

```console
PUT index1/_bulk?refresh=true
{"index": {"_id": "doc1" }}
{"comment": "run with scissors"}
{ "index" : {"_id": "doc2"} }
{"comment": "running with scissors"}
```
% TEST[continued]

```console
GET index1/_search
{
  "query": {
    "query_string": {
      "query": "running with scissors",
      "fields": ["comment", "comment.english"]
    }
  },
  "highlight": {
    "order": "score",
    "fields": {
      "comment": {}
    }
  }
}
```
% TEST[continued]

The above request matches both "run with scissors" and "running with scissors" and would highlight "running" and "scissors" but not "run". If both phrases appear in a large document then "running with scissors" is sorted above "run with scissors" in the fragments list because there are more matches in that fragment.

```console-result
{
  ...
  "hits" : {
    "total" : {
      "value" : 2,
      "relation" : "eq"
    },
    "max_score": 1.0577903,
    "hits" : [
      {
        "_index" : "index1",
        "_id" : "doc2",
        "_score" : 1.0577903,
        "_source" : {
          "comment" : "running with scissors"
        },
        "highlight" : {
          "comment" : [
            "<em>running</em> <em>with</em> <em>scissors</em>"
          ]
        }
      },
      {
        "_index" : "index1",
        "_id" : "doc1",
        "_score" : 0.36464313,
        "_source" : {
          "comment" : "run with scissors"
        },
        "highlight" : {
          "comment" : [
            "run <em>with</em> <em>scissors</em>"
          ]
        }
      }
    ]
  }
}
```
% TESTRESPONSE[s/\.\.\./"took" : $body.took,"timed_out" : $body.timed_out,"_shards" : $body._shards,/]

The below request highlights "run" as well as "running" and "scissors", because the `matched_fields` parameter instructs that for highlighting we need to combine matches from the `comment.english` field with the matches from the original `comment` field.

```console
GET index1/_search
{
  "query": {
    "query_string": {
      "query": "running with scissors",
      "fields": ["comment", "comment.english"]
    }
  },
  "highlight": {
    "order": "score",
    "fields": {
      "comment": {
        "matched_fields": ["comment.english"]
      }
    }
  }
}
```
% TEST[continued]

```console-result
{
  ...
  "hits" : {
    "total" : {
      "value" : 2,
      "relation" : "eq"
    },
    "max_score": 1.0577903,
    "hits" : [
      {
        "_index" : "index1",
        "_id" : "doc2",
        "_score" : 1.0577903,
        "_source" : {
          "comment" : "running with scissors"
        },
        "highlight" : {
          "comment" : [
            "<em>running</em> <em>with</em> <em>scissors</em>"
          ]
        }
      },
      {
        "_index" : "index1",
        "_id" : "doc1",
        "_score" : 0.36464313,
        "_source" : {
          "comment" : "run with scissors"
        },
        "highlight" : {
          "comment" : [
            "<em>run</em> <em>with</em> <em>scissors</em>"
          ]
        }
      }
    ]
  }
}
```
% TESTRESPONSE[s/\.\.\./"took" : $body.took,"timed_out" : $body.timed_out,"_shards" : $body._shards,/]
::::::

::::::{tab-item} FVH
In the following examples, `comment` is analyzed by the `standard` analyzer and `comment.english` is analyzed by the `english` analyzer.

```console
PUT index2
{
  "mappings": {
    "properties": {
      "comment": {
        "type": "text",
        "analyzer": "standard",
        "term_vector": "with_positions_offsets",
        "fields": {
          "english": {
            "type": "text",
            "analyzer": "english",
            "term_vector": "with_positions_offsets"
          }
        }
      }
    }
  }
}
```
% TEST[continued]

```console
PUT index2/_bulk?refresh=true
{"index": {"_id": "doc1" }}
{"comment": "run with scissors"}
{ "index" : {"_id": "doc2"} }
{"comment": "running with scissors"}
```
% TEST[continued]

```console
GET index2/_search
{
  "query": {
    "query_string": {
      "query": "running with scissors",
      "fields": ["comment", "comment.english"]
    }
  },
  "highlight": {
    "order": "score",
    "fields": {
      "comment": {
        "type" : "fvh"
      }
    }
  }
}
```
% TEST[continued]

The above request matches both "run with scissors" and "running with scissors" and would highlight "running" and "scissors" but not "run". If both phrases appear in a large document then "running with scissors" is sorted above "run with scissors" in the fragments list because there are more matches in that fragment.

```console-result
{
  ...
  "hits" : {
    "total" : {
      "value" : 2,
      "relation" : "eq"
    },
    "max_score": 1.0577903,
    "hits" : [
      {
        "_index" : "index2",
        "_id" : "doc2",
        "_score" : 1.0577903,
        "_source" : {
          "comment" : "running with scissors"
        },
        "highlight" : {
          "comment" : [
            "<em>running</em> <em>with</em> <em>scissors</em>"
          ]
        }
      },
      {
        "_index" : "index2",
        "_id" : "doc1",
        "_score" : 0.36464313,
        "_source" : {
          "comment" : "run with scissors"
        },
        "highlight" : {
          "comment" : [
            "run <em>with</em> <em>scissors</em>"
          ]
        }
      }
    ]
  }
}
```
% TESTRESPONSE[s/\.\.\./"took" : $body.took,"timed_out" : $body.timed_out,"_shards" : $body._shards,/]

The below request highlights "run" as well as "running" and "scissors", because the `matched_fields` parameter instructs that for highlighting we need to combine matches from the `comment` and `comment.english` fields.

```console
GET index2/_search
{
  "query": {
    "query_string": {
      "query": "running with scissors",
      "fields": ["comment", "comment.english"]
    }
  },
  "highlight": {
    "order": "score",
    "fields": {
      "comment": {
        "type" : "fvh",
        "matched_fields": ["comment", "comment.english"]
      }
    }
  }
}
```
% TEST[continued]

```console-result
{
  ...
  "hits" : {
    "total" : {
      "value" : 2,
      "relation" : "eq"
    },
    "max_score": 1.0577903,
    "hits" : [
      {
        "_index" : "index2",
        "_id" : "doc2",
        "_score" : 1.0577903,
        "_source" : {
          "comment" : "running with scissors"
        },
        "highlight" : {
          "comment" : [
            "<em>running</em> <em>with</em> <em>scissors</em>"
          ]
        }
      },
      {
        "_index" : "index2",
        "_id" : "doc1",
        "_score" : 0.36464313,
        "_source" : {
          "comment" : "run with scissors"
        },
        "highlight" : {
          "comment" : [
            "<em>run</em> <em>with</em> <em>scissors</em>"
          ]
        }
      }
    ]
  }
}
```
% TESTRESPONSE[s/\.\.\./"took" : $body.took,"timed_out" : $body.timed_out,"_shards" : $body._shards,/]

The below request wouldn’t highlight "run" or "scissor" but shows that it is just fine not to list the field to which the matches are combined (`comment.english`) in the matched fields.

```console
GET index2/_search
{
  "query": {
    "query_string": {
      "query": "running with scissors",
      "fields": ["comment", "comment.english"]
    }
  },
  "highlight": {
    "order": "score",
    "fields": {
      "comment.english": {
        "type" : "fvh",
        "matched_fields": ["comment"]
      }
    }
  }
}
```
% TEST[continued]

```console-result
{
  ...
  "hits" : {
    "total" : {
      "value" : 2,
      "relation" : "eq"
    },
    "max_score": 1.0577903,
    "hits" : [
      {
        "_index" : "index2",
        "_id" : "doc2",
        "_score" : 1.0577903,
        "_source" : {
          "comment" : "running with scissors"
        },
        "highlight" : {
          "comment.english" : [
            "<em>running</em> <em>with</em> <em>scissors</em>"
          ]
        }
      },
      {
        "_index" : "index2",
        "_id" : "doc1",
        "_score" : 0.36464313,
        "_source" : {
          "comment" : "run with scissors"
        },
        "highlight" : {
          "comment.english" : [
            "run <em>with</em> <em>scissors</em>"
          ]
        }
      }
    ]
  }
}
```
% TESTRESPONSE[s/\.\.\./"took" : $body.took,"timed_out" : $body.timed_out,"_shards" : $body._shards,/]

:::::{note}
There is a small amount of overhead involved with setting `matched_fields` to a non-empty array so always prefer

```js
    "highlight": {
        "fields": {
            "comment": {}
        }
    }
```
% NOTCONSOLE

to

```js
    "highlight": {
        "fields": {
            "comment": {
                "matched_fields": ["comment"],
                "type" : "fvh"
            }
        }
    }
```
% NOTCONSOLE
::::::

:::::::
::::{note}
Technically it is also fine to add fields to `matched_fields` that don’t share the same underlying string as the field to which the matches are combined. The results might not make much sense and if one of the matches is off the end of the text then the whole query will fail.
::::


:::::



## Explicitly order highlighted fields [explicit-field-order]

Elasticsearch highlights the fields in the order that they are sent, but per the JSON spec, objects are unordered. If you need to be explicit about the order in which fields are highlighted specify the `fields` as an array:

```console
GET /_search
{
  "highlight": {
    "fields": [
      { "title": {} },
      { "text": {} }
    ]
  }
}
```
% TEST[setup:my_index]

None of the highlighters built into Elasticsearch care about the order that the fields are highlighted but a plugin might.


## Control highlighted fragments [control-highlighted-frags]

Each field highlighted can control the size of the highlighted fragment in characters (defaults to `100`), and the maximum number of fragments to return (defaults to `5`). For example:

```console
GET /_search
{
  "query" : {
    "match": { "user.id": "kimchy" }
  },
  "highlight" : {
    "fields" : {
      "comment" : {"fragment_size" : 150, "number_of_fragments" : 3}
    }
  }
}
```
% TEST[setup:my_index]

On top of this it is possible to specify that highlighted fragments need to be sorted by score:

```console
GET /_search
{
  "query" : {
    "match": { "user.id": "kimchy" }
  },
  "highlight" : {
    "order" : "score",
    "fields" : {
      "comment" : {"fragment_size" : 150, "number_of_fragments" : 3}
    }
  }
}
```
% TEST[setup:my_index]

If the `number_of_fragments` value is set to `0` then no fragments are produced, instead the whole content of the field is returned, and of course it is highlighted. This can be very handy if short texts (like document title or address) need to be highlighted but no fragmentation is required. Note that `fragment_size` is ignored in this case.

```console
GET /_search
{
  "query" : {
    "match": { "user.id": "kimchy" }
  },
  "highlight" : {
    "fields" : {
      "body" : {},
      "blog.title" : {"number_of_fragments" : 0}
    }
  }
}
```
% TEST[setup:my_index]

When using `fvh` one can use `fragment_offset` parameter to control the margin to start highlighting from.

In the case where there is no matching fragment to highlight, the default is to not return anything. Instead, we can return a snippet of text from the beginning of the field by setting `no_match_size` (default `0`) to the length of the text that you want returned. The actual length may be shorter or longer than specified as it tries to break on a word boundary.

```console
GET /_search
{
  "query": {
    "match": { "user.id": "kimchy" }
  },
  "highlight": {
    "fields": {
      "comment": {
        "fragment_size": 150,
        "number_of_fragments": 3,
        "no_match_size": 150
      }
    }
  }
}
```
% TEST[setup:my_index]

## Highlight using the postings list [highlight-postings-list]

Here is an example of setting the `comment` field in the index mapping to allow for highlighting using the postings:

```console
PUT /example
{
  "mappings": {
    "properties": {
      "comment" : {
        "type": "text",
        "index_options" : "offsets"
      }
    }
  }
}
```

Here is an example of setting the `comment` field to allow for highlighting using the `term_vectors` (this will cause the index to be bigger):

```console
PUT /example
{
  "mappings": {
    "properties": {
      "comment" : {
        "type": "text",
        "term_vector" : "with_positions_offsets"
      }
    }
  }
}
```


## Specify a fragmenter for the plain highlighter [specify-fragmenter]

When using the `plain` highlighter, you can choose between the `simple` and `span` fragmenters:

```console
GET my-index-000001/_search
{
  "query": {
    "match_phrase": { "message": "number 1" }
  },
  "highlight": {
    "fields": {
      "message": {
        "type": "plain",
        "fragment_size": 15,
        "number_of_fragments": 3,
        "fragmenter": "simple"
      }
    }
  }
}
```
% TEST[setup:messages]

Response:

```console-result
{
  ...
  "hits": {
    "total": {
      "value": 1,
      "relation": "eq"
    },
    "max_score": 1.6011951,
    "hits": [
      {
        "_index": "my-index-000001",
        "_id": "1",
        "_score": 1.6011951,
        "_source": {
          "message": "some message with the number 1",
          "context": "bar"
        },
        "highlight": {
          "message": [
            " with the <em>number</em>",
            " <em>1</em>"
          ]
        }
      }
    ]
  }
}
```
%  TESTRESPONSE[s/\.\.\./"took": $body.took,"timed_out": false,"_shards": $body._shards,/]

```console
GET my-index-000001/_search
{
  "query": {
    "match_phrase": { "message": "number 1" }
  },
  "highlight": {
    "fields": {
      "message": {
        "type": "plain",
        "fragment_size": 15,
        "number_of_fragments": 3,
        "fragmenter": "span"
      }
    }
  }
}
```
% TEST[setup:messages]

Response:

```console-result
{
  ...
  "hits": {
    "total": {
      "value": 1,
      "relation": "eq"
    },
    "max_score": 1.6011951,
    "hits": [
      {
        "_index": "my-index-000001",
        "_id": "1",
        "_score": 1.6011951,
        "_source": {
          "message": "some message with the number 1",
          "context": "bar"
        },
        "highlight": {
          "message": [
            " with the <em>number</em> <em>1</em>"
          ]
        }
      }
    ]
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"timed_out": false,"_shards": $body._shards,/]

If the `number_of_fragments` option is set to `0`, `NullFragmenter` is used which does not fragment the text at all. This is useful for highlighting the entire contents of a document or field.