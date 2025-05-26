---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/highlighting.html
applies_to:
  stack: all
---

# Highlighting [highlighting]

Highlighters enable you to get highlighted snippets from one or more fields in your search results so you can show users where the query matches are. When you request highlights, the response contains an additional `highlight` element for each search hit that includes the highlighted fields and the highlighted fragments.

::::{note}
Highlighters don’t reflect the boolean logic of a query when extracting terms to highlight. Thus, for some complex boolean queries (e.g nested boolean queries, queries using `minimum_should_match` etc.), parts of documents may be highlighted that don’t correspond to query matches.
::::


Highlighting requires the actual content of a field. If the field is not stored (the mapping does not set `store` to `true`), the actual `_source` is loaded and the relevant field is extracted from `_source`.

For example, to get highlights for the `content` field in each search hit using the default highlighter, include a `highlight` object in the request body that specifies the `content` field:

```console
GET /_search
{
  "query": {
    "match": { "content": "kimchy" }
  },
  "highlight": {
    "fields": {
      "content": {}
    }
  }
}
```

{{es}} supports three highlighters: `unified`, `plain`, and `fvh` (fast vector highlighter) for `text` and `keyword` fields and the `semantic` highlighter for `semantic_text` fields. You can specify the highlighter `type` you want to use for each field or rely on the field type’s default highlighter.


### Unified highlighter [unified-highlighter]

The `unified` highlighter uses the Lucene Unified Highlighter. This highlighter breaks the text into sentences and uses the BM25 algorithm to score individual sentences as if they were documents in the corpus. It also supports accurate phrase and multi-term (fuzzy, prefix, regex) highlighting. The `unified` highlighter can combine matches from multiple fields into one result (see `matched_fields`).

This is the default highlighter for all `text` and `keyword` fields.


### Semantic Highlighter [semantic-highlighter]

The `semantic` highlighter is specifically designed for use with the [`semantic_text`](/reference/elasticsearch/mapping-reference/semantic-text.md) field. It identifies and extracts the most relevant fragments from the field based on semantic similarity between the query and each fragment.

By default, [`semantic_text`](/reference/elasticsearch/mapping-reference/semantic-text.md) fields use the semantic highlighter.


### Plain highlighter [plain-highlighter]

The `plain` highlighter uses the standard Lucene highlighter. It attempts to reflect the query matching logic in terms of understanding word importance and any word positioning criteria in phrase queries.

::::{warning}
The `plain` highlighter works best for highlighting simple query matches in a single field. To accurately reflect query logic, it creates a tiny in-memory index and re-runs the original query criteria through Lucene’s query execution planner to get access to low-level match information for the current document. This is repeated for every field and every document that needs to be highlighted. If you want to highlight a lot of fields in a lot of documents with complex queries, we recommend using the `unified` highlighter on `postings` or `term_vector` fields.
::::



### Fast vector highlighter [fast-vector-highlighter]

The `fvh` highlighter uses the Lucene Fast Vector highlighter. This highlighter can be used on fields with `term_vector` set to `with_positions_offsets` in the mapping. The fast vector highlighter:

* Can be customized with a [`boundary_scanner`](#boundary-scanners).
* Requires setting `term_vector` to `with_positions_offsets` which increases the size of the index
* Can combine matches from multiple fields into one result. See `matched_fields`
* Can assign different weights to matches at different positions allowing for things like phrase matches being sorted above term matches when highlighting a Boosting Query that boosts phrase matches over term matches

::::{warning}
The `fvh` highlighter does not support span queries. If you need support for span queries, try an alternative highlighter, such as the `unified` highlighter.
::::



### Offsets strategy [offsets-strategy]

To create meaningful search snippets from the terms being queried, the highlighter needs to know the start and end character offsets of each word in the original text. These offsets can be obtained from:

* The postings list. If `index_options` is set to `offsets` in the mapping, the `unified` highlighter uses this information to highlight documents without re-analyzing the text. It re-runs the original query directly on the postings and extracts the matching offsets from the index, limiting the collection to the highlighted documents. This is important if you have large fields because it doesn’t require reanalyzing the text to be highlighted. It also requires less disk space than using `term_vectors`.
* Term vectors. If `term_vector` information is provided by setting `term_vector` to `with_positions_offsets` in the mapping, the `unified` highlighter automatically uses the `term_vector` to highlight the field. It’s fast especially for large fields (> `1MB`) and for highlighting multi-term queries like `prefix` or `wildcard` because it can access the dictionary of terms for each document. The `fvh` highlighter always uses term vectors.
* Plain highlighting. This mode is used by the `unified` when there is no other alternative. It creates a tiny in-memory index and re-runs the original query criteria through Lucene’s query execution planner to get access to low-level match information on the current document. This is repeated for every field and every document that needs highlighting. The `plain` highlighter always uses plain highlighting.

::::{warning}
Plain highlighting for large texts may require substantial amount of time and memory. To protect against this, the maximum number of text characters that will be analyzed has been limited to 1000000. This default limit can be changed for a particular index with the index setting [`index.highlight.max_analyzed_offset`](/reference/elasticsearch/index-settings/index-modules.md#index-max-analyzed-offset).
::::



### Highlighting settings [highlighting-settings]

Highlighting settings can be set on a global level and overridden at the field level.

boundary_chars
:   A string that contains each boundary character. Defaults to `.,!? \t\n`.

boundary_max_scan
:   How far to scan for boundary characters. Defaults to `20`.

$$$boundary-scanners$$$

boundary_scanner
:   Specifies how to break the highlighted fragments: `chars`, `sentence`, or `word`. Only valid for the `unified` and `fvh` highlighters. Defaults to `sentence` for the `unified` highlighter. Defaults to `chars` for the `fvh` highlighter.

    `chars`
    :   Use the characters specified by `boundary_chars` as highlighting boundaries. The `boundary_max_scan` setting controls how far to scan for boundary characters. Only valid for the `fvh` highlighter.

    `sentence`
    :   Break highlighted fragments at the next sentence boundary, as determined by Java’s [BreakIterator](https://docs.oracle.com/javase/8/docs/api/java/text/BreakIterator.md). You can specify the locale to use with `boundary_scanner_locale`.
        ::::{note}
        When used with the `unified` highlighter, the `sentence` scanner splits sentences bigger than `fragment_size` at the first word boundary next to `fragment_size`. You can set `fragment_size` to 0 to never split any sentence.
        ::::


    `word`
    :   Break highlighted fragments at the next word boundary, as determined by Java’s [BreakIterator](https://docs.oracle.com/javase/8/docs/api/java/text/BreakIterator.md). You can specify the locale to use with `boundary_scanner_locale`.


boundary_scanner_locale
:   Controls which locale is used to search for sentence and word boundaries. This parameter takes a form of a language tag, e.g. `"en-US"`,  `"fr-FR"`, `"ja-JP"`. More info can be found in the [Locale Language Tag](https://docs.oracle.com/javase/8/docs/api/java/util/Locale.md#forLanguageTag-java.lang.String-) documentation. The default value is [ Locale.ROOT](https://docs.oracle.com/javase/8/docs/api/java/util/Locale.md#ROOT).

encoder
:   Indicates if the snippet should be HTML encoded: `default` (no encoding) or `html` (HTML-escape the snippet text and then insert the highlighting tags)

fields
:   Specifies the fields to retrieve highlights for. You can use wildcards to specify fields. For example, you could specify `comment_*` to get highlights for all [text](/reference/elasticsearch/mapping-reference/text.md), [match_only_text](/reference/elasticsearch/mapping-reference/text.md#match-only-text-field-type), and [keyword](/reference/elasticsearch/mapping-reference/keyword.md) fields that start with `comment_`.

    ::::{note}
    Only text, match_only_text, and keyword fields are highlighted when you use wildcards. If you use a custom mapper and want to highlight on a field anyway, you must explicitly specify that field name.
    ::::


fragmenter
:   Specifies how text should be broken up in highlight snippets: `simple` or `span`. Only valid for the `plain` highlighter. Defaults to `span`.

    `simple`
    :   Breaks up text into same-sized fragments.

    `span`
    :   Breaks up text into same-sized fragments, but tries to avoid breaking up text between highlighted terms. This is helpful when you’re querying for phrases. Default.


fragment_offset
:   Controls the margin from which you want to start highlighting. Only valid when using the `fvh` highlighter.

fragment_size
:   The size of the highlighted fragment in characters. Defaults to 100.

highlight_query
:   Highlight matches for a query other than the search query. This is especially useful if you use a rescore query because those are not taken into account by highlighting by default.

    ::::{important}
    {{es}} does not validate that `highlight_query` contains the search query in any way so it is possible to define it so legitimate query results are not highlighted. Generally, you should include the search query as part of the `highlight_query`.
    ::::


matched_fields
:   Combine matches on multiple fields to highlight a single field. This is most intuitive for multifields that analyze the same string in different ways. Valid for the `unified` and fvh` highlighters, but the behavior of this option is different for each highlighter.

For the `unified` highlighter:

* `matched_fields` array should **not** contain the original field that you want to highlight. The original field will be automatically added to the `matched_fields`, and there is no way to exclude its matches when highlighting.
* `matched_fields` and the original field can be indexed with different strategies (with or without `offsets`, with or without `term_vectors`).
* only the original field to which the matches are combined is loaded so only that field benefits from having `store` set to `yes`

For the `fvh` highlighter:

* `matched_fields` array may or may not contain the original field depending on your needs. If you want to include the original field’s matches in highlighting, add it to the `matched_fields` array.
* all `matched_fields` must have `term_vector` set to `with_positions_offsets`
* only the original field to which the matches are combined is loaded so only that field benefits from having `store` set to `yes`.

    no_match_size
    :   The amount of text you want to return from the beginning of the field if there are no matching fragments to highlight. Defaults to 0 (nothing is returned).

    number_of_fragments
    :   The maximum number of fragments to return. If the number of fragments is set to 0, no fragments are returned. Instead, the entire field contents are highlighted and returned. This can be handy when you need to highlight short texts such as a title or address, but fragmentation is not required. If `number_of_fragments` is 0, `fragment_size` is ignored. Defaults to 5.

    order
    :   Sorts highlighted fragments by score when set to `score`. By default, fragments will be output in the order they appear in the field (order: `none`). Setting this option to `score` will output the most relevant fragments first. Each highlighter applies its own logic to compute relevancy scores. See the document [How highlighters work internally](#how-es-highlighters-work-internally) for more details how different highlighters find the best fragments.

    phrase_limit
    :   Controls the number of matching phrases in a document that are considered. Prevents the `fvh` highlighter from analyzing too many phrases and consuming too much memory. When using `matched_fields`, `phrase_limit` phrases per matched field are considered. Raising the limit increases query time and consumes more memory. Only supported by the `fvh` highlighter. Defaults to 256.

    pre_tags
    :   Use in conjunction with `post_tags` to define the HTML tags to use for the highlighted text. By default, highlighted text is wrapped in `<em>` and `</em>` tags. Specify as an array of strings.

    post_tags
    :   Use in conjunction with `pre_tags` to define the HTML tags to use for the highlighted text. By default, highlighted text is wrapped in `<em>` and `</em>` tags. Specify as an array of strings.

    require_field_match
    :   By default, only fields that contains a query match are highlighted. Set `require_field_match` to `false` to highlight all fields. Defaults to `true`.


$$$max-analyzed-offset$$$

max_analyzed_offset
:   By default, the maximum number of characters analyzed for a highlight request is bounded by the value defined in the [`index.highlight.max_analyzed_offset`](/reference/elasticsearch/index-settings/index-modules.md#index-max-analyzed-offset) setting, and when the number of characters exceeds this limit an error is returned. If this setting is set to a positive value, the highlighting stops at this defined maximum limit, and the rest of the text is not processed, thus not highlighted and no error is returned. If it is specifically set to -1 then the value of [`index.highlight.max_analyzed_offset`](/reference/elasticsearch/index-settings/index-modules.md#index-max-analyzed-offset) is used instead. For values < -1 or 0, an error is returned. The [`max_analyzed_offset`](#max-analyzed-offset) query setting does **not** override the [`index.highlight.max_analyzed_offset`](/reference/elasticsearch/index-settings/index-modules.md#index-max-analyzed-offset) which prevails when it’s set to lower value than the query setting.

tags_schema
:   Set to `styled` to use the built-in tag schema. The `styled` schema defines the following `pre_tags` and defines `post_tags` as `</em>`.

    ```html
    <em class="hlt1">, <em class="hlt2">, <em class="hlt3">,
    <em class="hlt4">, <em class="hlt5">, <em class="hlt6">,
    <em class="hlt7">, <em class="hlt8">, <em class="hlt9">,
    <em class="hlt10">
    ```


$$$highlighter-type$$$

type
:   The highlighter to use: `unified`, `plain`, or `fvh`. Defaults to `unified`.


### Highlighting examples [highlighting-examples]

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


## Combine matches on multiple fields [matched-fields]

::::{warning}
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

```console
PUT index1/_bulk?refresh=true
{"index": {"_id": "doc1" }}
{"comment": "run with scissors"}
{ "index" : {"_id": "doc2"} }
{"comment": "running with scissors"}
```

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

```console
PUT index2/_bulk?refresh=true
{"index": {"_id": "doc1" }}
{"comment": "run with scissors"}
{ "index" : {"_id": "doc2"} }
{"comment": "running with scissors"}
```

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

:::::{note}
There is a small amount of overhead involved with setting `matched_fields` to a non-empty array so always prefer

```js
    "highlight": {
        "fields": {
            "comment": {}
        }
    }
```

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

If the `number_of_fragments` option is set to `0`, `NullFragmenter` is used which does not fragment the text at all. This is useful for highlighting the entire contents of a document or field.


## How highlighters work internally [how-es-highlighters-work-internally]

Given a query and a text (the content of a document field), the goal of a highlighter is to find the best text fragments for the query, and highlight the query terms in the found fragments. For this, a highlighter needs to address several questions:

* How to break a text into fragments?
* How to find the best fragments among all fragments?
* How to highlight the query terms in a fragment?


### How to break a text into fragments? [_how_to_break_a_text_into_fragments]

Relevant settings: `fragment_size`, `fragmenter`, `type` of highlighter, `boundary_chars`, `boundary_max_scan`, `boundary_scanner`, `boundary_scanner_locale`.

Plain highlighter begins with analyzing the text using the given analyzer, and creating a token stream from it. Plain highlighter uses a very simple algorithm to break the token stream into fragments. It loops through terms in the token stream, and every time the current term’s end_offset exceeds `fragment_size` multiplied by the number of created fragments, a new fragment is created. A little more computation is done with using `span` fragmenter to avoid breaking up text between highlighted terms. But overall, since the breaking is done only by `fragment_size`, some fragments can be quite odd, e.g. beginning with a punctuation mark.

Unified or FVH highlighters do a better job of breaking up a text into fragments by utilizing Java’s `BreakIterator`. This ensures that a fragment is a valid sentence as long as `fragment_size` allows for this.


### How to find the best fragments? [_how_to_find_the_best_fragments]

Relevant settings: `number_of_fragments`.

To find the best, most relevant, fragments, a highlighter needs to score each fragment in respect to the given query. The goal is to score only those terms that participated in generating the *hit* on the document. For some complex queries, this is still work in progress.

The plain highlighter creates an in-memory index from the current token stream, and re-runs the original query criteria through Lucene’s query execution planner to get access to low-level match information for the current text. For more complex queries the original query could be converted to a span query, as span queries can handle phrases more accurately. Then this obtained low-level match information is used to score each individual fragment. The scoring method of the plain highlighter is quite simple. Each fragment is scored by the number of unique query terms found in this fragment. The score of individual term is equal to its boost, which is by default is 1. Thus, by default, a fragment that contains one unique query term, will get a score of 1; and a fragment that contains two unique query terms, will get a score of 2 and so on. The fragments are then sorted by their scores, so the highest scored fragments will be output first.

FVH doesn’t need to analyze the text and build an in-memory index, as it uses pre-indexed document term vectors, and finds among them terms that correspond to the query. FVH scores each fragment by the number of query terms found in this fragment. Similarly to plain highlighter, score of individual term is equal to its boost value. In contrast to plain highlighter, all query terms are counted, not only unique terms.

Unified highlighter can use pre-indexed term vectors or pre-indexed terms offsets, if they are available. Otherwise, similar to Plain Highlighter, it has to create an in-memory index from the text. Unified highlighter uses the BM25 scoring model to score fragments.


### How to highlight the query terms in a fragment? [_how_to_highlight_the_query_terms_in_a_fragment]

Relevant settings:  `pre-tags`, `post-tags`.

The goal is to highlight only those terms that participated in generating the *hit* on the document. For some complex boolean queries, this is still work in progress, as highlighters don’t reflect the boolean logic of a query and only extract leaf (terms, phrases, prefix etc) queries.

Plain highlighter given the token stream and the original text, recomposes the original text to highlight only terms from the token stream that are contained in the low-level match information structure from the previous step.

FVH and unified highlighter use intermediate data structures to represent fragments in some raw form, and then populate them with actual text.

A highlighter uses `pre-tags`, `post-tags` to encode highlighted terms.


### An example of the work of the unified highlighter [_an_example_of_the_work_of_the_unified_highlighter]

Let’s look in more details how unified highlighter works.

First, we create a index with a text field `content`, that will be indexed using `english` analyzer, and will be indexed without offsets or term vectors.

```js
PUT test_index
{
  "mappings": {
    "properties": {
      "content": {
        "type": "text",
        "analyzer": "english"
      }
    }
  }
}
```

We put the following document into the index:

```js
PUT test_index/_doc/doc1
{
  "content" : "For you I'm only a fox like a hundred thousand other foxes. But if you tame me, we'll need each other. You'll be the only boy in the world for me. I'll be the only fox in the world for you."
}
```

And we ran the following query with a highlight request:

```js
GET test_index/_search
{
  "query": {
    "match_phrase" : {"content" : "only fox"}
  },
  "highlight": {
    "type" : "unified",
    "number_of_fragments" : 3,
    "fields": {
      "content": {}
    }
  }
}
```

After `doc1` is found as a hit for this query, this hit will be passed to the unified highlighter for highlighting the field `content` of the document. Since the field `content` was not indexed either with offsets or term vectors, its raw field value will be analyzed, and in-memory index will be built from the terms that match the query:

```
{"token":"onli","start_offset":12,"end_offset":16,"position":3},
{"token":"fox","start_offset":19,"end_offset":22,"position":5},
{"token":"fox","start_offset":53,"end_offset":58,"position":11},
{"token":"onli","start_offset":117,"end_offset":121,"position":24},
{"token":"onli","start_offset":159,"end_offset":163,"position":34},
{"token":"fox","start_offset":164,"end_offset":167,"position":35}
```
Our complex phrase query will be converted to the span query: `spanNear([text:onli, text:fox], 0, true)`, meaning that we are looking for terms "onli: and "fox" within 0 distance from each other, and in the given order. The span query will be run against the created before in-memory index, to find the following match:

```
{"term":"onli", "start_offset":159, "end_offset":163},
{"term":"fox", "start_offset":164, "end_offset":167}
```
In our example, we have got a single match, but there could be several matches. Given the matches, the unified highlighter breaks the text of the field into so called "passages". Each passage must contain at least one match. The unified highlighter with the use of Java’s `BreakIterator` ensures that each passage represents a full sentence as long as it doesn’t exceed `fragment_size`. For our example, we have got a single passage with the following properties (showing only a subset of the properties here):

```
Passage:
    startOffset: 147
    endOffset: 189
    score: 3.7158387
    matchStarts: [159, 164]
    matchEnds: [163, 167]
    numMatches: 2
```
Notice how a passage has a score, calculated using the BM25 scoring formula adapted for passages. Scores allow us to choose the best scoring passages if there are more passages available than the requested by the user `number_of_fragments`. Scores also let us to sort passages by `order: "score"` if requested by the user.

As the final step, the unified highlighter will extract from the field’s text a string corresponding to each passage:

```
"I'll be the only fox in the world for you."
```
and will format with the tags <em> and </em> all matches in this string using the passages’s `matchStarts` and `matchEnds` information:

```
I'll be the <em>only</em> <em>fox</em> in the world for you.
```
This kind of formatted strings are the final result of the highlighter returned to the user.
