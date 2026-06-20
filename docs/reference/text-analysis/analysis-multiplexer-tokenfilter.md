---
navigation_title: "Multiplexer"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-multiplexer-tokenfilter.html
---

# Multiplexer token filter [analysis-multiplexer-tokenfilter]


A token filter of type `multiplexer` will emit multiple tokens at the same position, each version of the token having been run through a different filter. Identical output tokens at the same position will be removed.

::::{warning}
If the incoming token stream has duplicate tokens, then these will also be removed by the multiplexer
::::



## Options [_options]

filters
:   a list of token filters to apply to incoming tokens. These can be any token filters defined elsewhere in the index mappings. Filters can be chained using a comma-delimited string, so for example `"lowercase, porter_stem"` would apply the `lowercase` filter and then the `porter_stem` filter to a single token.

::::{warning}
[Shingle](/reference/text-analysis/analysis-shingle-tokenfilter.md) or multi-word synonym token filters will not function normally when they are declared in the filters array because they read ahead internally which is unsupported by the multiplexer
::::


preserve_original
:   if `true` (the default) then emit the original token in addition to the filtered tokens


## Settings example [_settings_example]

You can set it up like:

```console
PUT /multiplexer_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_analyzer": {
          "tokenizer": "standard",
          "filter": [ "my_multiplexer" ]
        }
      },
      "filter": {
        "my_multiplexer": {
          "type": "multiplexer",
          "filters": [ "lowercase", "lowercase, porter_stem" ]
        }
      }
    }
  }
}
```

And test it like:

```console
POST /multiplexer_example/_analyze
{
  "analyzer" : "my_analyzer",
  "text" : "Going HOME"
}
```
% TEST[continued]

And it’d respond:

```console-result
{
  "tokens": [
    {
      "token": "Going",
      "start_offset": 0,
      "end_offset": 5,
      "type": "<ALPHANUM>",
      "position": 0
    },
    {
      "token": "going",
      "start_offset": 0,
      "end_offset": 5,
      "type": "<ALPHANUM>",
      "position": 0
    },
    {
      "token": "go",
      "start_offset": 0,
      "end_offset": 5,
      "type": "<ALPHANUM>",
      "position": 0
    },
    {
      "token": "HOME",
      "start_offset": 6,
      "end_offset": 10,
      "type": "<ALPHANUM>",
      "position": 1
    },
    {
      "token": "home",          <1>
      "start_offset": 6,
      "end_offset": 10,
      "type": "<ALPHANUM>",
      "position": 1
    }
  ]
}
```

1. The stemmer has also emitted a token `home` at position 1, but because it is a duplicate of this token it has been removed from the token stream


::::{note}
The synonym and synonym_graph filters use their preceding analysis chain to parse and analyse their synonym lists, and will throw an exception if that chain contains token filters that produce multiple tokens at the same position. If you want to apply synonyms to a token stream containing a multiplexer, then you should append the synonym filter to each relevant multiplexer filter list, rather than placing it after the multiplexer in the main token chain definition.
::::


