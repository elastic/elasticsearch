---
navigation_title: "Edge n-gram"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-edgengram-tokenizer.html
---

# Edge n-gram tokenizer [analysis-edgengram-tokenizer]


The `edge_ngram` tokenizer first breaks text down into words whenever it encounters one of a list of specified characters, then it emits [N-grams](https://en.wikipedia.org/wiki/N-gram) of each word where the start of the N-gram is anchored to the beginning of the word.

Edge N-Grams are useful for *search-as-you-type* queries.

::::{tip}
When you need *search-as-you-type* for text which has a widely known order, such as movie or song titles, the completion suggester is a much more efficient choice than edge N-grams. Edge N-grams have the advantage when trying to autocomplete words that can appear in any order. For more information about completion suggesters, refer to [](/reference/elasticsearch/rest-apis/search-suggesters.md).
::::



## Example output [_example_output_9]

With the default settings, the `edge_ngram` tokenizer treats the initial text as a single token and produces N-grams with minimum length `1` and maximum length `2`:

```console
POST _analyze
{
  "tokenizer": "edge_ngram",
  "text": "Quick Fox"
}
```

The above sentence would produce the following terms:

```text
[ Q, Qu ]
```

::::{note}
These default gram lengths are almost entirely useless. You need to configure the `edge_ngram` before using it.
::::



## Configuration [_configuration_10]

The `edge_ngram` tokenizer accepts the following parameters:

`min_gram`
:   Minimum length of characters in a gram. Defaults to `1`.

`max_gram`
:   Maximum length of characters in a gram. Defaults to `2`.

See [Limitations of the `max_gram` parameter](#max-gram-limits).


`token_chars`
:   Character classes that should be included in a token. Elasticsearch will split on characters that don’t belong to the classes specified. Defaults to `[]` (keep all characters).

    Character classes may be any of the following:

    * `letter` —      for example `a`, `b`, `ï` or `京`
    * `digit` —       for example `3` or `7`
    * `whitespace` —  for example `" "` or `"\n"`
    * `punctuation` — for example `!` or `"`
    * `symbol` —      for example `$` or `√`
    * `custom` —      custom characters which need to be set using the `custom_token_chars` setting.


`custom_token_chars`
:   Custom characters that should be treated as part of a token. For example, setting this to `+-_` will make the tokenizer treat the plus, minus and underscore sign as part of a token.


## Limitations of the `max_gram` parameter [max-gram-limits]

The `edge_ngram` tokenizer’s `max_gram` value limits the character length of tokens. When the `edge_ngram` tokenizer is used with an index analyzer, this means search terms longer than the `max_gram` length may not match any indexed terms.

For example, if the `max_gram` is `3`, searches for `apple` won’t match the indexed term `app`.

To account for this, you can use the [`truncate`](/reference/text-analysis/analysis-truncate-tokenfilter.md) token filter with a search analyzer to shorten search terms to the `max_gram` character length. However, this could return irrelevant results.

For example, if the `max_gram` is `3` and search terms are truncated to three characters, the search term `apple` is shortened to `app`. This means searches for `apple` return any indexed terms matching `app`, such as `apply`, `approximate` and `apple`.

We recommend testing both approaches to see which best fits your use case and desired search experience.


## Example configuration [_example_configuration_7]

In this example, we configure the `edge_ngram` tokenizer to treat letters and digits as tokens, and to produce grams with minimum length `2` and maximum length `10`:

```console
PUT my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_analyzer": {
          "tokenizer": "my_tokenizer"
        }
      },
      "tokenizer": {
        "my_tokenizer": {
          "type": "edge_ngram",
          "min_gram": 2,
          "max_gram": 10,
          "token_chars": [
            "letter",
            "digit"
          ]
        }
      }
    }
  }
}

POST my-index-000001/_analyze
{
  "analyzer": "my_analyzer",
  "text": "2 Quick Foxes."
}
```

The above example produces the following terms:

```text
[ Qu, Qui, Quic, Quick, Fo, Fox, Foxe, Foxes ]
```

Usually we recommend using the same `analyzer` at index time and at search time. In the case of the `edge_ngram` tokenizer, the advice is different. It only makes sense to use the `edge_ngram` tokenizer at index time, to ensure that partial words are available for matching in the index. At search time, just search for the terms the user has typed in, for instance: `Quick Fo`.

Below is an example of how to set up a field for *search-as-you-type*.

Note that the `max_gram` value for the index analyzer is `10`, which limits indexed terms to 10 characters. Search terms are not truncated, meaning that search terms longer than 10 characters may not match any indexed terms.

```console
PUT my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "autocomplete": {
          "tokenizer": "autocomplete",
          "filter": [
            "lowercase"
          ]
        },
        "autocomplete_search": {
          "tokenizer": "lowercase"
        }
      },
      "tokenizer": {
        "autocomplete": {
          "type": "edge_ngram",
          "min_gram": 2,
          "max_gram": 10,
          "token_chars": [
            "letter"
          ]
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "title": {
        "type": "text",
        "analyzer": "autocomplete",
        "search_analyzer": "autocomplete_search"
      }
    }
  }
}

PUT my-index-000001/_doc/1
{
  "title": "Quick Foxes" <1>
}

POST my-index-000001/_refresh

GET my-index-000001/_search
{
  "query": {
    "match": {
      "title": {
        "query": "Quick Fo", <2>
        "operator": "and"
      }
    }
  }
}
```

1. The `autocomplete` analyzer indexes the terms `[qu, qui, quic, quick, fo, fox, foxe, foxes]`.
2. The `autocomplete_search` analyzer searches for the terms `[quick, fo]`, both of which appear in the index.


