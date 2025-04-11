---
navigation_title: "N-gram"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-ngram-tokenizer.html
---

# N-gram tokenizer [analysis-ngram-tokenizer]


The `ngram` tokenizer first breaks text down into words whenever it encounters one of a list of specified characters, then it emits [N-grams](https://en.wikipedia.org/wiki/N-gram) of each word of the specified length.

N-grams are like a sliding window that moves across the word - a continuous sequence of characters of the specified length. They are useful for querying languages that don’t use spaces or that have long compound words, like German.


## Example output [_example_output_13]

With the default settings, the `ngram` tokenizer treats the initial text as a single token and produces N-grams with minimum length `1` and maximum length `2`:

```console
POST _analyze
{
  "tokenizer": "ngram",
  "text": "Quick Fox"
}
```

The above sentence would produce the following terms:

```text
[ Q, Qu, u, ui, i, ic, c, ck, k, "k ", " ", " F", F, Fo, o, ox, x ]
```


## Configuration [_configuration_14]

The `ngram` tokenizer accepts the following parameters:

`min_gram`
:   Minimum length of characters in a gram. Defaults to `1`.

`max_gram`
:   Maximum length of characters in a gram. Defaults to `2`.

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

::::{tip}
It usually makes sense to set `min_gram` and `max_gram` to the same value. The smaller the length, the more documents will match but the lower the quality of the matches. The longer the length, the more specific the matches. A tri-gram (length `3`) is a good place to start.
::::


The index level setting `index.max_ngram_diff` controls the maximum allowed difference between `max_gram` and `min_gram`.


## Example configuration [_example_configuration_8]

In this example, we configure the `ngram` tokenizer to treat letters and digits as tokens, and to produce tri-grams (grams of length `3`):

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
          "type": "ngram",
          "min_gram": 3,
          "max_gram": 3,
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
[ Qui, uic, ick, Fox, oxe, xes ]
```

