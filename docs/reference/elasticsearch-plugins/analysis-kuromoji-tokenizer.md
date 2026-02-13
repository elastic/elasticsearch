---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-kuromoji-tokenizer.html
---

# kuromoji_tokenizer [analysis-kuromoji-tokenizer]

The `kuromoji_tokenizer` accepts the following settings:

`mode`
:   The tokenization mode determines how the tokenizer handles compound and unknown words. It can be set to:

`normal`
:   Normal segmentation, no decomposition for compounds. Example output:

    ```
    関西国際空港
    アブラカダブラ
    ```


`search`
:   Segmentation geared towards search. This includes a decompounding process for long nouns, also including the full compound token as a synonym. Example output:

    ```
    関西, 関西国際空港, 国際, 空港
    アブラカダブラ
    ```


`extended`
:   Extended mode outputs unigrams for unknown words. Example output:

    ```
    関西, 関西国際空港, 国際, 空港
    ア, ブ, ラ, カ, ダ, ブ, ラ
    ```



`discard_punctuation`
:   Whether punctuation should be discarded from the output. Defaults to `true`.

`lenient`
:   Whether the `user_dictionary` should be deduplicated on the provided `text`. False by default causing duplicates to generate an error.

`user_dictionary`
:   The Kuromoji tokenizer uses the MeCab-IPADIC dictionary by default. A `user_dictionary` may be appended to the default dictionary. The dictionary should have the following CSV format:

```text
<text>,<token 1> ... <token n>,<reading 1> ... <reading n>,<part-of-speech tag>
```


As a demonstration of how the user dictionary can be used, save the following dictionary to `$ES_HOME/config/userdict_ja.txt`:

```text
東京スカイツリー,東京 スカイツリー,トウキョウ スカイツリー,カスタム名詞
```

You can also inline the rules directly in the tokenizer definition using the `user_dictionary_rules` option:

```console
PUT kuromoji_sample
{
  "settings": {
    "index": {
      "analysis": {
        "tokenizer": {
          "kuromoji_user_dict": {
            "type": "kuromoji_tokenizer",
            "mode": "extended",
            "user_dictionary_rules": ["東京スカイツリー,東京 スカイツリー,トウキョウ スカイツリー,カスタム名詞"]
          }
        },
        "analyzer": {
          "my_analyzer": {
            "type": "custom",
            "tokenizer": "kuromoji_user_dict"
          }
        }
      }
    }
  }
}
```

`nbest_cost`/`nbest_examples`
:   Additional expert user parameters `nbest_cost` and `nbest_examples` can be used to include additional tokens that are most likely according to the statistical model. If both parameters are used, the largest number of both is applied.

`nbest_cost`
:   The `nbest_cost` parameter specifies an additional Viterbi cost. The KuromojiTokenizer will include all tokens in Viterbi paths that are within the nbest_cost value of the best path.

`nbest_examples`
:   The `nbest_examples` can be used to find a `nbest_cost` value based on examples. For example, a value of /箱根山-箱根/成田空港-成田/ indicates that in the texts, 箱根山 (Mt. Hakone) and 成田空港 (Narita Airport) we’d like a cost that gives is us 箱根 (Hakone) and 成田 (Narita).


Then create an analyzer as follows:

```console
PUT kuromoji_sample
{
  "settings": {
    "index": {
      "analysis": {
        "tokenizer": {
          "kuromoji_user_dict": {
            "type": "kuromoji_tokenizer",
            "mode": "extended",
            "discard_punctuation": "false",
            "user_dictionary": "userdict_ja.txt",
            "lenient": "true"
          }
        },
        "analyzer": {
          "my_analyzer": {
            "type": "custom",
            "tokenizer": "kuromoji_user_dict"
          }
        }
      }
    }
  }
}

GET kuromoji_sample/_analyze
{
  "analyzer": "my_analyzer",
  "text": "東京スカイツリー"
}
```

The above `analyze` request returns the following:

```console-result
{
  "tokens" : [ {
    "token" : "東京",
    "start_offset" : 0,
    "end_offset" : 2,
    "type" : "word",
    "position" : 0
  }, {
    "token" : "スカイツリー",
    "start_offset" : 2,
    "end_offset" : 8,
    "type" : "word",
    "position" : 1
  } ]
}
```

`discard_compound_token`
:   Whether original compound tokens should be discarded from the output with `search` mode. Defaults to `false`. Example output with `search` or `extended` mode and this option `true`:

    ```
    関西, 国際, 空港
    ```


::::{note}
If a text contains full-width characters, the `kuromoji_tokenizer` tokenizer can produce unexpected tokens. To avoid this, add the [`icu_normalizer` character filter](/reference/elasticsearch-plugins/analysis-icu-normalization-charfilter.md) to your analyzer. See [Normalize full-width characters](/reference/elasticsearch-plugins/analysis-kuromoji-analyzer.md#kuromoji-analyzer-normalize-full-width-characters).
::::


