---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-kuromoji-analyzer.html
---

# kuromoji analyzer [analysis-kuromoji-analyzer]

The `kuromoji` analyzer uses the following analysis chain:

* `CJKWidthCharFilter` from Lucene
* [`kuromoji_tokenizer`](/reference/elasticsearch-plugins/analysis-kuromoji-tokenizer.md)
* [`kuromoji_baseform`](/reference/elasticsearch-plugins/analysis-kuromoji-baseform.md) token filter
* [`kuromoji_part_of_speech`](/reference/elasticsearch-plugins/analysis-kuromoji-speech.md) token filter
* [`ja_stop`](/reference/elasticsearch-plugins/analysis-kuromoji-stop.md) token filter
* [`kuromoji_stemmer`](/reference/elasticsearch-plugins/analysis-kuromoji-stemmer.md) token filter
* [`lowercase`](/reference/text-analysis/analysis-lowercase-tokenfilter.md) token filter

It supports the `mode` and `user_dictionary` settings from [`kuromoji_tokenizer`](/reference/elasticsearch-plugins/analysis-kuromoji-tokenizer.md).


## Normalize full-width characters [kuromoji-analyzer-normalize-full-width-characters]

The `kuromoji_tokenizer` tokenizer uses characters from the MeCab-IPADIC dictionary to split text into tokens. The dictionary includes some full-width characters, such as `ｏ` and `ｆ`. If a text contains full-width characters, the tokenizer can produce unexpected tokens.

For example, the `kuromoji_tokenizer` tokenizer converts the text `Ｃｕｌｔｕｒｅ ｏｆ Ｊａｐａｎ` to the tokens `[ culture, o, f, japan ]` instead of `[ culture, of, japan ]`.

To avoid this, add the [`icu_normalizer` character filter](/reference/elasticsearch-plugins/analysis-icu-normalization-charfilter.md) to a custom analyzer based on the `kuromoji` analyzer. The `icu_normalizer` character filter converts full-width characters to their normal equivalents.

First, duplicate the `kuromoji` analyzer to create the basis for a custom analyzer. Then add the `icu_normalizer` character filter to the custom analyzer. For example:

```console
PUT index-00001
{
  "settings": {
    "index": {
      "analysis": {
        "analyzer": {
          "kuromoji_normalize": {                 <1>
            "char_filter": [
              "icu_normalizer"                    <2>
            ],
            "tokenizer": "kuromoji_tokenizer",
            "filter": [
              "kuromoji_baseform",
              "kuromoji_part_of_speech",
              "cjk_width",
              "ja_stop",
              "kuromoji_stemmer",
              "lowercase"
            ]
          }
        }
      }
    }
  }
}
```

1. Creates a new custom analyzer, `kuromoji_normalize`, based on the `kuromoji` analyzer.
2. Adds the `icu_normalizer` character filter to the analyzer.


