---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-nori-number.html
---

# nori_number token filter [analysis-nori-number]

The `nori_number` token filter normalizes Korean numbers to regular Arabic decimal numbers in half-width characters.

Korean numbers are often written using a combination of Hangul and Arabic numbers with various kinds of punctuation. For example, ３．２천 means 3200. This filter does this kind of normalization and allows a search for 3200 to match ３．２천 in text, but can also be used to make range facets based on the normalized numbers and so on.

::::{note}
Notice that this analyzer uses a token composition scheme and relies on punctuation tokens being found in the token stream. Please make sure your `nori_tokenizer` has `discard_punctuation` set to false. In case punctuation characters, such as U+FF0E(．), is removed from the token stream, this filter would find input tokens ３ and ２천 and give outputs 3 and 2000 instead of 3200, which is likely not the intended result.

If you want to remove punctuation characters from your index that are not part of normalized numbers, add a `stop` token filter with the punctuation you wish to remove after `nori_number` in your analyzer chain.

::::


Below are some examples of normalizations this filter supports. The input is untokenized text and the result is the single term attribute emitted for the input.

* 영영칠 → 7
* 일영영영 → 1000
* 삼천2백2십삼 → 3223
* 일조육백만오천일 → 1000006005001
* ３.２천 →  3200
* １.２만３４５.６７ → 12345.67
* 4,647.100 → 4647.1
* 15,7 → 157 (be aware of this weakness)

For example:

```console
PUT nori_sample
{
  "settings": {
    "index": {
      "analysis": {
        "analyzer": {
          "my_analyzer": {
            "tokenizer": "tokenizer_discard_puncuation_false",
            "filter": [
              "part_of_speech_stop_sp", "nori_number"
            ]
          }
        },
        "tokenizer": {
          "tokenizer_discard_puncuation_false": {
            "type": "nori_tokenizer",
            "discard_punctuation": "false"
          }
        },
        "filter": {
            "part_of_speech_stop_sp": {
                "type": "nori_part_of_speech",
                "stoptags": ["SP"]
            }
        }
      }
    }
  }
}

GET nori_sample/_analyze
{
  "analyzer": "my_analyzer",
  "text": "십만이천오백과 ３.２천"
}
```

Which results in:

```console-result
{
  "tokens" : [{
    "token" : "102500",
    "start_offset" : 0,
    "end_offset" : 6,
    "type" : "word",
    "position" : 0
  }, {
    "token" : "과",
    "start_offset" : 6,
    "end_offset" : 7,
    "type" : "word",
    "position" : 1
  }, {
    "token" : "3200",
    "start_offset" : 8,
    "end_offset" : 12,
    "type" : "word",
    "position" : 2
  }]
}
```

