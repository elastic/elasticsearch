---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-icu-tokenizer.html
---

# ICU tokenizer [analysis-icu-tokenizer]

Tokenizes text into words on word boundaries, as defined in [UAX #29: Unicode Text Segmentation](https://www.unicode.org/reports/tr29/). It behaves much like the [`standard` tokenizer](/reference/text-analysis/analysis-standard-tokenizer.md), but adds better support for some Asian languages by using a dictionary-based approach to identify words in Thai, Lao, Chinese, Japanese, and Korean, and using custom rules to break Myanmar and Khmer text into syllables.

```console
PUT icu_sample
{
  "settings": {
    "index": {
      "analysis": {
        "analyzer": {
          "my_icu_analyzer": {
            "tokenizer": "icu_tokenizer"
          }
        }
      }
    }
  }
}
```

## Rules customization [_rules_customization]

::::{warning}
This functionality is marked as experimental in Lucene
::::


You can customize the `icu-tokenizer` behavior by specifying per-script rule files, see the [RBBI rules syntax reference](http://userguide.icu-project.org/boundaryanalysis#TOC-RBBI-Rules) for a more detailed explanation.

To add icu tokenizer rules, set the `rule_files` settings, which should contain a comma-separated list of `code:rulefile` pairs in the following format: [four-letter ISO 15924 script code](https://unicode.org/iso15924/iso15924-codes.html), followed by a colon, then a rule file name. Rule files are placed `ES_HOME/config` directory.

As a demonstration of how the rule files can be used, save the following user file to `$ES_HOME/config/KeywordTokenizer.rbbi`:

```text
.+ {200};
```

Then create an analyzer to use this rule file as follows:

```console
PUT icu_sample
{
  "settings": {
    "index": {
      "analysis": {
        "tokenizer": {
          "icu_user_file": {
            "type": "icu_tokenizer",
            "rule_files": "Latn:KeywordTokenizer.rbbi"
          }
        },
        "analyzer": {
          "my_analyzer": {
            "type": "custom",
            "tokenizer": "icu_user_file"
          }
        }
      }
    }
  }
}

GET icu_sample/_analyze
{
  "analyzer": "my_analyzer",
  "text": "Elasticsearch. Wow!"
}
```

The above `analyze` request returns the following:

```console-result
{
   "tokens": [
      {
         "token": "Elasticsearch. Wow!",
         "start_offset": 0,
         "end_offset": 19,
         "type": "<ALPHANUM>",
         "position": 0
      }
   ]
}
```


