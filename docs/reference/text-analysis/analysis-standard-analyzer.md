---
navigation_title: "Standard"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-standard-analyzer.html
---

# Standard analyzer [analysis-standard-analyzer]


The `standard` analyzer is the default analyzer which is used if none is specified. It provides grammar based tokenization (based on the Unicode Text Segmentation algorithm, as specified in [Unicode Standard Annex #29](https://unicode.org/reports/tr29/)) and works well for most languages.


## Example output [_example_output_4]

```console
POST _analyze
{
  "analyzer": "standard",
  "text": "The 2 QUICK Brown-Foxes jumped over the lazy dog's bone."
}
```

The above sentence would produce the following terms:

```text
[ the, 2, quick, brown, foxes, jumped, over, the, lazy, dog's, bone ]
```


## Configuration [_configuration_5]

The `standard` analyzer accepts the following parameters:

`max_token_length`
:   The maximum token length. If a token is seen that exceeds this length then it is split at `max_token_length` intervals. Defaults to `255`.

`stopwords`
:   A pre-defined stop words list like `_english_` or an array containing a list of stop words. Defaults to `_none_`.

`stopwords_path`
:   The path to a file containing stop words.

See the [Stop Token Filter](/reference/text-analysis/analysis-stop-tokenfilter.md) for more information about stop word configuration.


## Example configuration [_example_configuration_4]

In this example, we configure the `standard` analyzer to have a `max_token_length` of 5 (for demonstration purposes), and to use the pre-defined list of English stop words:

```console
PUT my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_english_analyzer": {
          "type": "standard",
          "max_token_length": 5,
          "stopwords": "_english_"
        }
      }
    }
  }
}

POST my-index-000001/_analyze
{
  "analyzer": "my_english_analyzer",
  "text": "The 2 QUICK Brown-Foxes jumped over the lazy dog's bone."
}
```

The above example produces the following terms:

```text
[ 2, quick, brown, foxes, jumpe, d, over, lazy, dog's, bone ]
```


## Definition [_definition_4]

The `standard` analyzer consists of:

Tokenizer
:   * [Standard Tokenizer](/reference/text-analysis/analysis-standard-tokenizer.md)


Token Filters
:   * [Lower Case Token Filter](/reference/text-analysis/analysis-lowercase-tokenfilter.md)
* [Stop Token Filter](/reference/text-analysis/analysis-stop-tokenfilter.md) (disabled by default)


If you need to customize the `standard` analyzer beyond the configuration parameters then you need to recreate it as a `custom` analyzer and modify it, usually by adding token filters. This would recreate the built-in `standard` analyzer and you can use it as a starting point:

```console
PUT /standard_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "rebuilt_standard": {
          "tokenizer": "standard",
          "filter": [
            "lowercase"       <1>
          ]
        }
      }
    }
  }
}
```

1. You’d add any token filters after `lowercase`.


