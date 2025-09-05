---
navigation_title: "Stop"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-stop-analyzer.html
---

# Stop analyzer [analysis-stop-analyzer]


The `stop` analyzer is the same as the [`simple` analyzer](/reference/text-analysis/analysis-simple-analyzer.md) but adds support for removing stop words. It defaults to using the `_english_` stop words.


## Example output [_example_output_5]

```console
POST _analyze
{
  "analyzer": "stop",
  "text": "The 2 QUICK Brown-Foxes jumped over the lazy dog's bone."
}
```

The above sentence would produce the following terms:

```text
[ quick, brown, foxes, jumped, over, lazy, dog, s, bone ]
```


## Configuration [_configuration_6]

The `stop` analyzer accepts the following parameters:

`stopwords`
:   A pre-defined stop words list like `_english_` or an array containing a list of stop words. Defaults to `_english_`.

`stopwords_path`
:   The path to a file containing stop words. This path is relative to the Elasticsearch `config` directory.

See the [Stop Token Filter](/reference/text-analysis/analysis-stop-tokenfilter.md) for more information about stop word configuration.


## Example configuration [_example_configuration_5]

In this example, we configure the `stop` analyzer to use a specified list of words as stop words:

```console
PUT my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_stop_analyzer": {
          "type": "stop",
          "stopwords": ["the", "over"]
        }
      }
    }
  }
}

POST my-index-000001/_analyze
{
  "analyzer": "my_stop_analyzer",
  "text": "The 2 QUICK Brown-Foxes jumped over the lazy dog's bone."
}
```

The above example produces the following terms:

```text
[ quick, brown, foxes, jumped, lazy, dog, s, bone ]
```


## Definition [_definition_5]

It consists of:

Tokenizer
:   * [Lower Case Tokenizer](/reference/text-analysis/analysis-lowercase-tokenizer.md)


Token filters
:   * [Stop Token Filter](/reference/text-analysis/analysis-stop-tokenfilter.md)


If you need to customize the `stop` analyzer beyond the configuration parameters then you need to recreate it as a `custom` analyzer and modify it, usually by adding token filters. This would recreate the built-in `stop` analyzer and you can use it as a starting point for further customization:

```console
PUT /stop_example
{
  "settings": {
    "analysis": {
      "filter": {
        "english_stop": {
          "type":       "stop",
          "stopwords":  "_english_" <1>
        }
      },
      "analyzer": {
        "rebuilt_stop": {
          "tokenizer": "lowercase",
          "filter": [
            "english_stop"          <2>
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. You’d add any token filters after `english_stop`.


