---
navigation_title: "Keyword"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-keyword-analyzer.html
---

# Keyword analyzer [analysis-keyword-analyzer]


The `keyword` analyzer is a noop analyzer which returns the entire input string as a single token.


## Example output [_example_output_2]

```console
POST _analyze
{
  "analyzer": "keyword",
  "text": "The 2 QUICK Brown-Foxes jumped over the lazy dog's bone."
}
```

The above sentence would produce the following single term:

```text
[ The 2 QUICK Brown-Foxes jumped over the lazy dog's bone. ]
```


## Configuration [_configuration_3]

The `keyword` analyzer is not configurable.


## Definition [_definition_2]

The `keyword` analyzer consists of:

Tokenizer
:   * [Keyword Tokenizer](/reference/text-analysis/analysis-keyword-tokenizer.md)


If you need to customize the `keyword` analyzer then you need to recreate it as a `custom` analyzer and modify it, usually by adding token filters. Usually, you should prefer the [Keyword type](/reference/elasticsearch/mapping-reference/keyword.md) when you want strings that are not split into tokens, but just in case you need it, this would recreate the built-in `keyword` analyzer and you can use it as a starting point for further customization:

```console
PUT /keyword_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "rebuilt_keyword": {
          "tokenizer": "keyword",
          "filter": [         <1>
          ]
        }
      }
    }
  }
}
```

1. You’d add any token filters here.


