---
navigation_title: "Whitespace"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-whitespace-analyzer.html
---

# Whitespace analyzer [analysis-whitespace-analyzer]


The `whitespace` analyzer breaks text into terms whenever it encounters a whitespace character.


## Example output [_example_output_6]

```console
POST _analyze
{
  "analyzer": "whitespace",
  "text": "The 2 QUICK Brown-Foxes jumped over the lazy dog's bone."
}
```

The above sentence would produce the following terms:

```text
[ The, 2, QUICK, Brown-Foxes, jumped, over, the, lazy, dog's, bone. ]
```


## Configuration [_configuration_7]

The `whitespace` analyzer is not configurable.


## Definition [_definition_6]

It consists of:

Tokenizer
:   * [Whitespace Tokenizer](/reference/text-analysis/analysis-whitespace-tokenizer.md)


If you need to customize the `whitespace` analyzer then you need to recreate it as a `custom` analyzer and modify it, usually by adding token filters. This would recreate the built-in `whitespace` analyzer and you can use it as a starting point for further customization:

```console
PUT /whitespace_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "rebuilt_whitespace": {
          "tokenizer": "whitespace",
          "filter": [         <1>
          ]
        }
      }
    }
  }
}
```
% TEST[s/\n$/\nstartyaml\n  - compare_analyzers: {index: whitespace_example, first: whitespace, second: rebuilt_whitespace}\nendyaml\n/]

1. You’d add any token filters here.


