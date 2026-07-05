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

<!--
```console-result
{
  "tokens": [
    {
      "token": "The",
      "start_offset": 0,
      "end_offset": 3,
      "type": "word",
      "position": 0
    },
    {
      "token": "2",
      "start_offset": 4,
      "end_offset": 5,
      "type": "word",
      "position": 1
    },
    {
      "token": "QUICK",
      "start_offset": 6,
      "end_offset": 11,
      "type": "word",
      "position": 2
    },
    {
      "token": "Brown-Foxes",
      "start_offset": 12,
      "end_offset": 23,
      "type": "word",
      "position": 3
    },
    {
      "token": "jumped",
      "start_offset": 24,
      "end_offset": 30,
      "type": "word",
      "position": 4
    },
    {
      "token": "over",
      "start_offset": 31,
      "end_offset": 35,
      "type": "word",
      "position": 5
    },
    {
      "token": "the",
      "start_offset": 36,
      "end_offset": 39,
      "type": "word",
      "position": 6
    },
    {
      "token": "lazy",
      "start_offset": 40,
      "end_offset": 44,
      "type": "word",
      "position": 7
    },
    {
      "token": "dog's",
      "start_offset": 45,
      "end_offset": 50,
      "type": "word",
      "position": 8
    },
    {
      "token": "bone.",
      "start_offset": 51,
      "end_offset": 56,
      "type": "word",
      "position": 9
    }
  ]
}
```
-->

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


