---
navigation_title: "Standard"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-standard-tokenizer.html
---

# Standard tokenizer [analysis-standard-tokenizer]


The `standard` tokenizer provides grammar based tokenization (based on the Unicode Text Segmentation algorithm, as specified in [Unicode Standard Annex #29](https://unicode.org/reports/tr29/)) and works well for most languages.


## Example output [_example_output_16]

```console
POST _analyze
{
  "tokenizer": "standard",
  "text": "The 2 QUICK Brown-Foxes jumped over the lazy dog's bone."
}
```

The above sentence would produce the following terms:

```text
[ The, 2, QUICK, Brown, Foxes, jumped, over, the, lazy, dog's, bone ]
```


## Configuration [_configuration_19]

The `standard` tokenizer accepts the following parameters:

`max_token_length`
:   The maximum token length. If a token is seen that exceeds this length then it is split at `max_token_length` intervals. Defaults to `255`.


## Example configuration [_example_configuration_13]

In this example, we configure the `standard` tokenizer to have a `max_token_length` of 5 (for demonstration purposes):

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
          "type": "standard",
          "max_token_length": 5
        }
      }
    }
  }
}

POST my-index-000001/_analyze
{
  "analyzer": "my_analyzer",
  "text": "The 2 QUICK Brown-Foxes jumped over the lazy dog's bone."
}
```

The above example produces the following terms:

```text
[ The, 2, QUICK, Brown, Foxes, jumpe, d, over, the, lazy, dog's, bone ]
```

