---
navigation_title: "UAX URL email"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-uaxurlemail-tokenizer.html
---

# UAX URL email tokenizer [analysis-uaxurlemail-tokenizer]


The `uax_url_email` tokenizer is like the [`standard` tokenizer](/reference/text-analysis/analysis-standard-tokenizer.md) except that it recognises URLs and email addresses as single tokens.


## Example output [_example_output_18]

```console
POST _analyze
{
  "tokenizer": "uax_url_email",
  "text": "Email me at john.smith@global-international.com"
}
```

The above sentence would produce the following terms:

```text
[ Email, me, at, john.smith@global-international.com ]
```

while the `standard` tokenizer would produce:

```text
[ Email, me, at, john.smith, global, international.com ]
```


## Configuration [_configuration_21]

The `uax_url_email` tokenizer accepts the following parameters:

`max_token_length`
:   The maximum token length. If a token is seen that exceeds this length then it is split at `max_token_length` intervals. Defaults to `255`.


## Example configuration [_example_configuration_14]

In this example, we configure the `uax_url_email` tokenizer to have a `max_token_length` of 5 (for demonstration purposes):

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
          "type": "uax_url_email",
          "max_token_length": 5
        }
      }
    }
  }
}

POST my-index-000001/_analyze
{
  "analyzer": "my_analyzer",
  "text": "john.smith@global-international.com"
}
```

The above example produces the following terms:

```text
[ john, smith, globa, l, inter, natio, nal.c, om ]
```

