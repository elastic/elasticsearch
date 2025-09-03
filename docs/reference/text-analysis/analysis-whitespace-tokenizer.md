---
navigation_title: "Whitespace"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-whitespace-tokenizer.html
---

# Whitespace tokenizer [analysis-whitespace-tokenizer]


The `whitespace` tokenizer breaks text into terms whenever it encounters a whitespace character.


## Example output [_example_output_19]

```console
POST _analyze
{
  "tokenizer": "whitespace",
  "text": "The 2 QUICK Brown-Foxes jumped over the lazy dog's bone."
}
```

The above sentence would produce the following terms:

```text
[ The, 2, QUICK, Brown-Foxes, jumped, over, the, lazy, dog's, bone. ]
```


## Configuration [_configuration_22]

The `whitespace` tokenizer accepts the following parameters:

`max_token_length`
:   The maximum token length. If a token is seen that exceeds this length then it is split at `max_token_length` intervals. Defaults to `255`.

