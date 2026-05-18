---
navigation_title: "Classic"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-classic-tokenizer.html
---

# Classic tokenizer [analysis-classic-tokenizer]


The `classic` tokenizer is a grammar based tokenizer that is good for English language documents. This tokenizer has heuristics for special treatment of acronyms, company names, email addresses, and internet host names. However, these rules don’t always work, and the tokenizer doesn’t work well for most languages other than English:

* It splits words at most punctuation characters, removing punctuation. However, a dot that’s not followed by whitespace is considered part of a token.
* It splits words at hyphens, unless there’s a number in the token, in which case the whole token is interpreted as a product number and is not split.
* It recognizes email addresses and internet hostnames as one token.


## Example output [_example_output_8]

```console
POST _analyze
{
  "tokenizer": "classic",
  "text": "The 2 QUICK Brown-Foxes jumped over the lazy dog's bone."
}
```

The above sentence would produce the following terms:

```text
[ The, 2, QUICK, Brown, Foxes, jumped, over, the, lazy, dog's, bone ]
```


## Configuration [_configuration_9]

The `classic` tokenizer accepts the following parameters:

`max_token_length`
:   The maximum token length. If a token is seen that exceeds this length then it is split at `max_token_length` intervals. Defaults to `255`.


## Example configuration [_example_configuration_6]

In this example, we configure the `classic` tokenizer to have a `max_token_length` of 5 (for demonstration purposes):

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
          "type": "classic",
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

