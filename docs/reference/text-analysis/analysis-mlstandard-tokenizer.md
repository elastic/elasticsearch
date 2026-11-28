---
navigation_title: "ML standard"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-mlstandard-tokenizer.html
---

# ML standard tokenizer [analysis-mlstandard-tokenizer]


The `ml_standard` tokenizer tokenizes log messages and other machine-generated text for use with machine learning categorization. It is similar to the `whitespace` tokenizer, but applies additional rules that work well for log formats in English.

The tokenizer keeps URLs, email addresses, Unix paths, and Windows paths as single tokens where possible. It also filters out tokens that are hexadecimal numbers or begin with a digit.


## Example output [ml-standard-tokenizer-example-output]

```console
POST _analyze
{
  "tokenizer": "ml_standard",
  "text": "Failed to bootstrap path: path = /usr/libexec/mdmclient, error = 108: Invalid path"
}
```

The above sentence would produce the following terms:

```text
[ Failed, to, bootstrap, path, path, /usr/libexec/mdmclient, error, Invalid, path ]
```


## Configuration [ml-standard-tokenizer-configuration]

The `ml_standard` tokenizer is not configurable.
