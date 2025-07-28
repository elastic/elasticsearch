---
navigation_title: "Lowercase"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-lowercase-tokenizer.html
---

# Lowercase tokenizer [analysis-lowercase-tokenizer]


The `lowercase` tokenizer, like the [`letter` tokenizer](/reference/text-analysis/analysis-letter-tokenizer.md) breaks text into terms whenever it encounters a character which is not a letter, but it also lowercases all terms. It is functionally equivalent to the [`letter` tokenizer](/reference/text-analysis/analysis-letter-tokenizer.md) combined with the [`lowercase` token filter](/reference/text-analysis/analysis-lowercase-tokenfilter.md), but is more efficient as it performs both steps in a single pass.


## Example output [_example_output_12]

```console
POST _analyze
{
  "tokenizer": "lowercase",
  "text": "The 2 QUICK Brown-Foxes jumped over the lazy dog's bone."
}
```

The above sentence would produce the following terms:

```text
[ the, quick, brown, foxes, jumped, over, the, lazy, dog, s, bone ]
```


## Configuration [_configuration_13]

The `lowercase` tokenizer is not configurable.

