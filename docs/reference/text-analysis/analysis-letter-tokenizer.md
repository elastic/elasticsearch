---
navigation_title: "Letter"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-letter-tokenizer.html
---

# Letter tokenizer [analysis-letter-tokenizer]


The `letter` tokenizer breaks text into terms whenever it encounters a character which is not a letter. It does a reasonable job for most European languages, but does a terrible job for some Asian languages, where words are not separated by spaces.


## Example output [_example_output_11]

```console
POST _analyze
{
  "tokenizer": "letter",
  "text": "The 2 QUICK Brown-Foxes jumped over the lazy dog's bone."
}
```

The above sentence would produce the following terms:

```text
[ The, QUICK, Brown, Foxes, jumped, over, the, lazy, dog, s, bone ]
```


## Configuration [_configuration_12]

The `letter` tokenizer is not configurable.

