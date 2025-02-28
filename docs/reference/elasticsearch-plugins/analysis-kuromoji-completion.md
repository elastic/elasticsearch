---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-kuromoji-completion.html
---

# kuromoji_completion token filter [analysis-kuromoji-completion]

The `kuromoji_completion` token filter adds Japanese romanized tokens to the term attributes along with the original tokens (surface forms).

```console
GET _analyze
{
  "analyzer": "kuromoji_completion",
  "text": "寿司" <1>
}
```

1. Returns `寿司`, `susi` (Kunrei-shiki) and `sushi` (Hepburn-shiki).


The `kuromoji_completion` token filter accepts the following settings:

`mode`
:   The tokenization mode determines how the tokenizer handles compound and unknown words. It can be set to:

`index`
:   Simple romanization. Expected to be used when indexing.

`query`
:   Input Method aware romanization. Expected to be used when querying.

Defaults to `index`.


