---
applies_to:
  stack:
  serverless:
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/index-options.html
---

# index_options [index-options]

The `index_options` parameter controls what information is added to the inverted index for search and highlighting purposes. Only term-based field types like [`text`](/reference/elasticsearch/mapping-reference/text.md) and [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) support this configuration.

:::{note}
The [`semantic_text`](../mapping-reference/semantic-text.md) field type also uses an `index_options` parameter. However, in that context, the parameter configures the underlying vector index settings (such as quantization or token pruning) rather than standard text indexing features. See the semantic text documentation to configure `index_options` for [sparse vectors](../mapping-reference/semantic-text-setup-configuration.md#index-options-sparse_vectors) or [dense vectors](../mapping-reference/semantic-text-setup-configuration.md#index-options-dense_vectors).
:::

The parameter accepts one of the following values. Each value retrieves information from the previous listed values. For example, `freqs` contains `docs`; `positions` contains both `freqs` and `docs`.

`docs`
:   Only the doc number is indexed. Can answer the question *Does this term exist in this field?*

`freqs`
:   Doc number and term frequencies are indexed. Term frequencies are used to score repeated terms higher than single terms.

`positions` (default)
:   Doc number, term frequencies, and term positions (or order) are indexed. Positions can be used for [proximity or phrase queries](/reference/query-languages/query-dsl/query-dsl-match-query-phrase.md).

`offsets`
:   Doc number, term frequencies, positions, and start and end character offsets (which map the term back to the original string) are indexed. Offsets are used by the [unified highlighter](/reference/elasticsearch/rest-apis/highlighting.md#unified-highlighter) to speed up highlighting.

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "text": {
        "type": "text",
        "index_options": "offsets"
      }
    }
  }
}

PUT my-index-000001/_doc/1
{
  "text": "Quick brown fox"
}

GET my-index-000001/_search
{
  "query": {
    "match": {
      "text": "brown fox"
    }
  },
  "highlight": {
    "fields": {
      "text": {} <1>
    }
  }
}
```

1. The `text` field will use the postings for the highlighting by default because `offsets` are indexed.


