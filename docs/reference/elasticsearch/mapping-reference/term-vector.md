---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/term-vector.html
---

# term_vector [term-vector]

Term vectors contain information about the terms produced by the [analysis](docs-content://manage-data/data-store/text-analysis.md) process, including:

* a list of terms.
* the position (or order) of each term.
* the start and end character offsets mapping the term to its origin in the original string.
* payloads (if they are available) — user-defined binary data associated with each term position.

These term vectors can be stored so that they can be retrieved for a particular document.

Refer to the [term vectors API examples](../rest-apis/term-vectors-examples.md) page for usage examples.

The `term_vector` setting accepts:

`no`
:   No term vectors are stored. (default)

`yes`
:   Just the terms in the field are stored.

`with_positions`
:   Terms and positions are stored.

`with_offsets`
:   Terms and character offsets are stored.

`with_positions_offsets`
:   Terms, positions, and character offsets are stored.

`with_positions_payloads`
:   Terms, positions, and payloads are stored.

`with_positions_offsets_payloads`
:   Terms, positions, offsets and payloads are stored.

The fast vector highlighter requires `with_positions_offsets`. [The term vectors API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-termvectors) can retrieve whatever is stored.

::::{warning}
Setting `with_positions_offsets` will double the size of a field’s index.
::::


```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "text": {
        "type":        "text",
        "term_vector": "with_positions_offsets"
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

1. The fast vector highlighter will be used by default for the `text` field because term vectors are enabled.


