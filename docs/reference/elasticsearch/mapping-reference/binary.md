---
navigation_title: "Binary"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/binary.html
---

# Binary field type [binary]


The `binary` type accepts a binary value as a [Base64](https://en.wikipedia.org/wiki/Base64) encoded string. The field is not stored by default and is not searchable:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "name": {
        "type": "text"
      },
      "blob": {
        "type": "binary"
      }
    }
  }
}

PUT my-index-000001/_doc/1
{
  "name": "Some binary blob",
  "blob": "U29tZSBiaW5hcnkgYmxvYg==" <1>
}
```

1. The Base64 encoded binary value must not have embedded newlines `\n`.


## Parameters for `binary` fields [binary-params]

The following parameters are accepted by `binary` fields:

[`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md)
:   Should the field be stored on disk in a column-stride fashion, so that it can later be used for sorting, aggregations, or scripting? Accepts `true` or `false` (default). This parameter will be automatically set to `true` for TSDB indices (indices that have `index.mode` set to `time_series`).

[`store`](/reference/elasticsearch/mapping-reference/mapping-store.md)
:   Whether the field value should be stored and retrievable separately from the [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) field. Accepts `true` or `false` (default).


## Synthetic `_source` [binary-synthetic-source]

::::{important}
Synthetic `_source` is Generally Available only for TSDB indices (indices that have `index.mode` set to `time_series`). For other indices synthetic `_source` is in technical preview. Features in technical preview may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


Synthetic source may sort `binary` values in order of their byte representation. For example:

$$$synthetic-source-binary-example$$$

```console
PUT idx
{
  "settings": {
    "index": {
      "mapping": {
        "source": {
          "mode": "synthetic"
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "binary": { "type": "binary", "doc_values": true }
    }
  }
}
PUT idx/_doc/1
{
  "binary": ["IAA=", "EAA="]
}
```
% TEST[s/$/\nGET idx\/_doc\/1?filter_path=_source\n/]

Will become:

```console-result
{
  "binary": ["EAA=", "IAA="]
}
```
% TEST[s/^/{"_source":/ s/\n$/}/]


