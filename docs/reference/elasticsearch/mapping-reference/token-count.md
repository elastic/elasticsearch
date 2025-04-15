---
navigation_title: "Token count"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/token-count.html
---

# Token count field type [token-count]


A field of type `token_count` is really an [`integer`](/reference/elasticsearch/mapping-reference/number.md) field which accepts string values, analyzes them, then indexes the number of tokens in the string.

For instance:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "name": { <1>
        "type": "text",
        "fields": {
          "length": { <2>
            "type":     "token_count",
            "analyzer": "standard"
          }
        }
      }
    }
  }
}

PUT my-index-000001/_doc/1
{ "name": "John Smith" }

PUT my-index-000001/_doc/2
{ "name": "Rachel Alice Williams" }

GET my-index-000001/_search
{
  "query": {
    "term": {
      "name.length": 3 <3>
    }
  }
}
```

1. The `name` field is a [`text`](/reference/elasticsearch/mapping-reference/text.md) field which uses the default `standard` analyzer.
2. The `name.length` field is a `token_count` [multi-field](/reference/elasticsearch/mapping-reference/multi-fields.md) which will index the number of tokens in the `name` field.
3. This query matches only the document containing `Rachel Alice Williams`, as it contains three tokens.


## Parameters for `token_count` fields [token-count-params]

The following parameters are accepted by `token_count` fields:

[`analyzer`](/reference/elasticsearch/mapping-reference/analyzer.md)
:   The [analyzer](docs-content://manage-data/data-store/text-analysis.md) which should be used to analyze the string value. Required. For best performance, use an analyzer without token filters.

`enable_position_increments`
:   Indicates if position increments should be counted. Set to `false` if you don’t want to count tokens removed by analyzer filters (like [`stop`](/reference/text-analysis/analysis-stop-tokenfilter.md)). Defaults to `true`.

[`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md)
:   Should the field be stored on disk in a column-stride fashion, so that it can later be used for sorting, aggregations, or scripting? Accepts `true` (default) or `false`.

[`index`](/reference/elasticsearch/mapping-reference/mapping-index.md)
:   Should the field be searchable? Accepts `true` (default) and `false`.

[`null_value`](/reference/elasticsearch/mapping-reference/null-value.md)
:   Accepts a numeric value of the same `type` as the field which is substituted for any explicit `null` values. Defaults to `null`, which means the field is treated as missing.

[`store`](/reference/elasticsearch/mapping-reference/mapping-store.md)
:   Whether the field value should be stored and retrievable separately from the [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) field. Accepts `true` or `false` (default).

### Synthetic `_source` [token-count-synthetic-source]

::::{important}
Synthetic `_source` is Generally Available only for TSDB indices (indices that have `index.mode` set to `time_series`). For other indices synthetic `_source` is in technical preview. Features in technical preview may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


`token_count` fields support [synthetic `_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md#synthetic-source) in their default configuration.



