---
navigation_title: "Text"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/text.html
---

# Text type family [text]


The text family includes the following field types:

* [`text`](#text-field-type), the traditional field type for full-text content such as the body of an email or the description of a product.
* [`match_only_text`](#match-only-text-field-type), a space-optimized variant of `text` that disables scoring and performs slower on queries that need positions. It is best suited for indexing log messages.


## Text field type [text-field-type]

A field to index full-text values, such as the body of an email or the description of a product. These fields are `analyzed`, that is they are passed through an [analyzer](docs-content://manage-data/data-store/text-analysis.md) to convert the string into a list of individual terms before being indexed. The analysis process allows Elasticsearch to search for individual words *within* each full text field. Text fields are not used for sorting and seldom used for aggregations (although the [significant text aggregation](/reference/aggregations/search-aggregations-bucket-significanttext-aggregation.md) is a notable exception).

`text` fields are best suited for unstructured but human-readable content. If you need to index unstructured machine-generated content, see [Mapping unstructured content](/reference/elasticsearch/mapping-reference/keyword.md#mapping-unstructured-content).

If you need to index structured content such as email addresses, hostnames, status codes, or tags, it is likely that you should rather use a [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) field.

Below is an example of a mapping for a text field:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "full_name": {
        "type":  "text"
      }
    }
  }
}
```

## Use a field as both text and keyword [text-multi-fields]

Sometimes it is useful to have both a full text (`text`) and a keyword (`keyword`) version of the same field: one for full text search and the other for aggregations and sorting. This can be achieved with [multi-fields](/reference/elasticsearch/mapping-reference/multi-fields.md).


## Parameters for text fields [text-params]

The following parameters are accepted by `text` fields:

[`analyzer`](/reference/elasticsearch/mapping-reference/analyzer.md)
:   The [analyzer](docs-content://manage-data/data-store/text-analysis.md) which should be used for the `text` field, both at index-time and at search-time (unless overridden by the  [`search_analyzer`](/reference/elasticsearch/mapping-reference/search-analyzer.md)). Defaults to the default index analyzer, or the [`standard` analyzer](/reference/text-analysis/analysis-standard-analyzer.md).

[`eager_global_ordinals`](/reference/elasticsearch/mapping-reference/eager-global-ordinals.md)
:   Should global ordinals be loaded eagerly on refresh? Accepts `true` or `false` (default). Enabling this is a good idea on fields that are frequently used for (significant) terms aggregations.

[`fielddata`](#fielddata-mapping-param)
:   Can the field use in-memory fielddata for sorting, aggregations, or scripting? Accepts `true` or `false` (default).

[`fielddata_frequency_filter`](#field-data-filtering)
:   Expert settings which allow to decide which values to load in memory when `fielddata` is enabled. By default all values are loaded.

[`fields`](/reference/elasticsearch/mapping-reference/multi-fields.md)
:   Multi-fields allow the same string value to be indexed in multiple ways for different purposes, such as one field for search and a multi-field for sorting and aggregations, or the same string value analyzed by different analyzers.

[`index`](/reference/elasticsearch/mapping-reference/mapping-index.md)
:   Should the field be searchable? Accepts `true` (default) or `false`.

[`index_options`](/reference/elasticsearch/mapping-reference/index-options.md)
:   What information should be stored in the index, for search and highlighting purposes. Defaults to `positions`.

[`index_prefixes`](/reference/elasticsearch/mapping-reference/index-prefixes.md)
:   If enabled, term prefixes of between 2 and 5 characters are indexed into a separate field. This allows prefix searches to run more efficiently, at the expense of a larger index.

[`index_phrases`](/reference/elasticsearch/mapping-reference/index-phrases.md)
:   If enabled, two-term word combinations (*shingles*) are indexed into a separate field. This allows exact phrase queries (no slop) to run more efficiently, at the expense of a larger index. Note that this works best when stopwords are not removed, as phrases containing stopwords will not use the subsidiary field and will fall back to a standard phrase query. Accepts `true` or `false` (default).

[`norms`](/reference/elasticsearch/mapping-reference/norms.md)
:   Whether field-length should be taken into account when scoring queries. Accepts `true` (default) or `false`.

[`position_increment_gap`](/reference/elasticsearch/mapping-reference/position-increment-gap.md)
:   The number of fake term position which should be inserted between each element of an array of strings. Defaults to the `position_increment_gap` configured on the analyzer which defaults to `100`. `100` was chosen because it prevents phrase queries with reasonably large slops (less than 100) from matching terms across field values.

[`store`](/reference/elasticsearch/mapping-reference/mapping-store.md)
:   Whether the field value should be stored and retrievable separately from the [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) field. Accepts `true` or `false` (default).

[`search_analyzer`](/reference/elasticsearch/mapping-reference/search-analyzer.md)
:   The [`analyzer`](/reference/elasticsearch/mapping-reference/analyzer.md) that should be used at search time on the `text` field. Defaults to the `analyzer` setting.

[`search_quote_analyzer`](/reference/elasticsearch/mapping-reference/analyzer.md#search-quote-analyzer)
:   The [`analyzer`](/reference/elasticsearch/mapping-reference/analyzer.md) that should be used at search time when a phrase is encountered. Defaults to the `search_analyzer` setting.

[`similarity`](/reference/elasticsearch/mapping-reference/similarity.md)
:   Which scoring algorithm or *similarity* should be used. Defaults to `BM25`.

[`term_vector`](/reference/elasticsearch/mapping-reference/term-vector.md)
:   Whether term vectors should be stored for the field. Defaults to `no`.

[`meta`](/reference/elasticsearch/mapping-reference/mapping-field-meta.md)
:   Metadata about the field.


## Synthetic `_source` [text-synthetic-source]

::::{important}
Synthetic `_source` is Generally Available only for TSDB indices (indices that have `index.mode` set to `time_series`). For other indices synthetic `_source` is in technical preview. Features in technical preview may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


`text` fields can use a [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md#keyword-synthetic-source) sub-field to support [synthetic `_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md#synthetic-source) without storing values of the text field itself.

In this case, the synthetic source of the `text` field will have the same [modifications](/reference/elasticsearch/mapping-reference/mapping-source-field.md#synthetic-source) as a `keyword` field.

These modifications can impact usage of `text` fields:
* Reordering text fields can have an effect on [phrase](/reference/query-languages/query-dsl/query-dsl-match-query-phrase.md) and [span](/reference/query-languages/query-dsl/span-queries.md) queries. See the discussion about [`position_increment_gap`](/reference/elasticsearch/mapping-reference/position-increment-gap.md) for more details. You can avoid this by making sure the `slop` parameter on the phrase queries is lower than the `position_increment_gap`. This is the default.
* Handling of `null` values is different. `text` fields ignore `null` values, but `keyword` fields support replacing `null` values with a value specified in the `null_value` parameter. This replacement is represented in synthetic source.

For example:

$$$synthetic-source-text-example-multi-field$$$

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
      "text": {
        "type": "text",
        "fields": {
          "kwd": {
            "type": "keyword",
            "null_value": "NA"
          }
        }
      }
    }
  }
}
PUT idx/_doc/1
{
  "text": [
    null,
    "the quick brown fox",
    "the quick brown fox",
    "jumped over the lazy dog"
  ]
}
```
% TEST[s/$/\nGET idx\/_doc\/1?filter_path=_source\n/]

Will become:

```console-result
{
  "text": [
    "NA",
    "jumped over the lazy dog",
    "the quick brown fox"
  ]
}
```
% TEST[s/^/{"_source":/ s/\n$/}/]


If the `text` field sets `store` to `true` then the sub-field is not used and no modifications are applied. For example:

$$$synthetic-source-text-example-stored$$$

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
      "text": {
        "type": "text",
        "store": true,
        "fields": {
          "raw": {
            "type": "keyword"
          }
        }
      }
    }
  }
}
PUT idx/_doc/1
{
  "text": [
    "the quick brown fox",
    "the quick brown fox",
    "jumped over the lazy dog"
  ]
}
```
% TEST[s/$/\nGET idx\/_doc\/1?filter_path=_source\n/]

Will become:

```console-result
{
  "text": [
    "the quick brown fox",
    "the quick brown fox",
    "jumped over the lazy dog"
  ]
}
```
% TEST[s/^/{"_source":/ s/\n$/}/]


## `fielddata` mapping parameter [fielddata-mapping-param]

`text` fields are searchable by default, but by default are not available for aggregations, sorting, or scripting. If you try to sort, aggregate, or access values from a `text` field using a script, you’ll see an exception indicating that field data is disabled by default on text fields. To load field data in memory, set `fielddata=true` on your field.

::::{note}
Loading field data in memory can consume significant memory.
::::


Field data is the only way to access the analyzed tokens from a full text field in aggregations, sorting, or scripting. For example, a full text field like `New York` would get analyzed as `new` and `york`. To aggregate on these tokens requires field data.


## Before enabling fielddata [before-enabling-fielddata]

It usually doesn’t make sense to enable fielddata on text fields. Field data is stored in the heap with the [field data cache](/reference/elasticsearch/configuration-reference/field-data-cache-settings.md) because it is expensive to calculate. Calculating the field data can cause latency spikes, and increasing heap usage is a cause of cluster performance issues.

Most users who want to do more with text fields use [multi-field mappings](/reference/elasticsearch/mapping-reference/multi-fields.md) by having both a `text` field for full text searches, and an unanalyzed [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) field for aggregations, as follows:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "my_field": { <1>
        "type": "text",
        "fields": {
          "keyword": { <2>
            "type": "keyword"
          }
        }
      }
    }
  }
}
```

1. Use the `my_field` field for searches.
2. Use the `my_field.keyword` field for aggregations, sorting, or in scripts.



## Enabling fielddata on `text` fields [enable-fielddata-text-fields]

You can enable fielddata on an existing `text` field using the [update mapping API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping) as follows:

```console
PUT my-index-000001/_mapping
{
  "properties": {
    "my_field": { <1>
      "type":     "text",
      "fielddata": true
    }
  }
}
```
% TEST[continued]

1. The mapping that you specify for `my_field` should consist of the existing mapping for that field, plus the `fielddata` parameter.



## `fielddata_frequency_filter` mapping parameter [field-data-filtering]

Fielddata filtering can be used to reduce the number of terms loaded into memory, and thus reduce memory usage. Terms can be filtered by *frequency*:

The frequency filter allows you to only load terms whose document frequency falls between a `min` and `max` value, which can be expressed an absolute number (when the number is bigger than 1.0) or as a percentage (eg `0.01` is `1%` and `1.0` is `100%`). Frequency is calculated **per segment**. Percentages are based on the number of docs which have a value for the field, as opposed to all docs in the segment.

Small segments can be excluded completely by specifying the minimum number of docs that the segment should contain with `min_segment_size`:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "tag": {
        "type": "text",
        "fielddata": true,
        "fielddata_frequency_filter": {
          "min": 0.001,
          "max": 0.1,
          "min_segment_size": 500
        }
      }
    }
  }
}
```


## Match-only text field type [match-only-text-field-type]

A variant of [`text`](#text-field-type) that trades scoring and efficiency of positional queries for space efficiency. This field effectively stores data the same way as a `text` field that only indexes documents (`index_options: docs`) and disables norms (`norms: false`). Term queries perform as fast if not faster as on `text` fields, however queries that need positions such as the [`match_phrase` query](/reference/query-languages/query-dsl/query-dsl-match-query-phrase.md) perform slower as they need to look at the `_source` document to verify whether a phrase matches. All queries return constant scores that are equal to 1.0.

Analysis is not configurable: text is always analyzed with the [default analyzer](docs-content://manage-data/data-store/text-analysis/specify-an-analyzer.md#specify-index-time-default-analyzer) ([`standard`](/reference/text-analysis/analysis-standard-analyzer.md) by default).

[span queries](/reference/query-languages/query-dsl/span-queries.md) are not supported with this field, use [interval queries](/reference/query-languages/query-dsl/query-dsl-intervals-query.md) instead, or the [`text`](#text-field-type) field type if you absolutely need span queries.

Other than that, `match_only_text` supports the same queries as `text`. And like `text`, it does not support sorting and has only limited support for aggregations.

```console
PUT logs
{
  "mappings": {
    "properties": {
      "@timestamp": {
        "type": "date"
      },
      "message": {
        "type": "match_only_text"
      }
    }
  }
}
```


### Parameters for match-only text fields [match-only-text-params]

The following mapping parameters are accepted:

[`fields`](/reference/elasticsearch/mapping-reference/multi-fields.md)
:   Multi-fields allow the same string value to be indexed in multiple ways for different purposes, such as one field for search and a multi-field for sorting and aggregations, or the same string value analyzed by different analyzers.

[`meta`](/reference/elasticsearch/mapping-reference/mapping-field-meta.md)
:   Metadata about the field.


