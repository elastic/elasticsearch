---
navigation_title: "Numeric"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/number.html
---

# Numeric field types [number]


The following numeric types are supported:

`long`
:   A signed 64-bit integer with a minimum value of `-2^63` and a maximum value of `2^63 - 1`.

`integer`
:   A signed 32-bit integer with a minimum value of `-2^31` and a maximum value of `2^31 - 1`.

`short`
:   A signed 16-bit integer with a minimum value of `-32,768` and a maximum value of `32,767`.

`byte`
:   A signed 8-bit integer with a minimum value of `-128` and a maximum value of `127`.

`double`
:   A double-precision 64-bit IEEE 754 floating point number, restricted to finite values.

`float`
:   A single-precision 32-bit IEEE 754 floating point number, restricted to finite values.

`half_float`
:   A half-precision 16-bit IEEE 754 floating point number, restricted to finite values.

`scaled_float`
:   A floating point number that is backed by a `long`, scaled by a fixed `double` scaling factor.

`unsigned_long`
:   An unsigned 64-bit integer with a minimum value of 0 and a maximum value of `2^64 - 1`.

Below is an example of configuring a mapping with numeric fields:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "number_of_bytes": {
        "type": "integer"
      },
      "time_in_seconds": {
        "type": "float"
      },
      "price": {
        "type": "scaled_float",
        "scaling_factor": 100
      }
    }
  }
}
```

::::{note}
The `double`, `float` and `half_float` types consider that `-0.0` and `+0.0` are different values. As a consequence, doing a `term` query on `-0.0` will not match `+0.0` and vice-versa. Same is true for range queries: if the upper bound is `-0.0` then `+0.0` will not match, and if the lower bound is `+0.0` then `-0.0` will not match.
::::


## Which type should I use? [_which_type_should_i_use]

As far as integer types (`byte`, `short`, `integer` and `long`) are concerned, you should pick the smallest type which is enough for your use-case. This will help indexing and searching be more efficient. Note however that storage is optimized based on the actual values that are stored, so picking one type over another one will have no impact on storage requirements.

For floating-point types, it is often more efficient to store floating-point data into an integer using a scaling factor, which is what the `scaled_float` type does under the hood. For instance, a `price` field could be stored in a `scaled_float` with a `scaling_factor` of `100`. All APIs would work as if the field was stored as a double, but under the hood Elasticsearch would be working with the number of cents, `price*100`, which is an integer. This is mostly helpful to save disk space since integers are way easier to compress than floating points. `scaled_float` is also fine to use in order to trade accuracy for disk space. For instance imagine that you are tracking cpu utilization as a number between `0` and `1`. It usually does not matter much whether cpu utilization is `12.7%` or `13%`, so you could use a `scaled_float` with a `scaling_factor` of `100` in order to round cpu utilization to the closest percent in order to save space.

If `scaled_float` is not a good fit, then you should pick the smallest type that is enough for the use-case among the floating-point types: `double`, `float` and `half_float`. Here is a table that compares these types in order to help make a decision.

$$$floating_point$$$

| Type | Minimum value | Maximum value | Significant<br>                                                      bits / digits | Example precision loss |
| --- | --- | --- | --- | --- |
| `double` | `2^-1074` | `(2 - 2^-52) * 2^1023` | `53 / 15.95` | `1.2345678912345678`→<br>`1.234567891234568` |
| `float` | `2^-149` | `(2 - 2^-23) * 2^127` | `24 / 7.22` | `1.23456789`→<br>`1.2345679` |
| `half_float` | `2^-24` | `65504` | `11 / 3.31` | `1.2345`→<br>`1.234375` |

::::{admonition} Mapping numeric identifiers
:class: tip

Not all numeric data should be mapped as a `numeric` field data type. {{es}} optimizes numeric fields, such as `integer` or `long`, for [`range`](/reference/query-languages/query-dsl/query-dsl-range-query.md) queries. However, [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) fields are better for [`term`](/reference/query-languages/query-dsl/query-dsl-term-query.md) and other [term-level](/reference/query-languages/query-dsl/term-level-queries.md) queries.

Identifiers, such as an ISBN or a product ID, are rarely used in `range` queries. However, they are often retrieved using term-level queries.

Consider mapping a numeric identifier as a `keyword` if:

* You don’t plan to search for the identifier data using [`range`](/reference/query-languages/query-dsl/query-dsl-range-query.md) queries.
* Fast retrieval is important. `term` query searches on `keyword` fields are often faster than `term` searches on numeric fields.

If you’re unsure which to use, you can use a [multi-field](/reference/elasticsearch/mapping-reference/multi-fields.md) to map the data as both a `keyword` *and* a numeric data type.

::::



## Parameters for numeric fields [number-params]

The following parameters are accepted by numeric types:

[`coerce`](/reference/elasticsearch/mapping-reference/coerce.md)
:   Try to convert strings to numbers and truncate fractions for integers. Accepts `true` (default) and `false`. Not applicable for `unsigned_long`. Note that this cannot be set if the `script` parameter is used.

[`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md)
:   Should the field be stored on disk in a column-stride fashion, so that it can later be used for sorting, aggregations, or scripting? Accepts `true` (default) or `false`.

[`ignore_malformed`](/reference/elasticsearch/mapping-reference/ignore-malformed.md)
:   If `true`, malformed numbers are ignored. If `false` (default), malformed numbers throw an exception and reject the whole document.  Note that this cannot be set if the `script` parameter is used.

[`index`](/reference/elasticsearch/mapping-reference/mapping-index.md)
:   Should the field be quickly searchable? Accepts `true` (default) and `false`. Numeric fields that only have [`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md) enabled can also be queried, albeit slower.

[`meta`](/reference/elasticsearch/mapping-reference/mapping-field-meta.md)
:   Metadata about the field.

[`null_value`](/reference/elasticsearch/mapping-reference/null-value.md)
:   Accepts a numeric value of the same `type` as the field which is substituted for any explicit `null` values. Defaults to `null`, which means the field is treated as missing. Note that this cannot be set if the `script` parameter is used.

`on_script_error`
:   Defines what to do if the script defined by the `script` parameter throws an error at indexing time. Accepts `fail` (default), which will cause the entire document to be rejected, and `continue`, which will register the field in the document’s [`_ignored`](/reference/elasticsearch/mapping-reference/mapping-ignored-field.md) metadata field and continue indexing. This parameter can only be set if the `script` field is also set.

`script`
:   If this parameter is set, then the field will index values generated by this script, rather than reading the values directly from the source. If a value is set for this field on the input document, then the document will be rejected with an error. Scripts are in the same format as their [runtime equivalent](docs-content://manage-data/data-store/mapping/map-runtime-field.md). Scripts can only be configured on `long` and `double` field types.

[`store`](/reference/elasticsearch/mapping-reference/mapping-store.md)
:   Whether the field value should be stored and retrievable separately from the [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) field. Accepts `true` or `false` (default).

`time_series_dimension`
:   (Optional, Boolean)

    Marks the field as a [time series dimension](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-series-dimension). Defaults to `false`.

    The `index.mapping.dimension_fields.limit` [index setting](/reference/elasticsearch/index-settings/time-series.md) limits the number of dimensions in an index.

    Dimension fields have the following constraints:

    * The `doc_values` and `index` mapping parameters must be `true`.

    Of the numeric field types, only `byte`, `short`, `integer`, `long`, and `unsigned_long` fields support this parameter.

    A numeric field can’t be both a time series dimension and a time series metric.


`time_series_metric`
:   (Optional, string) Marks the field as a [time series metric](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-series-metric). The value is the metric type. You can’t update this parameter for existing fields.

    **Valid `time_series_metric` values for numeric fields**:

    `counter`
    :   A cumulative metric that only monotonically increases or resets to `0` (zero). For example, a count of errors or completed tasks.

    `gauge`
    :   A metric that represents a single numeric that can arbitrarily increase or decrease. For example, a temperature or available disk space.

    `null` (Default)
    :   Not a time series metric.

    For a numeric time series metric, the `doc_values` parameter must be `true`. A numeric field can’t be both a time series dimension and a time series metric.



## Parameters for `scaled_float` [scaled-float-params]

`scaled_float` accepts an additional parameter:

`scaling_factor`
:   The scaling factor to use when encoding values. Values will be multiplied by this factor at index time and rounded to the closest long value. For instance, a `scaled_float` with a `scaling_factor` of `10` would internally store `2.34` as `23` and all search-time operations (queries, aggregations, sorting) will behave as if the document had a value of `2.3`. High values of `scaling_factor` improve accuracy but also increase space requirements. This parameter is required.


## `scaled_float` saturation [scaled-float-saturation]

`scaled_float` is stored as a single `long` value, which is the product of multiplying the original value by the scaling factor. If the multiplication results in a value that is outside the range of a `long`, the value is saturated to the minimum or maximum value of a `long`. For example, if the scaling factor is `100` and the value is `92233720368547758.08`, the expected value is `9223372036854775808`. However, the value that is stored is `9223372036854775807`, the maximum value for a `long`.

This can lead to unexpected results with [range queries](/reference/query-languages/query-dsl/query-dsl-range-query.md) when the scaling factor or provided `float` value are exceptionally large.


## Synthetic `_source` [numeric-synthetic-source]

::::{important}
Synthetic `_source` is Generally Available only for TSDB indices (indices that have `index.mode` set to `time_series`). For other indices synthetic `_source` is in technical preview. Features in technical preview may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


All numeric fields support [synthetic `_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md#synthetic-source) in their default configuration. Synthetic `_source` cannot be used together with [`copy_to`](/reference/elasticsearch/mapping-reference/copy-to.md), or with [`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md) disabled.

Synthetic source may sort numeric field values. For example:

$$$synthetic-source-numeric-example$$$

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
      "long": { "type": "long" }
    }
  }
}
PUT idx/_doc/1
{
  "long": [0, 0, -123466, 87612]
}
```
% TEST[s/$/\nGET idx\/_doc\/1?filter_path=_source\n/]

Will become:

```console-result
{
  "long": [-123466, 0, 0, 87612]
}
```
% TEST[s/^/{"_source":/ s/\n$/}/]

Scaled floats will always apply their scaling factor so:

$$$synthetic-source-scaled-float-example$$$

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
      "f": { "type": "scaled_float", "scaling_factor": 0.01 }
    }
  }
}
PUT idx/_doc/1
{
  "f": 123
}
```
% TEST[s/$/\nGET idx\/_doc\/1?filter_path=_source\n/]

Will become:

```console-result
{
  "f": 100.0
}
```
% TEST[s/^/{"_source":/ s/\n$/}/]


