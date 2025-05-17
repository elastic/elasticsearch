---
navigation_title: "Date"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html
---

# Date field type [date]


JSON doesn’t have a date data type, so dates in Elasticsearch can either be:

* strings containing formatted dates, e.g. `"2015-01-01"` or `"2015/01/01 12:10:30"`.
* a number representing *milliseconds-since-the-epoch*.
* a number representing *seconds-since-the-epoch* ([configuration](#date-epoch-seconds)).

Internally, dates are converted to UTC (if the time-zone is specified) and stored as a long number representing milliseconds-since-the-epoch.

::::{note}
Use the [date_nanos](/reference/elasticsearch/mapping-reference/date_nanos.md) field type if a nanosecond resolution is expected.
::::


Queries on dates are internally converted to range queries on this long representation, and the result of aggregations and stored fields is converted back to a string depending on the date format that is associated with the field.

::::{note}
Dates will always be rendered as strings, even if they were initially supplied as a long in the JSON document.
::::


Date formats can be customised, but if no `format` is specified then it uses the default:

```js
    "strict_date_optional_time||epoch_millis"
```
% NOTCONSOLE

This means that it will accept dates with optional timestamps, which conform to the formats supported by [`strict_date_optional_time`](/reference/elasticsearch/mapping-reference/mapping-date-format.md#strict-date-time) or milliseconds-since-the-epoch.

For instance:

$$$date-example$$$

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "date": {
        "type": "date" <1>
      }
    }
  }
}

PUT my-index-000001/_doc/1
{ "date": "2015-01-01" } <2>

PUT my-index-000001/_doc/2
{ "date": "2015-01-01T12:10:30Z" } <3>

PUT my-index-000001/_doc/3
{ "date": 1420070400001 } <4>

GET my-index-000001/_search
{
  "sort": { "date": "asc"} <5>
}
```

1. The `date` field uses the default `format`.
2. This document uses a plain date.
3. This document includes a time.
4. This document uses milliseconds-since-the-epoch.
5. Note that the `sort` values that are returned are all in milliseconds-since-the-epoch.


::::{warning}
Dates will accept numbers with a decimal point like `{"date": 1618249875.123456}` but there are some cases ({{es-issue}}70085[#70085]) where we’ll lose precision on those dates so they should be avoided.

::::


## Multiple date formats [multiple-date-formats]

Multiple formats can be specified by separating them with `||` as a separator. Each format will be tried in turn until a matching format is found. The first format will be used to convert the *milliseconds-since-the-epoch* value back into a string.

$$$date-format-example$$$

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "date": {
        "type":   "date",
        "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
      }
    }
  }
}
```


## Parameters for `date` fields [date-params]

The following parameters are accepted by `date` fields:

[`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md)
:   Should the field be stored on disk in a column-stride fashion, so that it can later be used for sorting, aggregations, or scripting? Accepts `true` (default) or `false`.

[`format`](/reference/elasticsearch/mapping-reference/mapping-date-format.md)
:   The date format(s) that can be parsed. Defaults to `strict_date_optional_time||epoch_millis`.

`locale`
:   The locale to use when parsing dates since months do not have the same names and/or abbreviations in all languages. The default is ENGLISH.

[`ignore_malformed`](/reference/elasticsearch/mapping-reference/ignore-malformed.md)
:   If `true`, malformed numbers are ignored. If `false` (default), malformed numbers throw an exception and reject the whole document. Note that this cannot be set if the `script` parameter is used.

[`index`](/reference/elasticsearch/mapping-reference/mapping-index.md)
:   Should the field be quickly searchable? Accepts `true` (default) and `false`. Date fields that only have [`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md) enabled can also be queried, albeit slower.

[`null_value`](/reference/elasticsearch/mapping-reference/null-value.md)
:   Accepts a date value in one of the configured `format’s as the field which is substituted for any explicit `null` values. Defaults to `null`, which means the field is treated as missing.  Note that this cannot be set of the `script` parameter is used.

`on_script_error`
:   Defines what to do if the script defined by the `script` parameter throws an error at indexing time. Accepts `fail` (default), which will cause the entire document to be rejected, and `continue`, which will register the field in the document’s [`_ignored`](/reference/elasticsearch/mapping-reference/mapping-ignored-field.md) metadata field and continue indexing. This parameter can only be set if the `script` field is also set.

`script`
:   If this parameter is set, then the field will index values generated by this script, rather than reading the values directly from the source. If a value is set for this field on the input document, then the document will be rejected with an error. Scripts are in the same format as their [runtime equivalent](docs-content://manage-data/data-store/mapping/map-runtime-field.md), and should emit long-valued timestamps.

[`store`](/reference/elasticsearch/mapping-reference/mapping-store.md)
:   Whether the field value should be stored and retrievable separately from the [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) field. Accepts `true` or `false` (default).

[`meta`](/reference/elasticsearch/mapping-reference/mapping-field-meta.md)
:   Metadata about the field.


## Epoch seconds [date-epoch-seconds]

If you need to send dates as *seconds-since-the-epoch* then make sure the `format` lists `epoch_second`:

$$$date-epoch-seconds-example$$$

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "date": {
        "type":   "date",
        "format": "strict_date_optional_time||epoch_second"
      }
    }
  }
}

PUT my-index-000001/_doc/example?refresh
{ "date": 1618321898 }

POST my-index-000001/_search
{
  "fields": [ {"field": "date"}],
  "_source": false
}
```
% TEST[s/_search/_search\?filter_path=hits.hits/]

Which will reply with a date like:

```console-result
{
  "hits": {
    "hits": [
      {
        "_id": "example",
        "_index": "my-index-000001",
        "_score": 1.0,
        "fields": {
          "date": ["2021-04-13T13:51:38.000Z"]
        }
      }
    ]
  }
}
```


## Synthetic `_source` [date-synthetic-source]

::::{important}
Synthetic `_source` is Generally Available only for TSDB indices (indices that have `index.mode` set to `time_series`). For other indices synthetic `_source` is in technical preview. Features in technical preview may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


Synthetic source may sort `date` field values. For example:

$$$synthetic-source-date-example$$$

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
      "date": { "type": "date" }
    }
  }
}
PUT idx/_doc/1
{
  "date": ["2015-01-01T12:10:30Z", "2014-01-01T12:10:30Z"]
}
```
% TEST[s/$/\nGET idx\/_doc\/1?filter_path=_source\n/]

Will become:

```console-result
{
  "date": ["2014-01-01T12:10:30.000Z", "2015-01-01T12:10:30.000Z"]
}
```
% TEST[s/^/{"_source":/ s/\n$/}/]


