---
navigation_title: "Date nanoseconds"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/date_nanos.html
---

# Date nanoseconds field type [date_nanos]


This data type is an addition to the `date` data type. However there is an important distinction between the two. The existing `date` data type stores dates in millisecond resolution. The `date_nanos` data type stores dates in nanosecond resolution, which limits its range of dates from roughly 1970 to 2262, as dates are still stored as a long representing nanoseconds since the epoch.

Queries on nanoseconds are internally converted to range queries on this long representation, and the result of aggregations and stored fields is converted back to a string depending on the date format that is associated with the field.

Date formats can be customised, but if no `format` is specified then it uses the default:

```js
    "strict_date_optional_time_nanos||epoch_millis"
```
% NOTCONSOLE

For instance:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "date": {
        "type": "date_nanos" <1>
      }
    }
  }
}

PUT my-index-000001/_bulk?refresh
{ "index" : { "_id" : "1" } }
{ "date": "2015-01-01" } <2>
{ "index" : { "_id" : "2" } }
{ "date": "2015-01-01T12:10:30.123456789Z" } <3>
{ "index" : { "_id" : "3" } }
{ "date": 1420070400000 } <4>

GET my-index-000001/_search
{
  "sort": { "date": "asc"}, <5>
  "runtime_mappings": {
    "date_has_nanos": {
      "type": "boolean",
      "script": "emit(doc['date'].value.nano != 0)" <6>
    }
  },
  "fields": [
    {
      "field": "date",
      "format": "strict_date_optional_time_nanos" <7>
    },
    {
      "field": "date_has_nanos"
    }
  ]
}
```
% TEST[s/_search/_search\?filter_path=hits.hits/]

1. The `date` field uses the default `format`.
2. This document uses a plain date.
3. This document includes a time.
4. This document uses milliseconds-since-the-epoch.
5. Note that the `sort` values that are returned are all in nanoseconds-since-the-epoch.
6. Use `.nano` in scripts to return the nanosecond component of the date.
7. You can specify the format when fetching data using the [`fields` parameter](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#search-fields-param). Use [`strict_date_optional_time_nanos`](/reference/elasticsearch/mapping-reference/mapping-date-format.md#strict-date-time-nanos) or you’ll get a rounded result.


You can also specify multiple date formats separated by `||`. The same mapping parameters than with the `date` field can be used.

::::{warning}
Date nanoseconds will accept numbers with a decimal point like `{"date": 1618249875.123456}` but there are some cases ({{es-issue}}70085[#70085]) where we’ll lose precision on those dates so they should be avoided.

::::


## Limitations [date-nanos-limitations]

Aggregations are still on millisecond resolution, even when using a `date_nanos` field. This limitation also affects [{{transforms}}](docs-content://explore-analyze/transforms.md).

<hr>

## Synthetic `_source` [date-nanos-synthetic-source]

::::{important}
Synthetic `_source` is Generally Available only for TSDB indices (indices that have `index.mode` set to `time_series`). For other indices synthetic `_source` is in technical preview. Features in technical preview may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


Synthetic source may sort `date_nanos` field values. For example:

$$$synthetic-source-date-nanos-example$$$

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
      "date": { "type": "date_nanos" }
    }
  }
}
PUT idx/_doc/1
{
  "date": ["2015-01-01T12:10:30.000Z", "2014-01-01T12:10:30.000Z"]
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


