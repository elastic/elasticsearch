---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-source-field.html
---

# _source field [mapping-source-field]

The `_source` field contains the original JSON document body that was passed at index time. The `_source` field itself is not indexed (and thus is not searchable), but it is stored so that it can be returned when executing *fetch* requests, like [get](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-get) or [search](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search).

If disk usage is important to you, then consider the following options:

* Using [synthetic `_source`](#synthetic-source), which reconstructs source content at the time of retrieval instead of storing it on disk. This shrinks disk usage, at the cost of slower access to `_source` in [Get](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-get) and [Search](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search) queries.
* [Disabling the `_source` field completely](#disable-source-field). This shrinks disk usage but disables features that rely on `_source`.

## Synthetic `_source` [synthetic-source]

Though very handy to have around, the source field takes up a significant amount of space on disk. Instead of storing source documents on disk exactly as you send them, Elasticsearch can reconstruct source content on the fly upon retrieval. To enable this [subscription](https://www.elastic.co/subscriptions) feature, use the value `synthetic` for the index setting `index.mapping.source.mode`:

$$$enable-synthetic-source-example$$$

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
  }
}
```
% TESTSETUP

While this on-the-fly reconstruction is *generally* slower than saving the source documents verbatim and loading them at query time, it saves a lot of storage space. Additional latency can be avoided by not loading `_source` field in queries when it is not needed.

### Supported fields [synthetic-source-fields]

Synthetic `_source` is supported by all field types. Depending on implementation details, field types have different properties when used with synthetic `_source`.

[Most field types](#synthetic-source-fields-native-list) construct synthetic `_source` using existing data, most commonly [`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md) and [stored fields](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#stored-fields). For these field types, no additional space is needed to store the contents of `_source` field. Due to the storage layout of [`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md), the generated `_source` field undergoes [modifications](#synthetic-source-modifications) compared to the original document.

For all other field types, the original value of the field is stored as is, in the same way as the `_source` field in non-synthetic mode. In this case there are no modifications and field data in `_source` is the same as in the original document. Similarly, malformed values of fields that use [`ignore_malformed`](/reference/elasticsearch/mapping-reference/ignore-malformed.md) or [`ignore_above`](/reference/elasticsearch/mapping-reference/ignore-above.md) need to be stored as is. This approach is less storage efficient since data needed for `_source` reconstruction is stored in addition to other data required to index the field (like `doc_values`).


### Synthetic `_source` restrictions [synthetic-source-restrictions]

Some field types have additional restrictions. These restrictions are documented in the **synthetic `_source`** section of the field type’s [documentation](/reference/elasticsearch/mapping-reference/field-data-types.md).

Synthetic source is not supported in [source-only](docs-content://deploy-manage/tools/snapshot-and-restore/source-only-repository.md) snapshot repositories. To store indexes that use synthetic `_source`, choose a different repository type.

### Synthetic `_source` modifications [synthetic-source-modifications]

When synthetic `_source` is enabled, retrieved documents undergo some modifications compared to the original JSON.

#### Arrays moved to leaf fields [synthetic-source-modifications-leaf-arrays]

Synthetic `_source` arrays are moved to leaves. For example:

$$$synthetic-source-leaf-arrays-example$$$

```console
PUT idx/_doc/1
{
  "foo": [
    {
      "bar": 1
    },
    {
      "bar": 2
    }
  ]
}
```
% TEST[s/$/\nGET idx\/_doc\/1?filter_path=_source\n/]

Will become:

```console-result
{
  "foo": {
    "bar": [1, 2]
  }
}
```
% TEST[s/^/{"_source":/ s/\n$/}/]

This can cause some arrays to vanish:

$$$synthetic-source-leaf-arrays-example-sneaky$$$

```console
PUT idx/_doc/1
{
  "foo": [
    {
      "bar": 1
    },
    {
      "baz": 2
    }
  ]
}
```
% TEST[s/$/\nGET idx\/_doc\/1?filter_path=_source\n/]

Will become:

```console-result
{
  "foo": {
    "bar": 1,
    "baz": 2
  }
}
```
% TEST[s/^/{"_source":/ s/\n$/}/]


#### Fields named as they are mapped [synthetic-source-modifications-field-names]

Synthetic source names fields as they are named in the mapping. When used with [dynamic mapping](/reference/elasticsearch/mapping-reference/dynamic.md), fields with dots (`.`) in their names are, by default, interpreted as multiple objects, while dots in field names are preserved within objects that have [`subobjects`](/reference/elasticsearch/mapping-reference/subobjects.md) disabled. For example:

$$$synthetic-source-objecty-example$$$

```console
PUT idx/_doc/1
{
  "foo.bar.baz": 1
}
```
% TEST[s/$/\nGET idx\/_doc\/1?filter_path=_source\n/]

Will become:

```console-result
{
  "foo": {
    "bar": {
      "baz": 1
    }
  }
}
```
% TEST[s/^/{"_source":/ s/\n$/}/]

This impacts how source contents can be referenced in [scripts](docs-content://explore-analyze/scripting/modules-scripting-using.md). For instance, referencing a script in its original source form will return null:

```js
"script": { "source": """  emit(params._source['foo.bar.baz'])  """ }
```
% NOTCONSOLE

Instead, source references need to be in line with the mapping structure:

```js
"script": { "source": """  emit(params._source['foo']['bar']['baz'])  """ }
```
% NOTCONSOLE

or simply

```js
"script": { "source": """  emit(params._source.foo.bar.baz)  """ }
```
% NOTCONSOLE

The following [field APIs](docs-content://explore-analyze/scripting/modules-scripting-fields.md) are preferable as, in addition to being agnostic to the mapping structure, they make use of docvalues if available and fall back to synthetic source only when needed. This reduces source synthesizing, a slow and costly operation.

```js
"script": { "source": """  emit(field('foo.bar.baz').get(null))   """ }
"script": { "source": """  emit($('foo.bar.baz', null))   """ }
```
% NOTCONSOLE


#### Alphabetical sorting [synthetic-source-modifications-alphabetical]

Synthetic `_source` fields are sorted alphabetically. The [JSON RFC](https://www.rfc-editor.org/rfc/rfc7159.md) defines objects as "an unordered collection of zero or more name/value pairs" so applications shouldn’t care but without synthetic `_source` the original ordering is preserved and some applications may, counter to the spec, do something with that ordering.


#### Representation of ranges [synthetic-source-modifications-ranges]

Range field values (e.g. `long_range`) are always represented as inclusive on both sides with bounds adjusted accordingly. See [examples](/reference/elasticsearch/mapping-reference/range.md#range-synthetic-source-inclusive).


#### Reduced precision of `geo_point` values [synthetic-source-precision-loss-for-point-types]

Values of `geo_point` fields are represented in synthetic `_source` with reduced precision. See [examples](/reference/elasticsearch/mapping-reference/geo-point.md#geo-point-synthetic-source).


#### Minimizing source modifications [synthetic-source-keep]

It is possible to avoid synthetic source modifications for a particular object or field, at extra storage cost. This is controlled through param `synthetic_source_keep` with the following option:

* `none`: synthetic source diverges from the original source as described above (default).
* `arrays`: arrays of the corresponding field or object preserve the original element ordering and duplicate elements. The synthetic source fragment for such arrays is not guaranteed to match the original source exactly, e.g. array `[1, 2, [5], [[4, [3]]], 5]` may appear as-is or in an equivalent format like `[1, 2, 5, 4, 3, 5]`. The exact format may change in the future, in an effort to reduce the storage overhead of this option.
* `all`: the source for both singleton instances and arrays of the corresponding field or object gets recorded. When applied to objects, the source of all sub-objects and sub-fields gets captured. Furthermore, the original source of arrays gets captured and appears in synthetic source with no modifications.

For instance:

$$$create-index-with-synthetic-source-keep$$$

```console
PUT idx_keep
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
      "path": {
        "type": "object",
        "synthetic_source_keep": "all"
      },
      "ids": {
        "type": "integer",
        "synthetic_source_keep": "arrays"
      }
    }
  }
}
```
% TEST

$$$synthetic-source-keep-example$$$

```console
PUT idx_keep/_doc/1
{
  "path": {
    "to": [
      { "foo": [3, 2, 1] },
      { "foo": [30, 20, 10] }
    ],
    "bar": "baz"
  },
  "ids": [ 200, 100, 300, 100 ]
}
```
% TEST[s/$/\nGET idx_keep/_doc/1\?filter_path=_source\n/]

returns the original source, with no array deduplication and sorting:

```console-result
{
  "path": {
    "to": [
      { "foo": [3, 2, 1] },
      { "foo": [30, 20, 10] }
    ],
    "bar": "baz"
  },
  "ids": [ 200, 100, 300, 100 ]
}
```
% TEST[s/^/{"_source":/ s/\n$/}/]

The option for capturing the source of arrays can be applied at index level, by setting `index.mapping.synthetic_source_keep` to `arrays`. This applies to all objects and fields in the index, except for the ones with explicit overrides of `synthetic_source_keep` set to `none`. In this case, the storage overhead grows with the number and sizes of arrays present in source of each document, naturally.



### Field types that support synthetic source with no storage overhead [synthetic-source-fields-native-list]

The following field types support synthetic source using data from [`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md) or [stored fields](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#stored-fields), and require no additional storage space to construct the `_source` field.

::::{note}
If you enable the [`ignore_malformed`](/reference/elasticsearch/mapping-reference/ignore-malformed.md) or [`ignore_above`](/reference/elasticsearch/mapping-reference/ignore-above.md) settings, then additional storage is required to store ignored field values for these types.
::::


* [`aggregate_metric_double`](/reference/elasticsearch/mapping-reference/aggregate-metric-double.md#aggregate-metric-double-synthetic-source)
* [`annotated-text`](/reference/elasticsearch-plugins/mapper-annotated-text-usage.md#annotated-text-synthetic-source)
* [`binary`](/reference/elasticsearch/mapping-reference/binary.md#binary-synthetic-source)
* [`boolean`](/reference/elasticsearch/mapping-reference/boolean.md#boolean-synthetic-source)
* [`byte`](/reference/elasticsearch/mapping-reference/number.md#numeric-synthetic-source)
* [`date`](/reference/elasticsearch/mapping-reference/date.md#date-synthetic-source)
* [`date_nanos`](/reference/elasticsearch/mapping-reference/date_nanos.md#date-nanos-synthetic-source)
* [`dense_vector`](/reference/elasticsearch/mapping-reference/dense-vector.md#dense-vector-synthetic-source)
* [`double`](/reference/elasticsearch/mapping-reference/number.md#numeric-synthetic-source)
* [`flattened`](/reference/elasticsearch/mapping-reference/flattened.md#flattened-synthetic-source)
* [`float`](/reference/elasticsearch/mapping-reference/number.md#numeric-synthetic-source)
* [`geo_point`](/reference/elasticsearch/mapping-reference/geo-point.md#geo-point-synthetic-source)
* [`half_float`](/reference/elasticsearch/mapping-reference/number.md#numeric-synthetic-source)
* [`histogram`](/reference/elasticsearch/mapping-reference/histogram.md)
* [`integer`](/reference/elasticsearch/mapping-reference/number.md#numeric-synthetic-source)
* [`ip`](/reference/elasticsearch/mapping-reference/ip.md#ip-synthetic-source)
* [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md#keyword-synthetic-source)
* [`long`](/reference/elasticsearch/mapping-reference/number.md#numeric-synthetic-source)
* [`range` types](/reference/elasticsearch/mapping-reference/range.md#range-synthetic-source)
* [`scaled_float`](/reference/elasticsearch/mapping-reference/number.md#numeric-synthetic-source)
* [`short`](/reference/elasticsearch/mapping-reference/number.md#numeric-synthetic-source)
* [`text`](/reference/elasticsearch/mapping-reference/text.md#text-synthetic-source)
* [`version`](/reference/elasticsearch/mapping-reference/version.md#version-synthetic-source)
* [`wildcard`](/reference/elasticsearch/mapping-reference/keyword.md#wildcard-synthetic-source)



## Disabling the `_source` field [disable-source-field]

Though very handy to have around, the source field does incur storage overhead within the index. For this reason, it can be disabled as follows:

```console
PUT my-index-000001
{
  "mappings": {
    "_source": {
      "enabled": false
    }
  }
}
```

::::{admonition} Think before disabling the _source field
:class: warning

Users often disable the `_source` field without thinking about the consequences, and then live to regret it. If the `_source` field isn’t available then a number of features are not supported:

* The [`update`](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update), [`update_by_query`](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update-by-query), and [`reindex`](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-reindex) APIs.
* In the {{kib}} [Discover](docs-content://explore-analyze/discover.md) application, field data will not be displayed.
* On the fly [highlighting](/reference/elasticsearch/rest-apis/highlighting.md).
* The ability to reindex from one Elasticsearch index to another, either to change mappings or analysis, or to upgrade an index to a new major version.
* The ability to debug queries or aggregations by viewing the original document used at index time.
* Potentially in the future, the ability to repair index corruption automatically.

::::

::::{note}
You can't disable the `_source` field for indexes with [`index_mode`](/reference/elasticsearch/index-settings/index-modules.md#index-mode-setting) set to `logsdb` or `time_series`.
::::


::::{tip}
If disk space is a concern, rather increase the [compression level](/reference/elasticsearch/index-settings/index-modules.md#index-codec) instead of disabling the `_source`.
::::



## Including / Excluding fields from `_source` [include-exclude]

An expert-only feature is the ability to prune the contents of the `_source` field after the document has been indexed, but before the `_source` field is stored.

::::{warning}
Removing fields from the `_source` has similar downsides to disabling `_source`, especially the fact that you cannot reindex documents from one Elasticsearch index to another. Consider using [source filtering](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#source-filtering) instead.
::::


The `includes`/`excludes` parameters (which also accept wildcards) can be used as follows:

```console
PUT logs
{
  "mappings": {
    "_source": {
      "includes": [
        "*.count",
        "meta.*"
      ],
      "excludes": [
        "meta.description",
        "meta.other.*"
      ]
    }
  }
}

PUT logs/_doc/1
{
  "requests": {
    "count": 10,
    "foo": "bar" <1>
  },
  "meta": {
    "name": "Some metric",
    "description": "Some metric description", <1>
    "other": {
      "foo": "one", <1>
      "baz": "two" <1>
    }
  }
}

GET logs/_search
{
  "query": {
    "match": {
      "meta.other.foo": "one" <2>
    }
  }
}
```

1. These fields will be removed from the stored `_source` field.
2. We can still search on this field, even though it is not in the stored `_source`.
