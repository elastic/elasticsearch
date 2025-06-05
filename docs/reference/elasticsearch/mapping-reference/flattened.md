---
navigation_title: "Flattened"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/flattened.html
---

# Flattened field type [flattened]


By default, each subfield in an object is mapped and indexed separately. If the names or types of the subfields are not known in advance, then they are [mapped dynamically](docs-content://manage-data/data-store/mapping/dynamic-mapping.md).

The `flattened` type provides an alternative approach, where the entire object is mapped as a single field. Given an object, the `flattened` mapping will parse out its leaf values and index them into one field as keywords. The object’s contents can then be searched through simple queries and aggregations.

This data type can be useful for indexing objects with a large or unknown number of unique keys. Only one field mapping is created for the whole JSON object, which can help prevent a [mappings explosion](docs-content://manage-data/data-store/mapping.md#mapping-limit-settings) from having too many distinct field mappings.

On the other hand, flattened object fields present a trade-off in terms of search functionality. Only basic queries are allowed, with no support for numeric range queries or highlighting. Further information on the limitations can be found in the [Supported operations](#supported-operations) section.

::::{note}
The `flattened` mapping type should **not** be used for indexing all document content, as it treats all values as keywords and does not provide full search functionality. The default approach, where each subfield has its own entry in the mappings, works well in the majority of cases.
::::


A flattened object field can be created as follows:

```console
PUT bug_reports
{
  "mappings": {
    "properties": {
      "title": {
        "type": "text"
      },
      "labels": {
        "type": "flattened"
      }
    }
  }
}

POST bug_reports/_doc/1
{
  "title": "Results are not sorted correctly.",
  "labels": {
    "priority": "urgent",
    "release": ["v1.2.5", "v1.3.0"],
    "timestamp": {
      "created": 1541458026,
      "closed": 1541457010
    }
  }
}
```

During indexing, tokens are created for each leaf value in the JSON object. The values are indexed as string keywords, without analysis or special handling for numbers or dates.

Querying the top-level `flattened` field searches all leaf values in the object:

```console
POST bug_reports/_search
{
  "query": {
    "term": {"labels": "urgent"}
  }
}
```

To query on a specific key in the flattened object, object dot notation is used:

```console
POST bug_reports/_search
{
  "query": {
    "term": {"labels.release": "v1.3.0"}
  }
}
```

## Supported operations [supported-operations]

Because of the similarities in the way values are indexed, `flattened` fields share much of the same mapping and search functionality as [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) fields.

Currently, flattened object fields can be used with the following query types:

* `term`, `terms`, and `terms_set`
* `prefix`
* `range`
* `match` and `multi_match`
* `query_string` and `simple_query_string`
* `exists`

When querying, it is not possible to refer to field keys using wildcards, as in `{ "term": {"labels.time*": 1541457010}}`. Note that all queries, including `range`, treat the values as string keywords. Highlighting is not supported on `flattened` fields.

It is possible to sort on a flattened object field, as well as perform simple keyword-style aggregations such as `terms`. As with queries, there is no special support for numerics — all values in the JSON object are treated as keywords. When sorting, this implies that values are compared lexicographically.

Flattened object fields currently cannot be stored. It is not possible to specify the [`store`](/reference/elasticsearch/mapping-reference/mapping-store.md) parameter in the mapping.


## Retrieving flattened fields [search-fields-flattened]

Field values and concrete subfields can be retrieved using the [fields parameter](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#search-fields-param). content. Since the `flattened` field maps an entire object with potentially many subfields as a single field, the response contains the unaltered structure from `_source`.

Single subfields, however, can be fetched by specifying them explicitly in the request. This only works for concrete paths, but not using wildcards:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "flattened_field": {
        "type": "flattened"
      }
    }
  }
}

PUT my-index-000001/_doc/1?refresh=true
{
  "flattened_field" : {
    "subfield" : "value"
  }
}

POST my-index-000001/_search
{
  "fields": ["flattened_field.subfield"],
  "_source": false
}
```

```console-result
{
  "took": 2,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": {
      "value": 1,
      "relation": "eq"
    },
    "max_score": 1.0,
    "hits": [{
      "_index": "my-index-000001",
      "_id": "1",
      "_score": 1.0,
      "fields": {
        "flattened_field.subfield" : [ "value" ]
      }
    }]
  }
}
```

You can also use a [Painless script](docs-content://explore-analyze/scripting/modules-scripting-painless.md) to retrieve values from sub-fields of flattened fields. Instead of including `doc['<field_name>'].value` in your Painless script, use `doc['<field_name>.<sub-field_name>'].value`. For example, if you have a flattened field called `label` with a `release` sub-field, your Painless script would be `doc['labels.release'].value`.

For example, let’s say your mapping contains two fields, one of which is of the `flattened` type:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "title": {
        "type": "text"
      },
      "labels": {
        "type": "flattened"
      }
    }
  }
}
```

Index a few documents containing your mapped fields. The `labels` field has three sub-fields:

```console
POST /my-index-000001/_bulk?refresh
{"index":{}}
{"title":"Something really urgent","labels":{"priority":"urgent","release":["v1.2.5","v1.3.0"],"timestamp":{"created":1541458026,"closed":1541457010}}}
{"index":{}}
{"title":"Somewhat less urgent","labels":{"priority":"high","release":["v1.3.0"],"timestamp":{"created":1541458026,"closed":1541457010}}}
{"index":{}}
{"title":"Not urgent","labels":{"priority":"low","release":["v1.2.0"],"timestamp":{"created":1541458026,"closed":1541457010}}}
```

Because `labels` is a `flattened` field type, the entire object is mapped as a single field. To retrieve values from this sub-field in a Painless script, use the `doc['<field_name>.<sub-field_name>'].value` format.

```painless
"script": {
  "source": """
    if (doc['labels.release'].value.equals('v1.3.0'))
    {emit(doc['labels.release'].value)}
    else{emit('Version mismatch')}
  """
```


## Parameters for flattened object fields [flattened-params]

The following mapping parameters are accepted:

`depth_limit`
:   The maximum allowed depth of the flattened object field, in terms of nested inner objects. If a flattened object field exceeds this limit, then an error will be thrown. Defaults to `20`. Note that `depth_limit` can be updated dynamically through the [update mapping](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping) API.

[`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md)
:   Should the field be stored on disk in a column-stride fashion, so that it can later be used for sorting, aggregations, or scripting? Accepts `true` (default) or `false`.

[`eager_global_ordinals`](/reference/elasticsearch/mapping-reference/eager-global-ordinals.md)
:   Should global ordinals be loaded eagerly on refresh? Accepts `true` or `false` (default). Enabling this is a good idea on fields that are frequently used for terms aggregations.

[`ignore_above`](/reference/elasticsearch/mapping-reference/ignore-above.md)
:   Leaf values longer than this limit will not be indexed. By default, there is no limit and all values will be indexed. Note that this limit applies to the leaf values within the flattened object field, and not the length of the entire field.

[`index`](/reference/elasticsearch/mapping-reference/mapping-index.md)
:   Determines if the field should be searchable. Accepts `true` (default) or `false`.

[`index_options`](/reference/elasticsearch/mapping-reference/index-options.md)
:   What information should be stored in the index for scoring purposes. Defaults to `docs` but can also be set to `freqs` to take term frequency into account when computing scores.

[`null_value`](/reference/elasticsearch/mapping-reference/null-value.md)
:   A string value which is substituted for any explicit `null` values within the flattened object field. Defaults to `null`, which means null fields are treated as if they were missing.

[`similarity`](/reference/elasticsearch/mapping-reference/similarity.md)
:   Which scoring algorithm or *similarity* should be used. Defaults to `BM25`.

`split_queries_on_whitespace`
:   Whether [full text queries](/reference/query-languages/query-dsl/full-text-queries.md) should split the input on whitespace when building a query for this field. Accepts `true` or `false` (default).

`time_series_dimensions`
:   (Optional, array of strings) A list of fields inside the flattened object, where each field is a dimension of the time series. Each field is specified using the relative path from the root field and does not include the root field name.


## Synthetic `_source` [flattened-synthetic-source]

Flattened fields support [synthetic`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md#synthetic-source) in their default configuration.

Synthetic source may sort `flattened` field values and remove duplicates. For example:

$$$synthetic-source-flattened-sorting-example$$$

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
      "flattened": { "type": "flattened" }
    }
  }
}
PUT idx/_doc/1
{
  "flattened": {
    "field": [ "apple", "apple", "banana", "avocado", "10", "200", "AVOCADO", "Banana", "Tangerine" ]
  }
}
```

Will become:

```console-result
{
  "flattened": {
    "field": [ "10", "200", "AVOCADO", "Banana", "Tangerine", "apple", "avocado", "banana" ]
  }
}
```

Synthetic source always uses nested objects instead of array of objects. For example:

$$$synthetic-source-flattened-array-example$$$

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
      "flattened": { "type": "flattened" }
    }
  }
}
PUT idx/_doc/1
{
  "flattened": {
      "field": [
        { "id": 1, "name": "foo" },
        { "id": 2, "name": "bar" },
        { "id": 3, "name": "baz" }
      ]
  }
}
```

Will become (note the nested objects instead of the "flattened" array):

```console-result
{
    "flattened": {
      "field": {
          "id": [ "1", "2", "3" ],
          "name": [ "bar", "baz", "foo" ]
      }
    }
}
```

Synthetic source always uses single-valued fields for one-element arrays. For example:

$$$synthetic-source-flattened-single-value-example$$$

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
      "flattened": { "type": "flattened" }
    }
  }
}
PUT idx/_doc/1
{
  "flattened": {
    "field": [ "foo" ]
  }
}
```

Will become (note the nested objects instead of the "flattened" array):

```console-result
{
  "flattened": {
    "field": "foo"
  }
}
```
