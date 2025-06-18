---
navigation_title: "Keyword"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/keyword.html
---

# Keyword type family [keyword]


The keyword family includes the following field types:

* [`keyword`](#keyword-field-type), which is used for structured content such as IDs, email addresses, hostnames, status codes, zip codes, or tags.
* [`constant_keyword`](#constant-keyword-field-type) for keyword fields that always contain the same value.
* [`wildcard`](#wildcard-field-type) for unstructured machine-generated content. The `wildcard` type is optimized for fields with large values or high cardinality.

Keyword fields are often used in [sorting](/reference/elasticsearch/rest-apis/sort-search-results.md), [aggregations](/reference/aggregations/index.md), and [term-level queries](/reference/query-languages/query-dsl/term-level-queries.md), such as [`term`](/reference/query-languages/query-dsl/query-dsl-term-query.md).

::::{tip}
Avoid using keyword fields for full-text search. Use the [`text`](/reference/elasticsearch/mapping-reference/text.md) field type instead.
::::



## Keyword field type [keyword-field-type]

Below is an example of a mapping for a basic `keyword` field:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "tags": {
        "type":  "keyword"
      }
    }
  }
}
```

::::{admonition} Mapping numeric identifiers
:class: tip

Not all numeric data should be mapped as a [numeric](/reference/elasticsearch/mapping-reference/number.md) field data type. {{es}} optimizes numeric fields, such as `integer` or `long`, for [`range`](/reference/query-languages/query-dsl/query-dsl-range-query.md) queries. However, `keyword` fields are better for [`term`](/reference/query-languages/query-dsl/query-dsl-term-query.md) and other [term-level](/reference/query-languages/query-dsl/term-level-queries.md) queries.

Identifiers, such as an ISBN or a product ID, are rarely used in `range` queries. However, they are often retrieved using term-level queries.

Consider mapping a numeric identifier as a `keyword` if:

* You don’t plan to search for the identifier data using [`range`](/reference/query-languages/query-dsl/query-dsl-range-query.md) queries.
* Fast retrieval is important. `term` query searches on `keyword` fields are often faster than `term` searches on numeric fields.

If you’re unsure which to use, you can use a [multi-field](/reference/elasticsearch/mapping-reference/multi-fields.md) to map the data as both a `keyword` *and* a numeric data type.

::::



### Parameters for basic keyword fields [keyword-params]

The following parameters are accepted by `keyword` fields:

[`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md)
:   Should the field be stored on disk in a column-stride fashion, so that it can later be used for sorting, aggregations, or scripting? Accepts `true` (default) or `false`.

[`eager_global_ordinals`](/reference/elasticsearch/mapping-reference/eager-global-ordinals.md)
:   Should global ordinals be loaded eagerly on refresh? Accepts `true` or `false` (default). Enabling this is a good idea on fields that are frequently used for terms aggregations.

[`fields`](/reference/elasticsearch/mapping-reference/multi-fields.md)
:   Multi-fields allow the same string value to be indexed in multiple ways for different purposes, such as one field for search and a multi-field for sorting and aggregations.

[`ignore_above`](/reference/elasticsearch/mapping-reference/ignore-above.md)
:   Do not index any string longer than this value. Defaults to `2147483647` in standard indices so that all values would be accepted, and `8191` in logsdb indices to protect against Lucene's term byte-length limit of `32766`. Please however note that default dynamic mapping rules create a sub `keyword` field that overrides this default by setting `ignore_above: 256`.

[`index`](/reference/elasticsearch/mapping-reference/mapping-index.md)
:   Should the field be quickly searchable? Accepts `true` (default) and `false`. `keyword` fields that only have [`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md) enabled can still be queried, albeit slower.

[`index_options`](/reference/elasticsearch/mapping-reference/index-options.md)
:   What information should be stored in the index, for scoring purposes. Defaults to `docs` but can also be set to `freqs` to take term frequency into account when computing scores.

[`meta`](/reference/elasticsearch/mapping-reference/mapping-field-meta.md)
:   Metadata about the field.

[`norms`](/reference/elasticsearch/mapping-reference/norms.md)
:   Whether field-length should be taken into account when scoring queries. Accepts `true` or `false` (default).

[`null_value`](/reference/elasticsearch/mapping-reference/null-value.md)
:   Accepts a string value which is substituted for any explicit `null` values. Defaults to `null`, which means the field is treated as missing. Note that this cannot be set if the `script` value is used.

`on_script_error`
:   Defines what to do if the script defined by the `script` parameter throws an error at indexing time. Accepts `fail` (default), which will cause the entire document to be rejected, and `continue`, which will register the field in the document’s [`_ignored`](/reference/elasticsearch/mapping-reference/mapping-ignored-field.md) metadata field and continue indexing. This parameter can only be set if the `script` field is also set.

`script`
:   If this parameter is set, then the field will index values generated by this script, rather than reading the values directly from the source. If a value is set for this field on the input document, then the document will be rejected with an error. Scripts are in the same format as their [runtime equivalent](docs-content://manage-data/data-store/mapping/map-runtime-field.md). Values emitted by the script are normalized as usual, and will be ignored if they are longer that the value set on `ignore_above`.

[`store`](/reference/elasticsearch/mapping-reference/mapping-store.md)
:   Whether the field value should be stored and retrievable separately from the [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) field. Accepts `true` or `false` (default).

[`similarity`](/reference/elasticsearch/mapping-reference/similarity.md)
:   Which scoring algorithm or *similarity* should be used. Defaults to `BM25`.

[`normalizer`](/reference/elasticsearch/mapping-reference/normalizer.md)
:   How to pre-process the keyword prior to indexing. Defaults to `null`, meaning the keyword is kept as-is.

`split_queries_on_whitespace`
:   Whether [full text queries](/reference/query-languages/query-dsl/full-text-queries.md) should split the input on whitespace when building a query for this field. Accepts `true` or `false` (default).

`time_series_dimension`
:   (Optional, Boolean)

    Marks the field as a [time series dimension](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-series-dimension). Defaults to `false`.

    The `index.mapping.dimension_fields.limit` [index setting](/reference/elasticsearch/index-settings/time-series.md) limits the number of dimensions in an index.

    Dimension fields have the following constraints:
    * The `doc_values` and `index` mapping parameters must be `true`.
    * Dimension values are used to identify a document’s time series. If dimension values are altered in any way during indexing, the document will be stored as belonging to different from intended time series. As a result there are additional constraints: the field cannot use a [`normalizer`](/reference/elasticsearch/mapping-reference/normalizer.md).


## Synthetic `_source` [keyword-synthetic-source]

Synthetic source may sort `keyword` fields and remove duplicates. For example:

$$$synthetic-source-keyword-example-default$$$

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
      "kwd": { "type": "keyword" }
    }
  }
}
PUT idx/_doc/1
{
  "kwd": ["foo", "foo", "bar", "baz"]
}
```

Will become:

```console-result
{
  "kwd": ["bar", "baz", "foo"]
}
```

If a `keyword` field sets `store` to `true` then order and duplicates are preserved. For example:

$$$synthetic-source-keyword-example-stored$$$

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
      "kwd": { "type": "keyword", "store": true }
    }
  }
}
PUT idx/_doc/1
{
  "kwd": ["foo", "foo", "bar", "baz"]
}
```

Will become:

```console-result
{
  "kwd": ["foo", "foo", "bar", "baz"]
}
```

Values longer than `ignore_above` are preserved but sorted to the end. For example:

$$$synthetic-source-keyword-example-ignore-above$$$

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
      "kwd": { "type": "keyword", "ignore_above": 3 }
    }
  }
}
PUT idx/_doc/1
{
  "kwd": ["foo", "foo", "bang", "bar", "baz"]
}
```

Will become:

```console-result
{
  "kwd": ["bar", "baz", "foo", "bang"]
}
```

If `null_value` is configured, `null` values are replaced with the `null_value` in synthetic source:

$$$synthetic-source-keyword-example-null-value$$$

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
      "kwd": { "type": "keyword", "null_value": "NA" }
    }
  }
}
PUT idx/_doc/1
{
  "kwd": ["foo", null, "bar"]
}
```

Will become:

```console-result
{
  "kwd": ["NA", "bar", "foo"]
}
```


## Constant keyword field type [constant-keyword-field-type]

Constant keyword is a specialization of the `keyword` field for the case that all documents in the index have the same value.

```console
PUT logs-debug
{
  "mappings": {
    "properties": {
      "@timestamp": {
        "type": "date"
      },
      "message": {
        "type": "text"
      },
      "level": {
        "type": "constant_keyword",
        "value": "debug"
      }
    }
  }
}
```

`constant_keyword` supports the same queries and aggregations as `keyword` fields do, but takes advantage of the fact that all documents have the same value per index to execute queries more efficiently.

It is both allowed to submit documents that don’t have a value for the field or that have a value equal to the value configured in mappings. The two below indexing requests are equivalent:

```console
POST logs-debug/_doc
{
  "@timestamp": "2019-12-12",
  "message": "Starting up Elasticsearch",
  "level": "debug"
}

POST logs-debug/_doc
{
  "@timestamp": "2019-12-12",
  "message": "Starting up Elasticsearch"
}
```

However providing a value that is different from the one configured in the mapping is disallowed.

In case no `value` is provided in the mappings, the field will automatically configure itself based on the value contained in the first indexed document. While this behavior can be convenient, note that it means that a single poisonous document can cause all other documents to be rejected if it had a wrong value.

Before a value has been provided (either through the mappings or from a document), queries on the field will not match any documents. This includes [`exists`](/reference/query-languages/query-dsl/query-dsl-exists-query.md) queries.

The `value` of the field cannot be changed after it has been set.


### Parameters for constant keyword fields [constant-keyword-params]

The following mapping parameters are accepted:

[`meta`](/reference/elasticsearch/mapping-reference/mapping-field-meta.md)
:   Metadata about the field.

`value`
:   The value to associate with all documents in the index. If this parameter is not provided, it is set based on the first document that gets indexed.


## Wildcard field type [wildcard-field-type]

The `wildcard` field type is a specialized keyword field for unstructured machine-generated content you plan to search using grep-like [`wildcard`](/reference/query-languages/query-dsl/query-dsl-wildcard-query.md) and [`regexp`](/reference/query-languages/query-dsl/query-dsl-regexp-query.md) queries. The `wildcard` type is optimized for fields with large values or high cardinality.

::::{admonition} Mapping unstructured content
:name: mapping-unstructured-content

You can map a field containing unstructured content to either a `text` or keyword family field. The best field type depends on the nature of the content and how you plan to search the field.

Use the `text` field type if:

* The content is human-readable, such as an email body or product description.
* You plan to search the field for individual words or phrases, such as `the brown fox jumped`, using [full text queries](/reference/query-languages/query-dsl/full-text-queries.md). {{es}} [analyzes](docs-content://manage-data/data-store/text-analysis.md) `text` fields to return the most relevant results for these queries.

Use a keyword family field type if:

* The content is machine-generated, such as a log message or HTTP request information.
* You plan to search the field for exact full values, such as `org.foo.bar`, or partial character sequences, such as `org.foo.*`, using [term-level queries](/reference/query-languages/query-dsl/term-level-queries.md).

**Choosing a keyword family field type**

If you choose a keyword family field type, you can map the field as a `keyword` or `wildcard` field depending on the cardinality and size of the field’s values. Use the `wildcard` type if you plan to regularly search the field using a [`wildcard`](/reference/query-languages/query-dsl/query-dsl-wildcard-query.md) or [`regexp`](/reference/query-languages/query-dsl/query-dsl-regexp-query.md) query and meet one of the following criteria:

* The field contains more than a million unique values.<br> AND<br> You plan to regularly search the field using a pattern with leading wildcards, such as `*foo` or `*baz`.
* The field contains values larger than 32KB.<br> AND<br> You plan to regularly search the field using any wildcard pattern.

Otherwise, use the `keyword` field type for faster searches, faster indexing, and lower storage costs. For an in-depth comparison and decision flowchart, see our [related blog post](https://www.elastic.co/blog/find-strings-within-strings-faster-with-the-new-elasticsearch-wildcard-field).

**Switching from a `text` field to a keyword field**

If you previously used a `text` field to index unstructured machine-generated content, you can [reindex to update the mapping](docs-content://manage-data/data-store/mapping/explicit-mapping.md#update-mapping) to a `keyword` or `wildcard` field. We also recommend you update your application or workflow to replace any word-based [full text queries](/reference/query-languages/query-dsl/full-text-queries.md) on the field to equivalent [term-level queries](/reference/query-languages/query-dsl/term-level-queries.md).

::::


Internally the `wildcard` field indexes the whole field value using ngrams and stores the full string. The index is used as a rough filter to cut down the number of values that are then checked by retrieving and checking the full values. This field is especially well suited to run grep-like queries on log lines. Storage costs are typically lower than those of `keyword` fields but search speeds for exact matches on full terms are slower. If the field values share many prefixes, such as URLs for the same website, storage costs for a `wildcard` field may be higher than an equivalent `keyword` field.

You index and search a wildcard field as follows

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "my_wildcard": {
        "type": "wildcard"
      }
    }
  }
}

PUT my-index-000001/_doc/1
{
  "my_wildcard" : "This string can be quite lengthy"
}

GET my-index-000001/_search
{
  "query": {
    "wildcard": {
      "my_wildcard": {
        "value": "*quite*lengthy"
      }
    }
  }
}
```


### Parameters for wildcard fields [wildcard-params]

The following parameters are accepted by `wildcard` fields:

[`null_value`](/reference/elasticsearch/mapping-reference/null-value.md)
:   Accepts a string value which is substituted for any explicit `null` values. Defaults to `null`, which means the field is treated as missing.

[`ignore_above`](/reference/elasticsearch/mapping-reference/ignore-above.md)
:   Do not index any string longer than this value. Defaults to `2147483647` in standard indices so that all values would be accepted, and `8191` in logsdb indices to protect against Lucene's term byte-length limit of `32766`.


### Limitations [_limitations]

* `wildcard` fields are untokenized like keyword fields, so do not support queries that rely on word positions such as phrase queries.
* When running `wildcard` queries any `rewrite` parameter is ignored. The scoring is always a constant score.


## Synthetic `_source` [wildcard-synthetic-source]

Synthetic source may sort `wildcard` field values. For example:

$$$synthetic-source-wildcard-example$$$

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
      "card": { "type": "wildcard" }
    }
  }
}
PUT idx/_doc/1
{
  "card": ["king", "ace", "ace", "jack"]
}
```

Will become:

```console-result
{
  "card": ["ace", "jack", "king"]
}
```
