---
navigation_title: "Composite"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-composite-aggregation.html
---

# Composite aggregation [search-aggregations-bucket-composite-aggregation]


::::{warning}
The composite aggregation is expensive. Load test your application before deploying a composite aggregation in production.
::::


A multi-bucket aggregation that creates composite buckets from different sources.

Unlike the other `multi-bucket` aggregations, you can use the `composite` aggregation to paginate **all** buckets from a multi-level aggregation efficiently. This aggregation provides a way to stream **all** buckets of a specific aggregation, similar to what [scroll](/reference/elasticsearch/rest-apis/paginate-search-results.md#scroll-search-results) does for documents.

The composite buckets are built from the combinations of the values extracted/created for each document and each combination is considered as a composite bucket.

For example, consider the following document:

```js
{
  "keyword": ["foo", "bar"],
  "number": [23, 65, 76]
}
```

Using `keyword` and `number` as source fields for the aggregation results in the following composite buckets:

```js
{ "keyword": "foo", "number": 23 }
{ "keyword": "foo", "number": 65 }
{ "keyword": "foo", "number": 76 }
{ "keyword": "bar", "number": 23 }
{ "keyword": "bar", "number": 65 }
{ "keyword": "bar", "number": 76 }
```

## Value sources [_value_sources]

The `sources` parameter defines the source fields to use when building composite buckets. The order that the `sources` are defined controls the order that the keys are returned.

::::{note}
You must use a unique name when defining `sources`.
::::


The `sources` parameter can be any of the following types:

* [Terms](#_terms)
* [Histogram](#_histogram)
* [Date histogram](#_date_histogram)
* [GeoTile grid](#_geotile_grid)

### Terms [_terms]

The `terms` value source is similar to a simple `terms` aggregation. The values are extracted from a field exactly like the `terms` aggregation.

Example:

$$$composite-aggregation-terms-field-example$$$

```console
GET /_search
{
  "size": 0,
  "aggs": {
    "my_buckets": {
      "composite": {
        "sources": [
          { "product": { "terms": { "field": "product" } } }
        ]
      }
    }
  }
}
```

Like the `terms` aggregation, it’s possible to use a [runtime field](docs-content://manage-data/data-store/mapping/runtime-fields.md) to create values for the composite buckets:

$$$composite-aggregation-terms-runtime-field-example$$$

```console
GET /_search
{
  "runtime_mappings": {
    "day_of_week": {
      "type": "keyword",
      "script": """
        emit(doc['timestamp'].value.dayOfWeekEnum
          .getDisplayName(TextStyle.FULL, Locale.ENGLISH))
      """
    }
  },
  "size": 0,
  "aggs": {
    "my_buckets": {
      "composite": {
        "sources": [
          {
            "dow": {
              "terms": { "field": "day_of_week" }
            }
          }
        ]
      }
    }
  }
}
```

Although similar, the `terms` value source doesn’t support the same set of parameters as the `terms` aggregation. For other supported value source parameters, see:

* [Order](#_order)
* [Missing bucket](#_missing_bucket)


### Histogram [_histogram]

The `histogram` value source can be applied on numeric values to build fixed size interval over the values. The `interval` parameter defines how the numeric values should be transformed. For instance an `interval` set to 5 will translate any numeric values to its closest interval, a value of `101` would be translated to `100` which is the key for the interval between 100 and 105.

Example:

$$$composite-aggregation-histogram-field-example$$$

```console
GET /_search
{
  "size": 0,
  "aggs": {
    "my_buckets": {
      "composite": {
        "sources": [
          { "histo": { "histogram": { "field": "price", "interval": 5 } } }
        ]
      }
    }
  }
}
```

Like the `histogram` aggregation it’s possible to use a [runtime field](docs-content://manage-data/data-store/mapping/runtime-fields.md) to create values for the composite buckets:

$$$composite-aggregation-histogram-runtime-field-example$$$

```console
GET /_search
{
  "runtime_mappings": {
    "price.discounted": {
      "type": "double",
      "script": """
        double price = doc['price'].value;
        if (doc['product'].value == 'mad max') {
          price *= 0.8;
        }
        emit(price);
      """
    }
  },
  "size": 0,
  "aggs": {
    "my_buckets": {
      "composite": {
        "sources": [
          {
            "price": {
              "histogram": {
                "interval": 5,
                "field": "price.discounted"
              }
            }
          }
        ]
      }
    }
  }
}
```


### Date histogram [_date_histogram]

The `date_histogram` is similar to the `histogram` value source except that the interval is specified by date/time expression:

$$$composite-aggregation-datehistogram-example$$$

```console
GET /_search
{
  "size": 0,
  "aggs": {
    "my_buckets": {
      "composite": {
        "sources": [
          { "date": { "date_histogram": { "field": "timestamp", "calendar_interval": "1d" } } }
        ]
      }
    }
  }
}
```

The example above creates an interval per day and translates all `timestamp` values to the start of its closest intervals. Available expressions for interval: `year`, `quarter`, `month`, `week`, `day`, `hour`, `minute`, `second`

Time values can also be specified via abbreviations supported by [time units](/reference/elasticsearch/rest-apis/api-conventions.md#time-units) parsing. Note that fractional time values are not supported, but you can address this by shifting to another time unit (e.g., `1.5h` could instead be specified as `90m`).

**Format**

Internally, a date is represented as a 64 bit number representing a timestamp in milliseconds-since-the-epoch. These timestamps are returned as the bucket keys. It is possible to return a formatted date string instead using the format specified with the format parameter:

$$$composite-aggregation-datehistogram-format-example$$$

```console
GET /_search
{
  "size": 0,
  "aggs": {
    "my_buckets": {
      "composite": {
        "sources": [
          {
            "date": {
              "date_histogram": {
                "field": "timestamp",
                "calendar_interval": "1d",
                "format": "yyyy-MM-dd"         <1>
              }
            }
          }
        ]
      }
    }
  }
}
```

1. Supports expressive date [format pattern](/reference/aggregations/search-aggregations-bucket-daterange-aggregation.md#date-format-pattern)


**Time Zone**

Date-times are stored in Elasticsearch in UTC. By default, all bucketing and rounding is also done in UTC. The `time_zone` parameter can be used to indicate that bucketing should use a different time zone.

Time zones may either be specified as an ISO 8601 UTC offset (e.g. `+01:00` or `-08:00`)  or as a timezone id, an identifier used in the TZ database like `America/Los_Angeles`.

**Offset**

Use the `offset` parameter to change the start value of each bucket by the specified positive (`+`) or negative offset (`-`) duration, such as `1h` for an hour, or `1d` for a day. See [Time units](/reference/elasticsearch/rest-apis/api-conventions.md#time-units) for more possible time duration options.

For example, when using an interval of `day`, each bucket runs from midnight to midnight. Setting the `offset` parameter to `+6h` changes each bucket to run from 6am to 6am:

$$$composite-aggregation-datehistogram-offset-example$$$

```console
PUT my-index-000001/_doc/1?refresh
{
  "date": "2015-10-01T05:30:00Z"
}

PUT my-index-000001/_doc/2?refresh
{
  "date": "2015-10-01T06:30:00Z"
}

GET my-index-000001/_search?size=0
{
  "aggs": {
    "my_buckets": {
      "composite" : {
        "sources" : [
          {
            "date": {
              "date_histogram" : {
                "field": "date",
                "calendar_interval": "day",
                "offset": "+6h",
                "format": "iso8601"
              }
            }
          }
        ]
      }
    }
  }
}
```

Instead of a single bucket starting at midnight, the above request groups the documents into buckets starting at 6am:

```console-result
{
  ...
  "aggregations": {
    "my_buckets": {
      "after_key": { "date": "2015-10-01T06:00:00.000Z" },
      "buckets": [
        {
          "key": { "date": "2015-09-30T06:00:00.000Z" },
          "doc_count": 1
        },
        {
          "key": { "date": "2015-10-01T06:00:00.000Z" },
          "doc_count": 1
        }
      ]
    }
  }
}
```

::::{note}
The start `offset` of each bucket is calculated after `time_zone` adjustments have been made.
::::



### GeoTile grid [_geotile_grid]

The `geotile_grid` value source works on `geo_point` fields and groups points into buckets that represent cells in a grid. The resulting grid can be sparse and only contains cells that have matching data. Each cell corresponds to a [map tile](https://en.wikipedia.org/wiki/Tiled_web_map) as used by many online map sites. Each cell is labeled using a `"{{zoom}}/{x}/{{y}}"` format, where zoom is equal to the user-specified precision.

$$$composite-aggregation-geotilegrid-example$$$

```console
GET /_search
{
  "size": 0,
  "aggs": {
    "my_buckets": {
      "composite": {
        "sources": [
          { "tile": { "geotile_grid": { "field": "location", "precision": 8 } } }
        ]
      }
    }
  }
}
```

**Precision**

The highest-precision geotile of length 29 produces cells that cover less than 10cm by 10cm of land. This precision is uniquely suited for composite aggregations as each tile does not have to be generated and loaded in memory.

See [Zoom level documentation](https://wiki.openstreetmap.org/wiki/Zoom_levels) on how precision (zoom) correlates to size on the ground. Precision for this aggregation can be between 0 and 29, inclusive.

**Bounding box filtering**

The geotile source can optionally be constrained to a specific geo bounding box, which reduces the range of tiles used. These bounds are useful when only a specific part of a geographical area needs high precision tiling.

$$$composite-aggregation-geotilegrid-boundingbox-example$$$

```console
GET /_search
{
  "size": 0,
  "aggs": {
    "my_buckets": {
      "composite": {
        "sources": [
          {
            "tile": {
              "geotile_grid": {
                "field": "location",
                "precision": 22,
                "bounds": {
                  "top_left": "POINT (4.9 52.4)",
                  "bottom_right": "POINT (5.0 52.3)"
                }
              }
            }
          }
        ]
      }
    }
  }
}
```


### Mixing different value sources [_mixing_different_value_sources]

The `sources` parameter accepts an array of value sources. It is possible to mix different value sources to create composite buckets. For example:

$$$composite-aggregation-mixing-sources-example$$$

```console
GET /_search
{
  "size": 0,
  "aggs": {
    "my_buckets": {
      "composite": {
        "sources": [
          { "date": { "date_histogram": { "field": "timestamp", "calendar_interval": "1d" } } },
          { "product": { "terms": { "field": "product" } } }
        ]
      }
    }
  }
}
```

This will create composite buckets from the values created by two value sources, a `date_histogram` and a `terms`. Each bucket is composed of two values, one for each value source defined in the aggregation. Any type of combinations is allowed and the order in the array is preserved in the composite buckets.

$$$composite-aggregation-mixing-three-sources-example$$$

```console
GET /_search
{
  "size": 0,
  "aggs": {
    "my_buckets": {
      "composite": {
        "sources": [
          { "shop": { "terms": { "field": "shop" } } },
          { "product": { "terms": { "field": "product" } } },
          { "date": { "date_histogram": { "field": "timestamp", "calendar_interval": "1d" } } }
        ]
      }
    }
  }
}
```



## Order [_order]

By default the composite buckets are sorted by their natural ordering. Values are sorted in ascending order of their values. When multiple value sources are requested, the ordering is done per value source, the first value of the composite bucket is compared to the first value of the other composite bucket and if they are equals the next values in the composite bucket are used for tie-breaking. This means that the composite bucket `[foo, 100]` is considered smaller than `[foobar, 0]` because `foo` is considered smaller than `foobar`. It is possible to define the direction of the sort for each value source by setting `order` to `asc` (default value) or `desc` (descending order) directly in the value source definition. For example:

$$$composite-aggregation-order-example$$$

```console
GET /_search
{
  "size": 0,
  "aggs": {
    "my_buckets": {
      "composite": {
        "sources": [
          { "date": { "date_histogram": { "field": "timestamp", "calendar_interval": "1d", "order": "desc" } } },
          { "product": { "terms": { "field": "product", "order": "asc" } } }
        ]
      }
    }
  }
}
```

... will sort the composite bucket in descending order when comparing values from the `date_histogram` source and in ascending order when comparing values from the `terms` source.


## Missing bucket [_missing_bucket]

By default documents without a value for a given source are ignored. It is possible to include them in the response by setting `missing_bucket` to `true` (defaults to `false`):

$$$composite-aggregation-missing-bucket-example$$$

```console
GET /_search
{
  "size": 0,
  "aggs": {
    "my_buckets": {
      "composite": {
        "sources": [{
          "product_name": {
            "terms": {
              "field": "product",
              "missing_bucket": true,
              "missing_order": "last"
            }
          }
        }]
      }
    }
  }
}
```

In the above example, the `product_name` source emits an explicit `null` bucket for documents without a `product` value. This bucket is placed last.

You can control the position of the `null` bucket using the optional `missing_order` parameter. If `missing_order` is `first` or `last`, the `null` bucket is placed in the respective first or last position. If `missing_order` is omitted or `default`, the source’s `order` determines the bucket’s position. If `order` is `asc` (ascending), the bucket is in the first position. If `order` is `desc` (descending), the bucket is in the last position.


## Size [_size]

The `size` parameter can be set to define how many composite buckets should be returned. Each composite bucket is considered as a single bucket, so setting a size of 10 will return the first 10 composite buckets created from the value sources. The response contains the values for each composite bucket in an array containing the values extracted from each value source. Defaults to `10`.


## Pagination [_pagination]

If the number of composite buckets is too high (or unknown) to be returned in a single response it is possible to split the retrieval in multiple requests. Since the composite buckets are flat by nature, the requested `size` is exactly the number of composite buckets that will be returned in the response (assuming that they are at least `size` composite buckets to return). If all composite buckets should be retrieved it is preferable to use a small size (`100` or `1000` for instance) and then use the `after` parameter to retrieve the next results. For example:

$$$composite-aggregation-after-key-example$$$

```console
GET /_search
{
  "size": 0,
  "aggs": {
    "my_buckets": {
      "composite": {
        "size": 2,
        "sources": [
          { "date": { "date_histogram": { "field": "timestamp", "calendar_interval": "1d" } } },
          { "product": { "terms": { "field": "product" } } }
        ]
      }
    }
  }
}
```

... returns:

```console-result
{
  ...
  "aggregations": {
    "my_buckets": {
      "after_key": {
        "date": 1494288000000,
        "product": "mad max"
      },
      "buckets": [
        {
          "key": {
            "date": 1494201600000,
            "product": "rocky"
          },
          "doc_count": 1
        },
        {
          "key": {
            "date": 1494288000000,
            "product": "mad max"
          },
          "doc_count": 2
        }
      ]
    }
  }
}
```

To get the next set of buckets, resend the same aggregation with the `after` parameter set to the `after_key` value returned in the response. For example, this request uses the `after_key` value provided in the previous response:

$$$composite-aggregation-after-example$$$

```console
GET /_search
{
  "size": 0,
  "aggs": {
    "my_buckets": {
      "composite": {
        "size": 2,
        "sources": [
          { "date": { "date_histogram": { "field": "timestamp", "calendar_interval": "1d", "order": "desc" } } },
          { "product": { "terms": { "field": "product", "order": "asc" } } }
        ],
        "after": { "date": 1494288000000, "product": "mad max" } <1>
      }
    }
  }
}
```

1. Should restrict the aggregation to buckets that sort **after** the provided values.


::::{note}
The `after_key` is **usually** the key to the last bucket returned in the response, but that isn’t guaranteed. Always use the returned `after_key` instead of deriving it from the buckets.
::::



## Early termination [_early_termination]

For optimal performance the [index sort](/reference/elasticsearch/index-settings/sorting.md) should be set on the index so that it matches parts or fully the source order in the composite aggregation. For instance the following index sort:

```console
PUT my-index-000001
{
  "settings": {
    "index": {
      "sort.field": [ "username", "timestamp" ],   <1>
      "sort.order": [ "asc", "desc" ]              <2>
    }
  },
  "mappings": {
    "properties": {
      "username": {
        "type": "keyword",
        "doc_values": true
      },
      "timestamp": {
        "type": "date"
      }
    }
  }
}
```

1. This index is sorted by `username` first then by `timestamp`.
2. …​ in ascending order for the `username` field and in descending order for the `timestamp` field.1. could be used to optimize these composite aggregations:



```console
GET /_search
{
  "size": 0,
  "aggs": {
    "my_buckets": {
      "composite": {
        "sources": [
          { "user_name": { "terms": { "field": "user_name" } } }     <1>
        ]
      }
    }
  }
}
```

1. `user_name` is a prefix of the index sort and the order matches (`asc`).


```console
GET /_search
{
  "size": 0,
  "aggs": {
    "my_buckets": {
      "composite": {
        "sources": [
          { "user_name": { "terms": { "field": "user_name" } } }, <1>
          { "date": { "date_histogram": { "field": "timestamp", "calendar_interval": "1d", "order": "desc" } } } <2>
        ]
      }
    }
  }
}
```

1. `user_name` is a prefix of the index sort and the order matches (`asc`).
2. `timestamp` matches also the prefix and the order matches (`desc`).


In order to optimize the early termination it is advised to set `track_total_hits` in the request to `false`. The number of total hits that match the request can be retrieved on the first request and it would be costly to compute this number on every page:

```console
GET /_search
{
  "size": 0,
  "track_total_hits": false,
  "aggs": {
    "my_buckets": {
      "composite": {
        "sources": [
          { "user_name": { "terms": { "field": "user_name" } } },
          { "date": { "date_histogram": { "field": "timestamp", "calendar_interval": "1d", "order": "desc" } } }
        ]
      }
    }
  }
}
```

Note that the order of the source is important, in the example below switching the `user_name` with the `timestamp` would deactivate the sort optimization since this configuration wouldn’t match the index sort specification. If the order of sources do not matter for your use case you can follow these simple guidelines:

* Put the fields with the highest cardinality first.
* Make sure that the order of the field matches the order of the index sort.
* Put multi-valued fields last since they cannot be used for early termination.

::::{warning}
[index sort](/reference/elasticsearch/index-settings/sorting.md) can slowdown indexing, it is very important to test index sorting with your specific use case and dataset to ensure that it matches your requirement. If it doesn’t note that `composite` aggregations will also try to early terminate on non-sorted indices if the query matches all document (`match_all` query).
::::



## Sub-aggregations [_sub_aggregations]

Like any `multi-bucket` aggregations the `composite` aggregation can hold sub-aggregations. These sub-aggregations can be used to compute other buckets or statistics on each composite bucket created by this parent aggregation. For instance the following example computes the average value of a field per composite bucket:

$$$composite-aggregation-subaggregations-example$$$

```console
GET /_search
{
  "size": 0,
  "aggs": {
    "my_buckets": {
      "composite": {
        "sources": [
          { "date": { "date_histogram": { "field": "timestamp", "calendar_interval": "1d", "order": "desc" } } },
          { "product": { "terms": { "field": "product" } } }
        ]
      },
      "aggregations": {
        "the_avg": {
          "avg": { "field": "price" }
        }
      }
    }
  }
}
```

... returns:

```console-result
{
  ...
  "aggregations": {
    "my_buckets": {
      "after_key": {
        "date": 1494201600000,
        "product": "rocky"
      },
      "buckets": [
        {
          "key": {
            "date": 1494460800000,
            "product": "apocalypse now"
          },
          "doc_count": 1,
          "the_avg": {
            "value": 10.0
          }
        },
        {
          "key": {
            "date": 1494374400000,
            "product": "mad max"
          },
          "doc_count": 1,
          "the_avg": {
            "value": 27.0
          }
        },
        {
          "key": {
            "date": 1494288000000,
            "product": "mad max"
          },
          "doc_count": 2,
          "the_avg": {
            "value": 22.5
          }
        },
        {
          "key": {
            "date": 1494201600000,
            "product": "rocky"
          },
          "doc_count": 1,
          "the_avg": {
            "value": 10.0
          }
        }
      ]
    }
  }
}
```


## Pipeline aggregations [search-aggregations-bucket-composite-aggregation-pipeline-aggregations]

The composite agg is not currently compatible with pipeline aggregations, nor does it make sense in most cases. E.g. due to the paging nature of composite aggs, a single logical partition (one day for example) might be spread over multiple pages. Since pipeline aggregations are purely post-processing on the final list of buckets, running something like a derivative on a composite page could lead to inaccurate results as it is only taking into account a "partial" result on that page.

Pipeline aggs that are self contained to a single bucket (such as `bucket_selector`) might be supported in the future.
