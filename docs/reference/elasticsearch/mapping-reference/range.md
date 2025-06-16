---
navigation_title: "Range"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/range.html
---

# Range field types [range]


Range field types represent a continuous range of values between an upper and lower bound. For example, a range can represent *any date in October* or *any integer from 0 to 9*. They are defined using the operators `gt` or `gte` for the lower bound, and `lt` or `lte` for the upper bound. They can be used for querying, and have limited support for aggregations. The only supported aggregations are [histogram](/reference/aggregations/search-aggregations-bucket-histogram-aggregation.md), [cardinality](/reference/aggregations/search-aggregations-metrics-cardinality-aggregation.md).

The following range types are supported:

`integer_range`
:   A range of signed 32-bit integers with a minimum value of `-2^31` and maximum of `2^31 - 1`.

`float_range`
:   A range of single-precision 32-bit IEEE 754 floating point values.

`long_range`
:   A range of signed 64-bit integers with a minimum value of `-2^63` and maximum of `2^63 - 1`.

`double_range`
:   A range of double-precision 64-bit IEEE 754 floating point values.

`date_range`
:   A range of [`date`](/reference/elasticsearch/mapping-reference/date.md) values. Date ranges support various date formats through the [`format`](/reference/elasticsearch/mapping-reference/mapping-date-format.md) mapping parameter. Regardless of the format used, date values are parsed into an unsigned 64-bit integer representing milliseconds since the Unix epoch in UTC. Values containing the `now` [date math](/reference/elasticsearch/rest-apis/common-options.md#date-math) expression are not supported.

`ip_range`
:   A range of ip values supporting either [IPv4](https://en.wikipedia.org/wiki/IPv4) or [IPv6](https://en.wikipedia.org/wiki/IPv6) (or mixed) addresses.

Below is an example of configuring a mapping with various range fields followed by an example that indexes several range types.

```console
PUT range_index
{
  "settings": {
    "number_of_shards": 2
  },
  "mappings": {
    "properties": {
      "expected_attendees": {
        "type": "integer_range"
      },
      "time_frame": {
        "type": "date_range", <1>
        "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
      }
    }
  }
}

PUT range_index/_doc/1?refresh
{
  "expected_attendees" : { <2>
    "gte" : 10,
    "lt" : 20
  },
  "time_frame" : {
    "gte" : "2015-10-31 12:00:00", <3>
    "lte" : "2015-11-01"
  }
}
```

1. `date_range` types accept the same field parameters defined by the [`date`](/reference/elasticsearch/mapping-reference/date.md) type.
2. Example indexing a meeting with 10 to 20 attendees, not including 20.
3. Example date range using date time stamp.


The following is an example of a [term query](/reference/query-languages/query-dsl/query-dsl-term-query.md) on the `integer_range` field named "expected_attendees". 12 is a value inside the range, so it will match.

```console
GET range_index/_search
{
  "query" : {
    "term" : {
      "expected_attendees" : {
        "value": 12
      }
    }
  }
}
```

The result produced by the above query.

```console-result
{
  "took": 13,
  "timed_out": false,
  "_shards" : {
    "total": 2,
    "successful": 2,
    "skipped" : 0,
    "failed": 0
  },
  "hits" : {
    "total" : {
        "value": 1,
        "relation": "eq"
    },
    "max_score" : 1.0,
    "hits" : [
      {
        "_index" : "range_index",
        "_id" : "1",
        "_score" : 1.0,
        "_source" : {
          "expected_attendees" : {
            "gte" : 10, "lt" : 20
          },
          "time_frame" : {
            "gte" : "2015-10-31 12:00:00", "lte" : "2015-11-01"
          }
        }
      }
    ]
  }
}
```

The following is an example of a `date_range` query over the `date_range` field named "time_frame".

```console
GET range_index/_search
{
  "query" : {
    "range" : {
      "time_frame" : { <1>
        "gte" : "2015-10-31",
        "lte" : "2015-11-01",
        "relation" : "within" <2>
      }
    }
  }
}
```

1. Range queries work the same as described in [range query](/reference/query-languages/query-dsl/query-dsl-range-query.md).
2. Range queries over range [fields](/reference/elasticsearch/mapping-reference/field-data-types.md) support a `relation` parameter which can be one of `WITHIN`, `CONTAINS`, `INTERSECTS` (default).


This query produces a similar result:

```console-result
{
  "took": 13,
  "timed_out": false,
  "_shards" : {
    "total": 2,
    "successful": 2,
    "skipped" : 0,
    "failed": 0
  },
  "hits" : {
    "total" : {
        "value": 1,
        "relation": "eq"
    },
    "max_score" : 1.0,
    "hits" : [
      {
        "_index" : "range_index",
        "_id" : "1",
        "_score" : 1.0,
        "_source" : {
          "expected_attendees" : {
            "gte" : 10, "lt" : 20
          },
          "time_frame" : {
            "gte" : "2015-10-31 12:00:00", "lte" : "2015-11-01"
          }
        }
      }
    ]
  }
}
```

## IP Range [ip-range]

In addition to the range format above, IP ranges can be provided in [CIDR](https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing#CIDR_notation) notation:

```console
PUT range_index/_mapping
{
  "properties": {
    "ip_allowlist": {
      "type": "ip_range"
    }
  }
}

PUT range_index/_doc/2
{
  "ip_allowlist" : "192.168.0.0/16"
}
```


## Parameters for range fields [range-params]

The following parameters are accepted by range types:

[`coerce`](/reference/elasticsearch/mapping-reference/coerce.md)
:   Try to convert strings to numbers and truncate fractions for integers. Accepts `true` (default) and `false`.

[`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md)
:   Should the field be stored on disk in a column-stride fashion, so that it can later be used for sorting, aggregations, or scripting? Accepts `true` (default) or `false`.

[`index`](/reference/elasticsearch/mapping-reference/mapping-index.md)
:   Should the field be searchable? Accepts `true` (default) and `false`.

[`store`](/reference/elasticsearch/mapping-reference/mapping-store.md)
:   Whether the field value should be stored and retrievable separately from the [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) field. Accepts `true` or `false` (default).


## Synthetic `_source` [range-synthetic-source]

`range` fields support [synthetic `_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md#synthetic-source) in their default configuration.

Synthetic source may sort `range` field values and remove duplicates for all `range` fields except `ip_range`. Ranges are sorted by their lower bound and then by upper bound. For example:

$$$synthetic-source-range-sorting-example$$$

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
      "my_range": { "type": "long_range" }
    }
  }
}

PUT idx/_doc/1
{
  "my_range": [
    {
        "gte": 200,
        "lte": 300
    },
    {
        "gte": 1,
        "lte": 100
    },
    {
        "gte": 200,
        "lte": 300
    },
    {
        "gte": 200,
        "lte": 500
    }
  ]
}
```

Will become:

```console-result
{
  "my_range": [
    {
        "gte": 1,
        "lte": 100
    },
    {
        "gte": 200,
        "lte": 300
    },
    {
        "gte": 200,
        "lte": 500
    }
  ]
}
```

Values of `ip_range` fields are not sorted but original order is not preserved. Duplicate ranges are removed. If `ip_range` field value is provided as a CIDR, it will be represented as a range of IP addresses in synthetic source.

For example:

$$$synthetic-source-range-ip-example$$$

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
      "my_range": { "type": "ip_range" }
    }
  }
}

PUT idx/_doc/1
{
  "my_range": [
    "10.0.0.0/24",
    {
      "gte": "10.0.0.0",
      "lte": "10.0.0.255"
    }
  ]
}
```

Will become:

```console-result
{
  "my_range": {
      "gte": "10.0.0.0",
      "lte": "10.0.0.255"
    }

}
```

$$$range-synthetic-source-inclusive$$$
Range field values are always represented as inclusive on both sides with bounds adjusted accordingly. Default values for range bounds are represented as `null`. This is true even if range bound was explicitly provided. For example:

$$$synthetic-source-range-normalization-example$$$

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
      "my_range": { "type": "long_range" }
    }
  }
}

PUT idx/_doc/1
{
  "my_range": {
    "gt": 200,
    "lt": 300
  }
}
```

Will become:

```console-result
{
  "my_range": {
    "gte": 201,
    "lte": 299
  }
}
```

$$$range-synthetic-source-default-bounds$$$
Default values for range bounds are represented as `null` in synthetic source. This is true even if range bound was explicitly provided with default value. For example:

$$$synthetic-source-range-bounds-example$$$

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
      "my_range": { "type": "integer_range" }
    }
  }
}

PUT idx/_doc/1
{
  "my_range": {
    "lte": 2147483647
  }
}
```

Will become:

```console-result
{
  "my_range": {
    "gte": null,
    "lte": null
  }
}
```

`date` ranges are formatted using provided `format` or by default using `yyyy-MM-dd'T'HH:mm:ss.SSSZ` format. For example:

$$$synthetic-source-range-date-example$$$

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
      "my_range": { "type": "date_range" }
    }
  }
}

PUT idx/_doc/1
{
  "my_range": [
    {
      "gte": 1504224000000,
      "lte": 1504569600000
    },
    {
      "gte": "2017-09-01",
      "lte": "2017-09-10"
    }
  ]
}
```

Will become:

```console-result
{
  "my_range": [
    {
      "gte": "2017-09-01T00:00:00.000Z",
      "lte": "2017-09-05T00:00:00.000Z"
    },
    {
      "gte": "2017-09-01T00:00:00.000Z",
      "lte": "2017-09-10T23:59:59.999Z"
    }
  ]
}
```


