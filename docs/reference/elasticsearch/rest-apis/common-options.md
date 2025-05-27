---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/common-options.html
applies_to:
  stack: all
navigation_title: Common options
---

# Elasticsearch API common options [common-options]

All {{es}} REST APIs support the following options.


## Pretty Results [_pretty_results]

When appending `?pretty=true` to any request made, the JSON returned will be pretty formatted (use it for debugging only!). Another option is to set `?format=yaml` which will cause the result to be returned in the (sometimes) more readable yaml format.


## Human readable output [_human_readable_output]

Statistics are returned in a format suitable for humans (e.g. `"exists_time": "1h"` or `"size": "1kb"`) and for computers (e.g. `"exists_time_in_millis": 3600000` or `"size_in_bytes": 1024`). The human readable values can be turned off by adding `?human=false` to the query string. This makes sense when the stats results are being consumed by a monitoring tool, rather than intended for human consumption. The default for the `human` flag is `false`.


## Date Math [date-math]

Most parameters which accept a formatted date value — such as `gt` and `lt` in [`range` queries](/reference/query-languages/query-dsl/query-dsl-range-query.md), or `from` and `to` in [`daterange` aggregations](/reference/aggregations/search-aggregations-bucket-daterange-aggregation.md) — understand date maths.

The expression starts with an anchor date, which can either be `now`, or a date string ending with `||`. This anchor date can optionally be followed by one or more maths expressions:

* `+1h`: Add one hour
* `-1d`: Subtract one day
* `/d`: Round down to the nearest day

The supported time units differ from those supported by [time units](/reference/elasticsearch/rest-apis/api-conventions.md#time-units) for durations. The supported units are:

`y`
:   Years

`M`
:   Months

`w`
:   Weeks

`d`
:   Days

`h`
:   Hours

`H`
:   Hours

`m`
:   Minutes

`s`
:   Seconds

Assuming `now` is `2001-01-01 12:00:00`, some examples are:

`now+1h`
:   `now` in milliseconds plus one hour. Resolves to: `2001-01-01 13:00:00`

`now-1h`
:   `now` in milliseconds minus one hour. Resolves to: `2001-01-01 11:00:00`

`now-1h/d`
:   `now` in milliseconds minus one hour, rounded down to UTC 00:00. Resolves to: `2001-01-01 00:00:00`

`2001.02.01\|\|+1M/d`
:   `2001-02-01` in milliseconds plus one month. Resolves to: `2001-03-01 00:00:00`


## Response Filtering [common-options-response-filtering]

All REST APIs accept a `filter_path` parameter that can be used to reduce the response returned by Elasticsearch. This parameter takes a comma separated list of filters expressed with the dot notation:

```console
GET /_search?q=kimchy&filter_path=took,hits.hits._id,hits.hits._score
```

Responds:

```console-result
{
  "took" : 3,
  "hits" : {
    "hits" : [
      {
        "_id" : "0",
        "_score" : 1.6375021
      }
    ]
  }
}
```

It also supports the `*` wildcard character to match any field or part of a field’s name:

```console
GET /_cluster/state?filter_path=metadata.indices.*.stat*
```

Responds:

```console-result
{
  "metadata" : {
    "indices" : {
      "my-index-000001": {"state": "open"}
    }
  }
}
```

And the `**` wildcard can be used to include fields without knowing the exact path of the field. For example, we can return the state of every shard with this request:

```console
GET /_cluster/state?filter_path=routing_table.indices.**.state
```

Responds:

```console-result
{
  "routing_table": {
    "indices": {
      "my-index-000001": {
        "shards": {
          "0": [{"state": "STARTED"}, {"state": "UNASSIGNED"}]
        }
      }
    }
  }
}
```

It is also possible to exclude one or more fields by prefixing the filter with the char `-`:

```console
GET /_count?filter_path=-_shards
```

Responds:

```console-result
{
  "count" : 5
}
```

And for more control, both inclusive and exclusive filters can be combined in the same expression. In this case, the exclusive filters will be applied first and the result will be filtered again using the inclusive filters:

```console
GET /_cluster/state?filter_path=metadata.indices.*.state,-metadata.indices.logstash-*
```

Responds:

```console-result
{
  "metadata" : {
    "indices" : {
      "my-index-000001" : {"state" : "open"},
      "my-index-000002" : {"state" : "open"},
      "my-index-000003" : {"state" : "open"}
    }
  }
}
```

Note that Elasticsearch sometimes returns directly the raw value of a field, like the `_source` field. If you want to filter `_source` fields, you should consider combining the already existing `_source` parameter (see [Get API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-get) for more details) with the `filter_path` parameter like this:

```console
POST /library/_doc?refresh
{"title": "Book #1", "rating": 200.1}
POST /library/_doc?refresh
{"title": "Book #2", "rating": 1.7}
POST /library/_doc?refresh
{"title": "Book #3", "rating": 0.1}
GET /_search?filter_path=hits.hits._source&_source=title&sort=rating:desc
```

```console-result
{
  "hits" : {
    "hits" : [ {
      "_source":{"title":"Book #1"}
    }, {
      "_source":{"title":"Book #2"}
    }, {
      "_source":{"title":"Book #3"}
    } ]
  }
}
```


## Flat Settings [_flat_settings]

The `flat_settings` flag affects rendering of the lists of settings. When the `flat_settings` flag is `true`, settings are returned in a flat format:

```console
GET my-index-000001/_settings?flat_settings=true
```

Returns:

```console-result
{
  "my-index-000001" : {
    "settings": {
      "index.number_of_replicas": "1",
      "index.number_of_shards": "1",
      "index.creation_date": "1474389951325",
      "index.uuid": "n6gzFZTgS664GUfx0Xrpjw",
      "index.version.created": ...,
      "index.routing.allocation.include._tier_preference" : "data_content",
      "index.provided_name" : "my-index-000001"
    }
  }
}
```

When the `flat_settings` flag is `false`, settings are returned in a more human readable structured format:

```console
GET my-index-000001/_settings?flat_settings=false
```

Returns:

```console-result
{
  "my-index-000001" : {
    "settings" : {
      "index" : {
        "number_of_replicas": "1",
        "number_of_shards": "1",
        "creation_date": "1474389951325",
        "uuid": "n6gzFZTgS664GUfx0Xrpjw",
        "version": {
          "created": ...
        },
        "routing": {
          "allocation": {
            "include": {
              "_tier_preference": "data_content"
            }
          }
        },
        "provided_name" : "my-index-000001"
      }
    }
  }
}
```

By default `flat_settings` is set to `false`.


## Fuzziness [fuzziness]

Some queries and APIs support parameters to allow inexact *fuzzy* matching, using the `fuzziness` parameter.

When querying `text` or `keyword` fields, `fuzziness` is interpreted as a [Levenshtein Edit Distance](https://en.wikipedia.org/wiki/Levenshtein_distance) — the number of one character changes that need to be made to one string to make it the same as another string.

The `fuzziness` parameter can be specified as:

`0`, `1`, `2`
:   The maximum allowed Levenshtein Edit Distance (or number of edits)

`AUTO`
:   Generates an edit distance based on the length of the term. Low and high distance arguments may be optionally provided `AUTO:[low],[high]`. If not specified, the default values are 3 and 6, equivalent to `AUTO:3,6` that make for lengths:

`0..2`
:   Must match exactly

`3..5`
:   One edit allowed

`>5`
:   Two edits allowed

`AUTO` should generally be the preferred value for `fuzziness`.



## Enabling stack traces [common-options-error-options]

By default when a request returns an error Elasticsearch doesn’t include the stack trace of the error. You can enable that behavior by setting the `error_trace` url parameter to `true`. For example, by default when you send an invalid `size` parameter to the `_search` API:

```console
POST /my-index-000001/_search?size=surprise_me
```

The response looks like:

```console-result
{
  "error" : {
    "root_cause" : [
      {
        "type" : "illegal_argument_exception",
        "reason" : "Failed to parse int parameter [size] with value [surprise_me]"
      }
    ],
    "type" : "illegal_argument_exception",
    "reason" : "Failed to parse int parameter [size] with value [surprise_me]",
    "caused_by" : {
      "type" : "number_format_exception",
      "reason" : "For input string: \"surprise_me\""
    }
  },
  "status" : 400
}
```

But if you set `error_trace=true`:

```console
POST /my-index-000001/_search?size=surprise_me&error_trace=true
```

The response looks like:

```console-result
{
  "error": {
    "root_cause": [
      {
        "type": "illegal_argument_exception",
        "reason": "Failed to parse int parameter [size] with value [surprise_me]",
        "stack_trace": "Failed to parse int parameter [size] with value [surprise_me]]; nested: IllegalArgumentException..."
      }
    ],
    "type": "illegal_argument_exception",
    "reason": "Failed to parse int parameter [size] with value [surprise_me]",
    "stack_trace": "java.lang.IllegalArgumentException: Failed to parse int parameter [size] with value [surprise_me]\n    at org.elasticsearch.rest.RestRequest.paramAsInt(RestRequest.java:175)...",
    "caused_by": {
      "type": "number_format_exception",
      "reason": "For input string: \"surprise_me\"",
      "stack_trace": "java.lang.NumberFormatException: For input string: \"surprise_me\"\n    at java.lang.NumberFormatException.forInputString(NumberFormatException.java:65)..."
    }
  },
  "status": 400
}
```

