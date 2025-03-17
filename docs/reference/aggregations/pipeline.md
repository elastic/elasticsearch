---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-pipeline.html
---

# Pipeline [search-aggregations-pipeline]

Pipeline aggregations work on the outputs produced from other aggregations rather than from document sets, adding information to the output tree. There are many different types of pipeline aggregation, each computing different information from other aggregations, but these types can be broken down into two families:

*Parent*
:   A family of pipeline aggregations that is provided with the output of its parent aggregation and is able to compute new buckets or new aggregations to add to existing buckets.

*Sibling*
:   Pipeline aggregations that are provided with the output of a sibling aggregation and are able to compute a new aggregation which will be at the same level as the sibling aggregation.

Pipeline aggregations can reference the aggregations they need to perform their computation by using the `buckets_path` parameter to indicate the paths to the required metrics. The syntax for defining these paths can be found in the [`buckets_path` Syntax](#buckets-path-syntax) section below.

Pipeline aggregations cannot have sub-aggregations but depending on the type it can reference another pipeline in the `buckets_path` allowing pipeline aggregations to be chained. For example, you can chain together two derivatives to calculate the second derivative (i.e. a derivative of a derivative).

::::{note}
Because pipeline aggregations only add to the output, when chaining pipeline aggregations the output of each pipeline aggregation will be included in the final output.
::::



## `buckets_path` Syntax [buckets-path-syntax]

Most pipeline aggregations require another aggregation as their input. The input aggregation is defined via the `buckets_path` parameter, which follows a specific format:

```ebnf
AGG_SEPARATOR       =  `>` ;
METRIC_SEPARATOR    =  `.` ;
AGG_NAME            =  <the name of the aggregation> ;
METRIC              =  <the name of the metric (in case of multi-value metrics aggregation)> ;
MULTIBUCKET_KEY     =  `[<KEY_NAME>]`
PATH                =  <AGG_NAME><MULTIBUCKET_KEY>? (<AGG_SEPARATOR>, <AGG_NAME> )* ( <METRIC_SEPARATOR>, <METRIC> ) ;
```

For example, the path `"my_bucket>my_stats.avg"` will path to the `avg` value in the `"my_stats"` metric, which is contained in the `"my_bucket"` bucket aggregation.

Here are some more examples:

* `multi_bucket["foo"]>single_bucket>multi_metric.avg` will go to the `avg` metric in the `"multi_metric"` agg under the single bucket `"single_bucket"` within the `"foo"` bucket of the `"multi_bucket"` multi-bucket aggregation.
* `agg1["foo"]._count` will get the `_count` metric for the `"foo"` bucket in the multi-bucket aggregation `"multi_bucket"`

Paths are relative from the position of the pipeline aggregation; they are not absolute paths, and the path cannot go back "up" the aggregation tree. For example, this derivative is embedded inside a date_histogram and refers to a "sibling" metric `"the_sum"`:

$$$buckets-path-example$$$

```console
POST /_search
{
  "aggs": {
    "my_date_histo": {
      "date_histogram": {
        "field": "timestamp",
        "calendar_interval": "day"
      },
      "aggs": {
        "the_sum": {
          "sum": { "field": "lemmings" }              <1>
        },
        "the_deriv": {
          "derivative": { "buckets_path": "the_sum" } <2>
        }
      }
    }
  }
}
```

1. The metric is called `"the_sum"`
2. The `buckets_path` refers to the metric via a relative path `"the_sum"`


`buckets_path` is also used for Sibling pipeline aggregations, where the aggregation is "next" to a series of buckets instead of embedded "inside" them. For example, the `max_bucket` aggregation uses the `buckets_path` to specify a metric embedded inside a sibling aggregation:

$$$buckets-path-sibling-example$$$

```console
POST /_search
{
  "aggs": {
    "sales_per_month": {
      "date_histogram": {
        "field": "date",
        "calendar_interval": "month"
      },
      "aggs": {
        "sales": {
          "sum": {
            "field": "price"
          }
        }
      }
    },
    "max_monthly_sales": {
      "max_bucket": {
        "buckets_path": "sales_per_month>sales" <1>
      }
    }
  }
}
```

1. `buckets_path` instructs this max_bucket aggregation that we want the maximum value of the `sales` aggregation in the `sales_per_month` date histogram.


If a Sibling pipeline agg references a multi-bucket aggregation, such as a `terms` agg, it also has the option to select specific keys from the multi-bucket. For example, a `bucket_script` could select two specific buckets (via their bucket keys) to perform the calculation:

$$$buckets-path-specific-bucket-example$$$

```console
POST /_search
{
  "aggs": {
    "sales_per_month": {
      "date_histogram": {
        "field": "date",
        "calendar_interval": "month"
      },
      "aggs": {
        "sale_type": {
          "terms": {
            "field": "type"
          },
          "aggs": {
            "sales": {
              "sum": {
                "field": "price"
              }
            }
          }
        },
        "hat_vs_bag_ratio": {
          "bucket_script": {
            "buckets_path": {
              "hats": "sale_type['hat']>sales",   <1>
              "bags": "sale_type['bag']>sales"    <1>
            },
            "script": "params.hats / params.bags"
          }
        }
      }
    }
  }
}
```

1. `buckets_path` selects the hats and bags buckets (via `['hat']`/`['bag']``) to use in the script specifically, instead of fetching all the buckets from `sale_type` aggregation



## Special Paths [_special_paths]

Instead of pathing to a metric, `buckets_path` can use a special `"_count"` path. This instructs the pipeline aggregation to use the document count as its input. For example, a derivative can be calculated on the document count of each bucket, instead of a specific metric:

$$$buckets-path-count-example$$$

```console
POST /_search
{
  "aggs": {
    "my_date_histo": {
      "date_histogram": {
        "field": "timestamp",
        "calendar_interval": "day"
      },
      "aggs": {
        "the_deriv": {
          "derivative": { "buckets_path": "_count" } <1>
        }
      }
    }
  }
}
```

1. By using `_count` instead of a metric name, we can calculate the derivative of document counts in the histogram


The `buckets_path` can also use `"_bucket_count"` and path to a multi-bucket aggregation to use the number of buckets returned by that aggregation in the pipeline aggregation instead of a metric. For example, a `bucket_selector` can be used here to filter out buckets which contain no buckets for an inner terms aggregation:

$$$buckets-path-bucket-count-example$$$

```console
POST /sales/_search
{
  "size": 0,
  "aggs": {
    "histo": {
      "date_histogram": {
        "field": "date",
        "calendar_interval": "day"
      },
      "aggs": {
        "categories": {
          "terms": {
            "field": "category"
          }
        },
        "min_bucket_selector": {
          "bucket_selector": {
            "buckets_path": {
              "count": "categories._bucket_count" <1>
            },
            "script": {
              "source": "params.count != 0"
            }
          }
        }
      }
    }
  }
}
```

1. By using `_bucket_count` instead of a metric name, we can filter out `histo` buckets where they contain no buckets for the `categories` aggregation



## Dealing with dots in agg names [dots-in-agg-names]

An alternate syntax is supported to cope with aggregations or metrics which have dots in the name, such as the `99.9`th [percentile](/reference/aggregations/search-aggregations-metrics-percentile-aggregation.md). This metric may be referred to as:

```js
"buckets_path": "my_percentile[99.9]"
```


## Dealing with gaps in the data [gap-policy]

Data in the real world is often noisy and sometimes contains **gaps** — places where data simply doesn’t exist. This can occur for a variety of reasons, the most common being:

* Documents falling into a bucket do not contain a required field
* There are no documents matching the query for one or more buckets
* The metric being calculated is unable to generate a value, likely because another dependent bucket is missing a value. Some pipeline aggregations have specific requirements that must be met (e.g. a derivative cannot calculate a metric for the first value because there is no previous value, HoltWinters moving average need "warmup" data to begin calculating, etc)

Gap policies are a mechanism to inform the pipeline aggregation about the desired behavior when "gappy" or missing data is encountered. All pipeline aggregations accept the `gap_policy` parameter. There are currently two gap policies to choose from:

*skip*
:   This option treats missing data as if the bucket does not exist. It will skip the bucket and continue calculating using the next available value.

*insert_zeros*
:   This option will replace missing values with a zero (`0`) and pipeline aggregation computation will proceed as normal.

*keep_values*
:   This option is similar to skip, except if the metric provides a non-null, non-NaN value this value is used, otherwise the empty bucket is skipped.






















