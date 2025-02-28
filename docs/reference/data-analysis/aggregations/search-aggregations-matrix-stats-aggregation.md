---
navigation_title: "Matrix stats"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-matrix-stats-aggregation.html
---

# Matrix stats aggregation [search-aggregations-matrix-stats-aggregation]


The `matrix_stats` aggregation is a numeric aggregation that computes the following statistics over a set of document fields:

`count`
:   Number of per field samples included in the calculation.

`mean`
:   The average value for each field.

`variance`
:   Per field Measurement for how spread out the samples are from the mean.

`skewness`
:   Per field measurement quantifying the asymmetric distribution around the mean.

`kurtosis`
:   Per field measurement quantifying the shape of the distribution.

`covariance`
:   A matrix that quantitatively describes how changes in one field are associated with another.

`correlation`
:   The covariance matrix scaled to a range of -1 to 1, inclusive. Describes the relationship between field distributions.

::::{important}
Unlike other metric aggregations, the `matrix_stats` aggregation does not support scripting.
::::


The following example demonstrates the use of matrix stats to describe the relationship between income and poverty.

$$$stats-aggregation-example$$$

```console
GET /_search
{
  "aggs": {
    "statistics": {
      "matrix_stats": {
        "fields": [ "poverty", "income" ]
      }
    }
  }
}
```

The aggregation type is `matrix_stats` and the `fields` setting defines the set of fields (as an array) for computing the statistics. The above request returns the following response:

```console-result
{
  ...
  "aggregations": {
    "statistics": {
      "doc_count": 50,
      "fields": [ {
          "name": "income",
          "count": 50,
          "mean": 51985.1,
          "variance": 7.383377037755103E7,
          "skewness": 0.5595114003506483,
          "kurtosis": 2.5692365287787124,
          "covariance": {
            "income": 7.383377037755103E7,
            "poverty": -21093.65836734694
          },
          "correlation": {
            "income": 1.0,
            "poverty": -0.8352655256272504
          }
        }, {
          "name": "poverty",
          "count": 50,
          "mean": 12.732000000000001,
          "variance": 8.637730612244896,
          "skewness": 0.4516049811903419,
          "kurtosis": 2.8615929677997767,
          "covariance": {
            "income": -21093.65836734694,
            "poverty": 8.637730612244896
          },
          "correlation": {
            "income": -0.8352655256272504,
            "poverty": 1.0
          }
        } ]
    }
  }
}
```

The `doc_count` field indicates the number of documents involved in the computation of the statistics.

## Multi Value Fields [_multi_value_fields]

The `matrix_stats` aggregation treats each document field as an independent sample. The `mode` parameter controls what array value the aggregation will use for array or multi-valued fields. This parameter can take one of the following:

`avg`
:   (default) Use the average of all values.

`min`
:   Pick the lowest value.

`max`
:   Pick the highest value.

`sum`
:   Use the sum of all values.

`median`
:   Use the median of all values.


## Missing Values [_missing_values_3]

The `missing` parameter defines how documents that are missing a value should be treated. By default they will be ignored but it is also possible to treat them as if they had a value. This is done by adding a set of fieldname : value mappings to specify default values per field.

$$$stats-aggregation-missing-example$$$

```console
GET /_search
{
  "aggs": {
    "matrixstats": {
      "matrix_stats": {
        "fields": [ "poverty", "income" ],
        "missing": { "income": 50000 }      <1>
      }
    }
  }
}
```

1. Documents without a value in the `income` field will have the default value `50000`.



