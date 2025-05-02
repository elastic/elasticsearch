---
navigation_title: "{{infer-cap}} bucket"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-pipeline-inference-bucket-aggregation.html
---

# {{infer-cap}} bucket aggregation [search-aggregations-pipeline-inference-bucket-aggregation]


A parent pipeline aggregation which loads a pre-trained model and performs {{infer}} on the collated result fields from the parent bucket aggregation.

To use the {{infer}} bucket aggregation, you need to have the same security privileges that are required for using the [get trained models API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-get-trained-models).

## Syntax [inference-bucket-agg-syntax]

A `inference` aggregation looks like this in isolation:

```js
{
  "inference": {
    "model_id": "a_model_for_inference", <1>
    "inference_config": { <2>
      "regression_config": {
        "num_top_feature_importance_values": 2
      }
    },
    "buckets_path": {
      "avg_cost": "avg_agg", <3>
      "max_cost": "max_agg"
    }
  }
}
```

1. The unique identifier or alias for the trained model.
2. The optional inference config which overrides the model’s default settings
3. Map the value of `avg_agg` to the model’s input field `avg_cost`


$$$inference-bucket-params$$$

| Parameter Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `model_id` | The ID or alias for the trained model. | Required | - |
| `inference_config` | Contains the inference type and its options. There are two types: [`regression`](#inference-agg-regression-opt) and [`classification`](#inference-agg-classification-opt) | Optional | - |
| `buckets_path` | Defines the paths to the input aggregations and maps the aggregation names to the field names expected by the model.See [`buckets_path` Syntax](/reference/aggregations/pipeline.md#buckets-path-syntax) for more details | Required | - |


## Configuration options for {{infer}} models [_configuration_options_for_infer_models]

The `inference_config` setting is optional and usually isn’t required as the pre-trained models come equipped with sensible defaults. In the context of aggregations some options can be overridden for each of the two types of model.


#### Configuration options for {{regression}} models [inference-agg-regression-opt]

`num_top_feature_importance_values`
:   (Optional, integer) Specifies the maximum number of [{{feat-imp}}](docs-content://explore-analyze/machine-learning/data-frame-analytics/ml-feature-importance.md) values per document. By default, it is zero and no {{feat-imp}} calculation occurs.


#### Configuration options for {{classification}} models [inference-agg-classification-opt]

`num_top_classes`
:   (Optional, integer) Specifies the number of top class predictions to return. Defaults to 0.

`num_top_feature_importance_values`
:   (Optional, integer) Specifies the maximum number of [{{feat-imp}}](docs-content://explore-analyze/machine-learning/data-frame-analytics/ml-feature-importance.md) values per document. Defaults to 0 which means no {{feat-imp}} calculation occurs.

`prediction_field_type`
:   (Optional, string) Specifies the type of the predicted field to write. Valid values are: `string`, `number`, `boolean`. When `boolean` is provided `1.0` is transformed to `true` and `0.0` to `false`.


## Example [inference-bucket-agg-example]

The following snippet aggregates a web log by `client_ip` and extracts a number of features via metric and bucket sub-aggregations as input to the {{infer}} aggregation configured with a model trained to identify suspicious client IPs:

```console
GET kibana_sample_data_logs/_search
{
  "size": 0,
  "aggs": {
    "client_ip": { <1>
      "composite": {
        "sources": [
          {
            "client_ip": {
              "terms": {
                "field": "clientip"
              }
            }
          }
        ]
      },
      "aggs": { <2>
        "url_dc": {
          "cardinality": {
            "field": "url.keyword"
          }
        },
        "bytes_sum": {
          "sum": {
            "field": "bytes"
          }
        },
        "geo_src_dc": {
          "cardinality": {
            "field": "geo.src"
          }
        },
        "geo_dest_dc": {
          "cardinality": {
            "field": "geo.dest"
          }
        },
        "responses_total": {
          "value_count": {
            "field": "timestamp"
          }
        },
        "success": {
          "filter": {
            "term": {
              "response": "200"
            }
          }
        },
        "error404": {
          "filter": {
            "term": {
              "response": "404"
            }
          }
        },
        "error503": {
          "filter": {
            "term": {
              "response": "503"
            }
          }
        },
        "malicious_client_ip": { <3>
          "inference": {
            "model_id": "malicious_clients_model",
            "buckets_path": {
              "response_count": "responses_total",
              "url_dc": "url_dc",
              "bytes_sum": "bytes_sum",
              "geo_src_dc": "geo_src_dc",
              "geo_dest_dc": "geo_dest_dc",
              "success": "success._count",
              "error404": "error404._count",
              "error503": "error503._count"
            }
          }
        }
      }
    }
  }
}
```

1. A composite bucket aggregation that aggregates the data by `client_ip`.
2. A series of metrics and bucket sub-aggregations.
3. {{infer-cap}} bucket aggregation that specifies the trained model and maps the aggregation names to the model’s input fields.



