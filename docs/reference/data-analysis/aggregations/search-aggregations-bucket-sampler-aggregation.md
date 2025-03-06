---
navigation_title: "Sampler"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-sampler-aggregation.html
---

# Sampler aggregation [search-aggregations-bucket-sampler-aggregation]


A filtering aggregation used to limit any sub aggregations' processing to a sample of the top-scoring documents.

* Tightening the focus of analytics to high-relevance matches rather than the potentially very long tail of low-quality matches
* Reducing the running cost of aggregations that can produce useful results using only samples e.g. `significant_terms`

Example:

A query on StackOverflow data for the popular term `javascript` OR the rarer term `kibana` will match many documents - most of them missing the word Kibana. To focus the `significant_terms` aggregation on top-scoring documents that are more likely to match the most interesting parts of our query we use a sample.

$$$sampler-aggregation-example$$$

```console
POST /stackoverflow/_search?size=0
{
  "query": {
    "query_string": {
      "query": "tags:kibana OR tags:javascript"
    }
  },
  "aggs": {
    "sample": {
      "sampler": {
        "shard_size": 200
      },
      "aggs": {
        "keywords": {
          "significant_terms": {
            "field": "tags",
            "exclude": [ "kibana", "javascript" ]
          }
        }
      }
    }
  }
}
```

Response:

```console-result
{
  ...
  "aggregations": {
    "sample": {
      "doc_count": 200, <1>
      "keywords": {
        "doc_count": 200,
        "bg_count": 650,
        "buckets": [
          {
            "key": "elasticsearch",
            "doc_count": 150,
            "score": 1.078125,
            "bg_count": 200
          },
          {
            "key": "logstash",
            "doc_count": 50,
            "score": 0.5625,
            "bg_count": 50
          }
        ]
      }
    }
  }
}
```

1. 200 documents were sampled in total. The cost of performing the nested significant_terms aggregation was therefore limited rather than unbounded.


Without the `sampler` aggregation the request query considers the full "long tail" of low-quality matches and therefore identifies less significant terms such as `jquery` and `angular` rather than focusing on the more insightful Kibana-related terms.

$$$sampler-aggregation-no-sampler-example$$$

```console
POST /stackoverflow/_search?size=0
{
  "query": {
    "query_string": {
      "query": "tags:kibana OR tags:javascript"
    }
  },
  "aggs": {
    "low_quality_keywords": {
      "significant_terms": {
        "field": "tags",
        "size": 3,
        "exclude": [ "kibana", "javascript" ]
      }
    }
  }
}
```

Response:

```console-result
{
  ...
  "aggregations": {
    "low_quality_keywords": {
      "doc_count": 600,
      "bg_count": 650,
      "buckets": [
        {
          "key": "angular",
          "doc_count": 200,
          "score": 0.02777,
          "bg_count": 200
        },
        {
          "key": "jquery",
          "doc_count": 200,
          "score": 0.02777,
          "bg_count": 200
        },
        {
          "key": "logstash",
          "doc_count": 50,
          "score": 0.0069,
          "bg_count": 50
        }
      ]
    }
  }
}
```

## shard_size [_shard_size_2]

The `shard_size` parameter limits how many top-scoring documents are collected in the sample processed on each shard. The default value is 100.


## Limitations [_limitations_7]

### Cannot be nested under `breadth_first` aggregations [sampler-breadth-first-nested-agg]

Being a quality-based filter the sampler aggregation needs access to the relevance score produced for each document. It therefore cannot be nested under a `terms` aggregation which has the `collect_mode` switched from the default `depth_first` mode to `breadth_first` as this discards scores. In this situation an error will be thrown.



