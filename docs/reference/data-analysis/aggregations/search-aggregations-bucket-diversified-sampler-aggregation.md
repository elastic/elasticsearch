---
navigation_title: "Diversified sampler"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-diversified-sampler-aggregation.html
---

# Diversified sampler aggregation [search-aggregations-bucket-diversified-sampler-aggregation]


Like the `sampler` aggregation this is a filtering aggregation used to limit any sub aggregations' processing to a sample of the top-scoring documents. The `diversified_sampler` aggregation adds the ability to limit the number of matches that share a common value such as an "author".

::::{note}
Any good market researcher will tell you that when working with samples of data it is important that the sample represents a healthy variety of opinions rather than being skewed by any single voice. The same is true with aggregations and sampling with these diversify settings can offer a way to remove the bias in your content (an over-populated geography, a large spike in a timeline or an over-active forum spammer).
::::


* Tightening the focus of analytics to high-relevance matches rather than the potentially very long tail of low-quality matches
* Removing bias from analytics by ensuring fair representation of content from different sources
* Reducing the running cost of aggregations that can produce useful results using only samples e.g. `significant_terms`

The `field` setting is used to provide values used for de-duplication and the `max_docs_per_value` setting controls the maximum number of documents collected on any one shard which share a common value. The default setting for `max_docs_per_value` is 1.

The aggregation will throw an error if the `field` produces multiple values for a single document (de-duplication using multi-valued fields is not supported due to efficiency concerns).

Example:

We might want to see which tags are strongly associated with `#elasticsearch` on StackOverflow forum posts but ignoring the effects of some prolific users with a tendency to misspell #Kibana as #Cabana.

$$$diversified-sampler-aggregation-example$$$

```console
POST /stackoverflow/_search?size=0
{
  "query": {
    "query_string": {
      "query": "tags:elasticsearch"
    }
  },
  "aggs": {
    "my_unbiased_sample": {
      "diversified_sampler": {
        "shard_size": 200,
        "field": "author"
      },
      "aggs": {
        "keywords": {
          "significant_terms": {
            "field": "tags",
            "exclude": [ "elasticsearch" ]
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
    "my_unbiased_sample": {
      "doc_count": 151,           <1>
      "keywords": {               <2>
        "doc_count": 151,
        "bg_count": 650,
        "buckets": [
          {
            "key": "kibana",
            "doc_count": 150,
            "score": 2.213,
            "bg_count": 200
          }
        ]
      }
    }
  }
}
```

1. 151 documents were sampled in total.
2. The results of the significant_terms aggregation are not skewed by any single author’s quirks because we asked for a maximum of one post from any one author in our sample.


## Scripted example [_scripted_example]

In this scenario we might want to diversify on a combination of field values. We can use a [runtime field](docs-content://manage-data/data-store/mapping/runtime-fields.md) to produce a hash of the multiple values in a tags field to ensure we don’t have a sample that consists of the same repeated combinations of tags.

$$$diversified-sampler-aggregation-runtime-field-example$$$

```console
POST /stackoverflow/_search?size=0
{
  "query": {
    "query_string": {
      "query": "tags:kibana"
    }
  },
  "runtime_mappings": {
    "tags.hash": {
      "type": "long",
      "script": "emit(doc['tags'].hashCode())"
    }
  },
  "aggs": {
    "my_unbiased_sample": {
      "diversified_sampler": {
        "shard_size": 200,
        "max_docs_per_value": 3,
        "field": "tags.hash"
      },
      "aggs": {
        "keywords": {
          "significant_terms": {
            "field": "tags",
            "exclude": [ "kibana" ]
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
    "my_unbiased_sample": {
      "doc_count": 6,
      "keywords": {
        "doc_count": 6,
        "bg_count": 650,
        "buckets": [
          {
            "key": "logstash",
            "doc_count": 3,
            "score": 2.213,
            "bg_count": 50
          },
          {
            "key": "elasticsearch",
            "doc_count": 3,
            "score": 1.34,
            "bg_count": 200
          }
        ]
      }
    }
  }
}
```


## shard_size [_shard_size]

The `shard_size` parameter limits how many top-scoring documents are collected in the sample processed on each shard. The default value is 100.


## max_docs_per_value [_max_docs_per_value]

The `max_docs_per_value` is an optional parameter and limits how many documents are permitted per choice of de-duplicating value. The default setting is "1".


## execution_hint [_execution_hint]

The optional `execution_hint` setting can influence the management of the values used for de-duplication. Each option will hold up to `shard_size` values in memory while performing de-duplication but the type of value held can be controlled as follows:

* hold field values directly (`map`)
* hold ordinals of the field as determined by the Lucene index (`global_ordinals`)
* hold hashes of the field values - with potential for hash collisions (`bytes_hash`)

The default setting is to use [`global_ordinals`](/reference/elasticsearch/mapping-reference/eager-global-ordinals.md) if this information is available from the Lucene index and reverting to `map` if not. The `bytes_hash` setting may prove faster in some cases but introduces the possibility of false positives in de-duplication logic due to the possibility of hash collisions. Please note that Elasticsearch will ignore the choice of execution hint if it is not applicable and that there is no backward compatibility guarantee on these hints.


## Limitations [_limitations_6]

### Cannot be nested under `breadth_first` aggregations [div-sampler-breadth-first-nested-agg]

Being a quality-based filter the diversified_sampler aggregation needs access to the relevance score produced for each document. It therefore cannot be nested under a `terms` aggregation which has the `collect_mode` switched from the default `depth_first` mode to `breadth_first` as this discards scores. In this situation an error will be thrown.


### Limited de-dup logic. [_limited_de_dup_logic]

The de-duplication logic applies only at a shard level so will not apply across shards.


### No specialized syntax for geo/date fields [spec-syntax-geo-date-fields]

Currently the syntax for defining the diversifying values is defined by a choice of `field` or `script` - there is no added syntactical sugar for expressing geo or date units such as "7d" (7 days). This support may be added in a later release and users will currently have to create these sorts of values using a script.



