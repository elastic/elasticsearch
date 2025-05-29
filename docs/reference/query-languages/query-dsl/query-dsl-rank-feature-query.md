---
navigation_title: "Rank feature"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-rank-feature-query.html
---

# Rank feature query [query-dsl-rank-feature-query]


Boosts the [relevance score](/reference/query-languages/query-dsl/query-filter-context.md#relevance-scores) of documents based on the numeric value of a [`rank_feature`](/reference/elasticsearch/mapping-reference/rank-feature.md) or [`rank_features`](/reference/elasticsearch/mapping-reference/rank-features.md) field.

The `rank_feature` query is typically used in the `should` clause of a [`bool`](/reference/query-languages/query-dsl/query-dsl-bool-query.md) query so its relevance scores are added to other scores from the `bool` query.

With `positive_score_impact` set to `false` for a `rank_feature` or `rank_features` field, we recommend that every document that participates in a query has a value for this field. Otherwise, if a `rank_feature` query is used in the should clause, it doesn’t add anything to a score of a document with a missing value, but adds some boost for a document containing a feature. This is contrary to what we want – as we consider these features negative, we want to rank documents containing them lower than documents missing them.

Unlike the [`function_score`](/reference/query-languages/query-dsl/query-dsl-function-score-query.md) query or other ways to change [relevance scores](/reference/query-languages/query-dsl/query-filter-context.md#relevance-scores), the `rank_feature` query efficiently skips non-competitive hits when the [`track_total_hits`](docs-content://solutions/search/the-search-api.md#track-total-hits) parameter is **not** `true`. This can dramatically improve query speed.

## Rank feature functions [rank-feature-query-functions]

To calculate relevance scores based on rank feature fields, the `rank_feature` query supports the following mathematical functions:

* [Saturation](#rank-feature-query-saturation)
* [Logarithm](#rank-feature-query-logarithm)
* [Sigmoid](#rank-feature-query-sigmoid)
* [Linear](#rank-feature-query-linear)

If you don’t know where to start, we recommend using the `saturation` function. If no function is provided, the `rank_feature` query uses the `saturation` function by default.


## Example request [rank-feature-query-ex-request]

### Index setup [rank-feature-query-index-setup]

To use the `rank_feature` query, your index must include a [`rank_feature`](/reference/elasticsearch/mapping-reference/rank-feature.md) or [`rank_features`](/reference/elasticsearch/mapping-reference/rank-features.md) field mapping. To see how you can set up an index for the `rank_feature` query, try the following example.

Create a `test` index with the following field mappings:

* `pagerank`, a [`rank_feature`](/reference/elasticsearch/mapping-reference/rank-feature.md) field which measures the importance of a website
* `url_length`, a [`rank_feature`](/reference/elasticsearch/mapping-reference/rank-feature.md) field which contains the length of the website’s URL. For this example, a long URL correlates negatively to relevance, indicated by a `positive_score_impact` value of `false`.
* `topics`, a [`rank_features`](/reference/elasticsearch/mapping-reference/rank-features.md) field which contains a list of topics and a measure of how well each document is connected to this topic

```console
PUT /test
{
  "mappings": {
    "properties": {
      "pagerank": {
        "type": "rank_feature"
      },
      "url_length": {
        "type": "rank_feature",
        "positive_score_impact": false
      },
      "topics": {
        "type": "rank_features"
      }
    }
  }
}
```
% TESTSETUP

Index several documents to the `test` index.

```console
PUT /test/_doc/1?refresh
{
  "url": "https://en.wikipedia.org/wiki/2016_Summer_Olympics",
  "content": "Rio 2016",
  "pagerank": 50.3,
  "url_length": 42,
  "topics": {
    "sports": 50,
    "brazil": 30
  }
}

PUT /test/_doc/2?refresh
{
  "url": "https://en.wikipedia.org/wiki/2016_Brazilian_Grand_Prix",
  "content": "Formula One motor race held on 13 November 2016",
  "pagerank": 50.3,
  "url_length": 47,
  "topics": {
    "sports": 35,
    "formula one": 65,
    "brazil": 20
  }
}

PUT /test/_doc/3?refresh
{
  "url": "https://en.wikipedia.org/wiki/Deadpool_(film)",
  "content": "Deadpool is a 2016 American superhero film",
  "pagerank": 50.3,
  "url_length": 37,
  "topics": {
    "movies": 60,
    "super hero": 65
  }
}
```


### Example query [rank-feature-query-ex-query]

The following query searches for `2016` and boosts relevance scores based on `pagerank`, `url_length`, and the `sports` topic.

```console
GET /test/_search
{
  "query": {
    "bool": {
      "must": [
        {
          "match": {
            "content": "2016"
          }
        }
      ],
      "should": [
        {
          "rank_feature": {
            "field": "pagerank"
          }
        },
        {
          "rank_feature": {
            "field": "url_length",
            "boost": 0.1
          }
        },
        {
          "rank_feature": {
            "field": "topics.sports",
            "boost": 0.4
          }
        }
      ]
    }
  }
}
```



## Top-level parameters for `rank_feature` [rank-feature-top-level-params]

`field`
:   (Required, string) [`rank_feature`](/reference/elasticsearch/mapping-reference/rank-feature.md) or [`rank_features`](/reference/elasticsearch/mapping-reference/rank-features.md) field used to boost [relevance scores](/reference/query-languages/query-dsl/query-filter-context.md#relevance-scores).

`boost`
:   (Optional, float) Floating point number used to decrease or increase [relevance scores](/reference/query-languages/query-dsl/query-filter-context.md#relevance-scores). Defaults to `1.0`.

Boost values are relative to the default value of `1.0`. A boost value between `0` and `1.0` decreases the relevance score. A value greater than `1.0` increases the relevance score.


`saturation`
:   (Optional, [function object](#rank-feature-query-saturation)) Saturation function used to boost [relevance scores](/reference/query-languages/query-dsl/query-filter-context.md#relevance-scores) based on the value of the rank feature `field`. If no function is provided, the `rank_feature` query defaults to the `saturation` function. See [Saturation](#rank-feature-query-saturation) for more information.

Only one function `saturation`, `log`, `sigmoid` or `linear` can be provided.


`log`
:   (Optional, [function object](#rank-feature-query-logarithm)) Logarithmic function used to boost [relevance scores](/reference/query-languages/query-dsl/query-filter-context.md#relevance-scores) based on the value of the rank feature `field`. See [Logarithm](#rank-feature-query-logarithm) for more information.

Only one function `saturation`, `log`, `sigmoid` or `linear` can be provided.


`sigmoid`
:   (Optional, [function object](#rank-feature-query-sigmoid)) Sigmoid function used to boost [relevance scores](/reference/query-languages/query-dsl/query-filter-context.md#relevance-scores) based on the value of the rank feature `field`. See [Sigmoid](#rank-feature-query-sigmoid) for more information.

Only one function `saturation`, `log`, `sigmoid` or `linear` can be provided.


`linear`
:   (Optional, [function object](#rank-feature-query-linear)) Linear function used to boost [relevance scores](/reference/query-languages/query-dsl/query-filter-context.md#relevance-scores) based on the value of the rank feature `field`. See [Linear](#rank-feature-query-linear) for more information.

Only one function `saturation`, `log`, `sigmoid` or `linear` can be provided.



## Notes [rank-feature-query-notes]

### Saturation [rank-feature-query-saturation]

The `saturation` function gives a score equal to `S / (S + pivot)`, where `S` is the value of the rank feature field and `pivot` is a configurable pivot value so that the result will be less than `0.5` if `S` is less than pivot and greater than `0.5` otherwise. Scores are always `(0,1)`.

If the rank feature has a negative score impact then the function will be computed as `pivot / (S + pivot)`, which decreases when `S` increases.

```console
GET /test/_search
{
  "query": {
    "rank_feature": {
      "field": "pagerank",
      "saturation": {
        "pivot": 8
      }
    }
  }
}
```

If a `pivot` value is not provided, {{es}} computes a default value equal to the approximate geometric mean of all rank feature values in the index. We recommend using this default value if you haven’t had the opportunity to train a good pivot value.

```console
GET /test/_search
{
  "query": {
    "rank_feature": {
      "field": "pagerank",
      "saturation": {}
    }
  }
}
```


### Logarithm [rank-feature-query-logarithm]

The `log` function gives a score equal to `log(scaling_factor + S)`, where `S` is the value of the rank feature field and `scaling_factor` is a configurable scaling factor. Scores are unbounded.

This function only supports rank features that have a positive score impact.

```console
GET /test/_search
{
  "query": {
    "rank_feature": {
      "field": "pagerank",
      "log": {
        "scaling_factor": 4
      }
    }
  }
}
```


### Sigmoid [rank-feature-query-sigmoid]

The `sigmoid` function is an extension of `saturation` which adds a configurable exponent. Scores are computed as `S^exp^ / (S^exp^ + pivot^exp^)`. Like for the `saturation` function, `pivot` is the value of `S` that gives a score of `0.5` and scores are `(0,1)`.

The `exponent` must be positive and is typically in `[0.5, 1]`. A good value should be computed via training. If you don’t have the opportunity to do so, we recommend you use the `saturation` function instead.

```console
GET /test/_search
{
  "query": {
    "rank_feature": {
      "field": "pagerank",
      "sigmoid": {
        "pivot": 7,
        "exponent": 0.6
      }
    }
  }
}
```


### Linear [rank-feature-query-linear]

The `linear` function is the simplest function, and gives a score equal to the indexed value of `S`, where `S` is the value of the rank feature field. If a rank feature field is indexed with `"positive_score_impact": true`, its indexed value is equal to `S` and rounded to preserve only 9 significant bits for the precision. If a rank feature field is indexed with `"positive_score_impact": false`, its indexed value is equal to `1/S` and rounded to preserve only 9 significant bits for the precision.

```console
GET /test/_search
{
  "query": {
    "rank_feature": {
      "field": "pagerank",
      "linear": {}
    }
  }
}
```
