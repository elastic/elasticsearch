---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-similarity.html
navigation_title: Similarity
---

# Similarity settings [index-modules-similarity]

A similarity (scoring / ranking model) defines how matching documents are scored. Similarity is per field, meaning that via the mapping one can define a different similarity per field.

Similarity is only applicable for text type and keyword type fields.

Configuring a custom similarity is considered an expert feature and the builtin similarities are most likely sufficient as is described in [`similarity`](/reference/elasticsearch/mapping-reference/similarity.md).


## Configuring a similarity [configuration]

Most existing or custom Similarities have configuration options which can be configured via the index settings as shown below. The index options can be provided when creating an index or updating index settings.

```console
PUT /index
{
  "settings": {
    "index": {
      "similarity": {
        "my_similarity": {
          "type": "DFR",
          "basic_model": "g",
          "after_effect": "l",
          "normalization": "h2",
          "normalization.h2.c": "3.0"
        }
      }
    }
  }
}
```

Here we configure the DFR similarity so it can be referenced as `my_similarity` in mappings as is illustrate in the below example:

```console
PUT /index/_mapping
{
  "properties" : {
    "title" : { "type" : "text", "similarity" : "my_similarity" }
  }
}
```


## Available similarities [_available_similarities]


### BM25 similarity (**default**) [bm25]

TF/IDF based similarity that has built-in tf normalization and is supposed to work better for short fields (like names). See [Okapi_BM25](https://en.wikipedia.org/wiki/Okapi_BM25) for more details. This similarity has the following options:

`k1`
:   Controls non-linear term frequency normalization (saturation). The default value is `1.2`.

`b`
:   Controls to what degree document length normalizes tf values. The default value is `0.75`.

`discount_overlaps`
:   Determines whether overlap tokens (Tokens with 0 position increment) are ignored when computing norm. By default this is true, meaning overlap tokens do not count when computing norms.

Type name: `BM25`


### DFR similarity [dfr]

Similarity that implements the [divergence from randomness](https://lucene.apache.org/core/10_0_0/core/org/apache/lucene/search/similarities/DFRSimilarity.md) framework. This similarity has the following options:

`basic_model`
:   Possible values: [`g`](https://lucene.apache.org/core/10_0_0/core/org/apache/lucene/search/similarities/BasicModelG.md), [`if`](https://lucene.apache.org/core/10_0_0/core/org/apache/lucene/search/similarities/BasicModelIF.md), [`in`](https://lucene.apache.org/core/10_0_0/core/org/apache/lucene/search/similarities/BasicModelIn.md) and [`ine`](https://lucene.apache.org/core/10_0_0/core/org/apache/lucene/search/similarities/BasicModelIne.md).

`after_effect`
:   Possible values: [`b`](https://lucene.apache.org/core/10_0_0/core/org/apache/lucene/search/similarities/AfterEffectB.md) and [`l`](https://lucene.apache.org/core/10_0_0/core/org/apache/lucene/search/similarities/AfterEffectL.md).

`normalization`
:   Possible values: [`no`](https://lucene.apache.org/core/10_0_0/core/org/apache/lucene/search/similarities/Normalization.NoNormalization.md), [`h1`](https://lucene.apache.org/core/10_0_0/core/org/apache/lucene/search/similarities/NormalizationH1.md), [`h2`](https://lucene.apache.org/core/10_0_0/core/org/apache/lucene/search/similarities/NormalizationH2.md), [`h3`](https://lucene.apache.org/core/10_0_0/core/org/apache/lucene/search/similarities/NormalizationH3.md) and [`z`](https://lucene.apache.org/core/10_0_0/core/org/apache/lucene/search/similarities/NormalizationZ.md).

All options but the first option need a normalization value.

Type name: `DFR`


### DFI similarity [dfi]

Similarity that implements the [divergence from independence](https://trec.nist.gov/pubs/trec21/papers/irra.web.nb.pdf) model. This similarity has the following options:

`independence_measure`
:   Possible values [`standardized`](https://lucene.apache.org/core/10_0_0/core/org/apache/lucene/search/similarities/IndependenceStandardized.md), [`saturated`](https://lucene.apache.org/core/10_0_0/core/org/apache/lucene/search/similarities/IndependenceSaturated.md), [`chisquared`](https://lucene.apache.org/core/10_0_0/core/org/apache/lucene/search/similarities/IndependenceChiSquared.md).

When using this similarity, it is highly recommended **not** to remove stop words to get good relevance. Also beware that terms whose frequency is less than the expected frequency will get a score equal to 0.

Type name: `DFI`


### IB similarity. [ib]

[Information based model](https://lucene.apache.org/core/10_0_0/core/org/apache/lucene/search/similarities/IBSimilarity.md) . The algorithm is based on the concept that the information content in any symbolic *distribution* sequence is primarily determined by the repetitive usage of its basic elements. For written texts this challenge would correspond to comparing the writing styles of different authors. This similarity has the following options:

`distribution`
:   Possible values: [`ll`](https://lucene.apache.org/core/10_0_0/core/org/apache/lucene/search/similarities/DistributionLL.md) and [`spl`](https://lucene.apache.org/core/10_0_0/core/org/apache/lucene/search/similarities/DistributionSPL.md).

`lambda`
:   Possible values: [`df`](https://lucene.apache.org/core/10_0_0/core/org/apache/lucene/search/similarities/LambdaDF.md) and [`ttf`](https://lucene.apache.org/core/10_0_0/core/org/apache/lucene/search/similarities/LambdaTTF.md).

`normalization`
:   Same as in `DFR` similarity.

Type name: `IB`


### LM Dirichlet similarity. [lm_dirichlet]

[LM Dirichlet similarity](https://lucene.apache.org/core/10_0_0/core/org/apache/lucene/search/similarities/LMDirichletSimilarity.md) . This similarity has the following options:

`mu`
:   Default to `2000`.

The scoring formula in the paper assigns negative scores to terms that have fewer occurrences than predicted by the language model, which is illegal to Lucene, so such terms get a score of 0.

Type name: `LMDirichlet`


### LM Jelinek Mercer similarity. [lm_jelinek_mercer]

[LM Jelinek Mercer similarity](https://lucene.apache.org/core/10_0_0/core/org/apache/lucene/search/similarities/LMJelinekMercerSimilarity.md) . The algorithm attempts to capture important patterns in the text, while leaving out noise. This similarity has the following options:

`lambda`
:   The optimal value depends on both the collection and the query. The optimal value is around `0.1` for title queries and `0.7` for long queries. Default to `0.1`. When value approaches `0`, documents that match more query terms will be ranked higher than those that match fewer terms.

Type name: `LMJelinekMercer`


### Scripted similarity [scripted_similarity]

A similarity that allows you to use a script in order to specify how scores should be computed. For instance, the below example shows how to reimplement TF-IDF:

```console
PUT /index
{
  "settings": {
    "number_of_shards": 1,
    "similarity": {
      "scripted_tfidf": {
        "type": "scripted",
        "script": {
          "source": "double tf = Math.sqrt(doc.freq); double idf = Math.log((field.docCount+1.0)/(term.docFreq+1.0)) + 1.0; double norm = 1/Math.sqrt(doc.length); return query.boost * tf * idf * norm;"
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "field": {
        "type": "text",
        "similarity": "scripted_tfidf"
      }
    }
  }
}

PUT /index/_doc/1
{
  "field": "foo bar foo"
}

PUT /index/_doc/2
{
  "field": "bar baz"
}

POST /index/_refresh

GET /index/_search?explain=true
{
  "query": {
    "query_string": {
      "query": "foo^1.7",
      "default_field": "field"
    }
  }
}
```

Which yields:

```console-result
{
  "took": 12,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": {
        "value": 1,
        "relation": "eq"
    },
    "max_score": 1.9508477,
    "hits": [
      {
        "_shard": "[index][0]",
        "_node": "OzrdjxNtQGaqs4DmioFw9A",
        "_index": "index",
        "_id": "1",
        "_score": 1.9508477,
        "_source": {
          "field": "foo bar foo"
        },
        "_explanation": {
          "value": 1.9508477,
          "description": "weight(field:foo in 0) [PerFieldSimilarity], result of:",
          "details": [
            {
              "value": 1.9508477,
              "description": "score from ScriptedSimilarity(weightScript=[null], script=[Script{type=inline, lang='painless', idOrCode='double tf = Math.sqrt(doc.freq); double idf = Math.log((field.docCount+1.0)/(term.docFreq+1.0)) + 1.0; double norm = 1/Math.sqrt(doc.length); return query.boost * tf * idf * norm;', options={}, params={}}]) computed from:",
              "details": [
                {
                  "value": 1.0,
                  "description": "weight",
                  "details": []
                },
                {
                  "value": 1.7,
                  "description": "query.boost",
                  "details": []
                },
                {
                  "value": 2,
                  "description": "field.docCount",
                  "details": []
                },
                {
                  "value": 4,
                  "description": "field.sumDocFreq",
                  "details": []
                },
                {
                  "value": 5,
                  "description": "field.sumTotalTermFreq",
                  "details": []
                },
                {
                  "value": 1,
                  "description": "term.docFreq",
                  "details": []
                },
                {
                  "value": 2,
                  "description": "term.totalTermFreq",
                  "details": []
                },
                {
                  "value": 2.0,
                  "description": "doc.freq",
                  "details": []
                },
                {
                  "value": 3,
                  "description": "doc.length",
                  "details": []
                }
              ]
            }
          ]
        }
      }
    ]
  }
}
```

::::{warning}
While scripted similarities provide a lot of flexibility, there is a set of rules that they need to satisfy. Failing to do so could make Elasticsearch silently return wrong top hits or fail with internal errors at search time:
::::


* Returned scores must be positive.
* All other variables remaining equal, scores must not decrease when `doc.freq` increases.
* All other variables remaining equal, scores must not increase when `doc.length` increases.

You might have noticed that a significant part of the above script depends on statistics that are the same for every document. It is possible to make the above slightly more efficient by providing an `weight_script` which will compute the document-independent part of the score and will be available under the `weight` variable. When no `weight_script` is provided, `weight` is equal to `1`. The `weight_script` has access to the same variables as the `script` except `doc` since it is supposed to compute a document-independent contribution to the score.

The below configuration will give the same tf-idf scores but is slightly more efficient:

```console
PUT /index
{
  "settings": {
    "number_of_shards": 1,
    "similarity": {
      "scripted_tfidf": {
        "type": "scripted",
        "weight_script": {
          "source": "double idf = Math.log((field.docCount+1.0)/(term.docFreq+1.0)) + 1.0; return query.boost * idf;"
        },
        "script": {
          "source": "double tf = Math.sqrt(doc.freq); double norm = 1/Math.sqrt(doc.length); return weight * tf * norm;"
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "field": {
        "type": "text",
        "similarity": "scripted_tfidf"
      }
    }
  }
}
```

Type name: `scripted`


### Default Similarity [default-base]

By default, Elasticsearch will use whatever similarity is configured as `default`.

You can change the default similarity for all fields in an index when it is [created](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create):

```console
PUT /index
{
  "settings": {
    "index": {
      "similarity": {
        "default": {
          "type": "boolean"
        }
      }
    }
  }
}
```

If you want to change the default similarity after creating the index you must [close](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-open) your index, send the following request and [open](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-open) it again afterwards:

```console
POST /index/_close

PUT /index/_settings
{
  "index": {
    "similarity": {
      "default": {
        "type": "boolean"
      }
    }
  }
}

POST /index/_open
```

