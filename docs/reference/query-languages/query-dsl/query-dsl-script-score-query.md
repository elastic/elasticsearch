---
navigation_title: "Script score"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-script-score-query.html
---

# Script score query [query-dsl-script-score-query]


Uses a [script](docs-content://explore-analyze/scripting.md) to provide a custom score for returned documents.

The `script_score` query is useful if, for example, a scoring function is expensive and you only need to calculate the score of a filtered set of documents.

## Example request [script-score-query-ex-request]

The following `script_score` query assigns each returned document a score equal to the `my-int` field value divided by `10`.

```console
GET /_search
{
  "query": {
    "script_score": {
      "query": {
        "match": { "message": "elasticsearch" }
      },
      "script": {
        "source": "doc['my-int'].value / 10 "
      }
    }
  }
}
```


## Top-level parameters for `script_score` [script-score-top-level-params]

`query`
:   (Required, query object) Query used to return documents.

`script`
:   (Required, [script object](docs-content://explore-analyze/scripting/modules-scripting-using.md)) Script used to compute the score of documents returned by the `query`.

::::{important}
Final relevance scores from the `script_score` query cannot be negative. To support certain search optimizations, Lucene requires scores be positive or `0`.
::::



`min_score`
:   (Optional, float) Documents with a score lower than this floating point number are excluded from search results and results collected by aggregations.

`boost`
:   (Optional, float) Documents' scores produced by `script` are multiplied by `boost` to produce final documents' scores. Defaults to `1.0`.


## Notes [script-score-query-notes]

### Use relevance scores in a script [script-score-access-scores]

Within a script, you can [access](docs-content://explore-analyze/scripting/modules-scripting-fields.md#scripting-score) the `_score` variable which represents the current relevance score of a document.


### Use term statistics in a script [script-score-access-term-statistics]

Within a script, you can [access](docs-content://explore-analyze/scripting/modules-scripting-fields.md#scripting-term-statistics) the `_termStats` variable which provides statistical information about the terms used in the child query of the `script_score` query.


### Predefined functions [script-score-predefined-functions]

You can use any of the available [painless functions](/reference/scripting-languages/painless/painless-contexts.md) in your `script`. You can also use the following predefined functions to customize scoring:

* [Saturation](#script-score-saturation)
* [Sigmoid](#script-score-sigmoid)
* [Random score function](#random-score-function)
* [Decay functions for numeric fields](#decay-functions-numeric-fields)
* [Decay functions for geo fields](#decay-functions-geo-fields)
* [Decay functions for date fields](#decay-functions-date-fields)
* [Functions for vector fields](#script-score-functions-vector-fields)

We suggest using these predefined functions instead of writing your own. These functions take advantage of efficiencies from {{es}}' internal mechanisms.

#### Saturation [script-score-saturation]

`saturation(value,k) = value/(k + value)`

```js
"script" : {
    "source" : "saturation(doc['my-int'].value, 1)"
}
```
% NOTCONSOLE


#### Sigmoid [script-score-sigmoid]

`sigmoid(value, k, a) = value^a/ (k^a + value^a)`

```js
"script" : {
    "source" : "sigmoid(doc['my-int'].value, 2, 1)"
}
```
% NOTCONSOLE


#### Random score function [random-score-function]

`random_score` function generates scores that are uniformly distributed from 0 up to but not including 1.

`randomScore` function has the following syntax: `randomScore(<seed>, <fieldName>)`. It has a required parameter - `seed` as an integer value, and an optional parameter - `fieldName` as a string value.

```js
"script" : {
    "source" : "randomScore(100, '_seq_no')"
}
```
% NOTCONSOLE

If the `fieldName` parameter is omitted, the internal Lucene document ids will be used as a source of randomness. This is very efficient, but unfortunately not reproducible since documents might be renumbered by merges.

```js
"script" : {
    "source" : "randomScore(100)"
}
```
% NOTCONSOLE

Note that documents that are within the same shard and have the same value for field will get the same score, so it is usually desirable to use a field that has unique values for all documents across a shard. A good default choice might be to use the `_seq_no` field, whose only drawback is that scores will change if the document is updated since update operations also update the value of the `_seq_no` field.


#### Decay functions for numeric fields [decay-functions-numeric-fields]

You can read more about decay functions [here](/reference/query-languages/query-dsl/query-dsl-function-score-query.md#function-decay).

* `double decayNumericLinear(double origin, double scale, double offset, double decay, double docValue)`
* `double decayNumericExp(double origin, double scale, double offset, double decay, double docValue)`
* `double decayNumericGauss(double origin, double scale, double offset, double decay, double docValue)`

```js
"script" : {
    "source" : "decayNumericLinear(params.origin, params.scale, params.offset, params.decay, doc['dval'].value)",
    "params": { <1>
        "origin": 20,
        "scale": 10,
        "decay" : 0.5,
        "offset" : 0
    }
}
```
% NOTCONSOLE

1. Using `params` allows to compile the script only once, even if params change.



#### Decay functions for geo fields [decay-functions-geo-fields]

* `double decayGeoLinear(String originStr, String scaleStr, String offsetStr, double decay, GeoPoint docValue)`
* `double decayGeoExp(String originStr, String scaleStr, String offsetStr, double decay, GeoPoint docValue)`
* `double decayGeoGauss(String originStr, String scaleStr, String offsetStr, double decay, GeoPoint docValue)`

```js
"script" : {
    "source" : "decayGeoExp(params.origin, params.scale, params.offset, params.decay, doc['location'].value)",
    "params": {
        "origin": "40, -70.12",
        "scale": "200km",
        "offset": "0km",
        "decay" : 0.2
    }
}
```
% NOTCONSOLE


#### Decay functions for date fields [decay-functions-date-fields]

* `double decayDateLinear(String originStr, String scaleStr, String offsetStr, double decay, JodaCompatibleZonedDateTime docValueDate)`
* `double decayDateExp(String originStr, String scaleStr, String offsetStr, double decay, JodaCompatibleZonedDateTime docValueDate)`
* `double decayDateGauss(String originStr, String scaleStr, String offsetStr, double decay, JodaCompatibleZonedDateTime docValueDate)`

```js
"script" : {
    "source" : "decayDateGauss(params.origin, params.scale, params.offset, params.decay, doc['date'].value)",
    "params": {
        "origin": "2008-01-01T01:00:00Z",
        "scale": "1h",
        "offset" : "0",
        "decay" : 0.5
    }
}
```
% NOTCONSOLE

::::{note}
Decay functions on dates are limited to dates in the default format and default time zone. Also calculations with `now` are not supported.
::::



#### Functions for vector fields [script-score-functions-vector-fields]

[Functions for vector fields](#vector-functions) are accessible through `script_score` query.



### Allow expensive queries [_allow_expensive_queries_5]

Script score queries will not be executed if [`search.allow_expensive_queries`](/reference/query-languages/querydsl.md#query-dsl-allow-expensive-queries) is set to false.


### Faster alternatives [script-score-faster-alt]

The `script_score` query calculates the score for every matching document, or hit. There are faster alternative query types that can efficiently skip non-competitive hits:

* If you want to boost documents on some static fields, use the [`rank_feature`](/reference/query-languages/query-dsl/query-dsl-rank-feature-query.md) query.
* If you want to boost documents closer to a date or geographic point, use the [`distance_feature`](/reference/query-languages/query-dsl/query-dsl-distance-feature-query.md) query.


### Transition from the function score query [script-score-function-score-transition]

We recommend using the `script_score` query instead of [`function_score`](/reference/query-languages/query-dsl/query-dsl-function-score-query.md) query for the simplicity of the `script_score` query.

You can implement the following functions of the `function_score` query using the `script_score` query:

* [`script_score`](#script-score)
* [`weight`](#weight)
* [`random_score`](#random-score)
* [`field_value_factor`](#field-value-factor)
* [`decay` functions](#decay-functions)

#### `script_score` [script-score]

What you used in `script_score` of the Function Score query, you can copy into the Script Score query. No changes here.


#### `weight` [weight]

`weight` function can be implemented in the Script Score query through the following script:

```js
"script" : {
    "source" : "params.weight * _score",
    "params": {
        "weight": 2
    }
}
```
% NOTCONSOLE


#### `random_score` [random-score]

Use `randomScore` function as described in [random score function](#random-score-function).


#### `field_value_factor` [field-value-factor]

`field_value_factor` function can be easily implemented through script:

```js
"script" : {
    "source" : "Math.log10(doc['field'].value * params.factor)",
    "params" : {
        "factor" : 5
    }
}
```
% NOTCONSOLE

For checking if a document has a missing value, you can use `doc['field'].size() == 0`. For example, this script will use a value `1` if a document doesn’t have a field `field`:

```js
"script" : {
    "source" : "Math.log10((doc['field'].size() == 0 ? 1 : doc['field'].value()) * params.factor)",
    "params" : {
        "factor" : 5
    }
}
```
% NOTCONSOLE

This table lists how `field_value_factor` modifiers can be implemented through a script:

| Modifier | Implementation in Script Score |
| --- | --- |
| `none` | - |
| `log` | `Math.log10(doc['f'].value)` |
| `log1p` | `Math.log10(doc['f'].value + 1)` |
| `log2p` | `Math.log10(doc['f'].value + 2)` |
| `ln` | `Math.log(doc['f'].value)` |
| `ln1p` | `Math.log(doc['f'].value + 1)` |
| `ln2p` | `Math.log(doc['f'].value + 2)` |
| `square` | `Math.pow(doc['f'].value, 2)` |
| `sqrt` | `Math.sqrt(doc['f'].value)` |
| `reciprocal` | `1.0 / doc['f'].value` |


#### `decay` functions [decay-functions]

The `script_score` query has equivalent [decay functions](#decay-functions-numeric-fields) that can be used in scripts.



### Functions for vector fields [vector-functions]

::::{note}
During vector functions' calculation, all matched documents are linearly scanned. Thus, expect the query time grow linearly with the number of matched documents. For this reason, we recommend to limit the number of matched documents with a `query` parameter.
::::


This is the list of available vector functions and vector access methods:

1. [`cosineSimilarity`](#vector-functions-cosine) – calculates cosine similarity
2. [`dotProduct`](#vector-functions-dot-product) – calculates dot product
3. [`l1norm`](#vector-functions-l1) – calculates L1 distance
4. [`hamming`](#vector-functions-hamming) – calculates Hamming distance
5. [`l2norm`](#vector-functions-l2) - calculates L2 distance
6. [`doc[<field>].vectorValue`](#vector-functions-accessing-vectors) – returns a vector’s value as an array of floats
7. [`doc[<field>].magnitude`](#vector-functions-accessing-vectors) – returns a vector’s magnitude

::::{note}
The `cosineSimilarity` function is not supported for `bit` vectors.
::::


::::{note}
The recommended way to access dense vectors is through the `cosineSimilarity`, `dotProduct`, `l1norm` or `l2norm` functions. Please note however, that you should call these functions only once per script. For example, don’t use these functions in a loop to calculate the similarity between a document vector and multiple other vectors. If you need that functionality, reimplement these functions yourself by [accessing vector values directly](#vector-functions-accessing-vectors).
::::


Let’s create an index with a `dense_vector` mapping and index a couple of documents into it.

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "my_dense_vector": {
        "type": "dense_vector",
        "index": false,
        "dims": 3
      },
      "my_byte_dense_vector": {
        "type": "dense_vector",
        "index": false,
        "dims": 3,
        "element_type": "byte"
      },
      "status" : {
        "type" : "keyword"
      }
    }
  }
}

PUT my-index-000001/_doc/1
{
  "my_dense_vector": [0.5, 10, 6],
  "my_byte_dense_vector": [0, 10, 6],
  "status" : "published"
}

PUT my-index-000001/_doc/2
{
  "my_dense_vector": [-0.5, 10, 10],
  "my_byte_dense_vector": [0, 10, 10],
  "status" : "published"
}

POST my-index-000001/_refresh
```
% TESTSETUP

#### Cosine similarity [vector-functions-cosine]

The `cosineSimilarity` function calculates the measure of cosine similarity between a given query vector and document vectors.

```console
GET my-index-000001/_search
{
  "query": {
    "script_score": {
      "query" : {
        "bool" : {
          "filter" : {
            "term" : {
              "status" : "published" <1>
            }
          }
        }
      },
      "script": {
        "source": "cosineSimilarity(params.query_vector, 'my_dense_vector') + 1.0", <2>
        "params": {
          "query_vector": [4, 3.4, -0.2]  <3>
        }
      }
    }
  }
}
```

1. To restrict the number of documents on which script score calculation is applied, provide a filter.
2. The script adds 1.0 to the cosine similarity to prevent the score from being negative.
3. To take advantage of the script optimizations, provide a query vector as a script parameter.


::::{note}
If a document’s dense vector field has a number of dimensions different from the query’s vector, an error will be thrown.
::::



#### Dot product [vector-functions-dot-product]

The `dotProduct` function calculates the measure of dot product between a given query vector and document vectors.

```console
GET my-index-000001/_search
{
  "query": {
    "script_score": {
      "query" : {
        "bool" : {
          "filter" : {
            "term" : {
              "status" : "published"
            }
          }
        }
      },
      "script": {
        "source": """
          double value = dotProduct(params.query_vector, 'my_dense_vector');
          return sigmoid(1, Math.E, -value); <1>
        """,
        "params": {
          "query_vector": [4, 3.4, -0.2]
        }
      }
    }
  }
}
```

1. Using the standard sigmoid function prevents scores from being negative.



#### L1 distance (Manhattan distance) [vector-functions-l1]

The `l1norm` function calculates L1 distance (Manhattan distance) between a given query vector and document vectors.

```console
GET my-index-000001/_search
{
  "query": {
    "script_score": {
      "query" : {
        "bool" : {
          "filter" : {
            "term" : {
              "status" : "published"
            }
          }
        }
      },
      "script": {
        "source": "1 / (1 + l1norm(params.queryVector, 'my_dense_vector'))", <1>
        "params": {
          "queryVector": [4, 3.4, -0.2]
        }
      }
    }
  }
}
```

1. Unlike `cosineSimilarity` that represent similarity, `l1norm` and `l2norm` shown below represent distances or differences. This means, that the more similar the vectors are, the lower the scores will be that are produced by the `l1norm` and `l2norm` functions. Thus, as we need more similar vectors to score higher, we reversed the output from `l1norm` and `l2norm`. Also, to avoid division by 0 when a document vector matches the query exactly, we added `1` in the denominator.



#### Hamming distance [vector-functions-hamming]

The `hamming` function calculates [Hamming distance](https://en.wikipedia.org/wiki/Hamming_distance) between a given query vector and document vectors. It is only available for byte and bit vectors.

```console
GET my-index-000001/_search
{
  "query": {
    "script_score": {
      "query" : {
        "bool" : {
          "filter" : {
            "term" : {
              "status" : "published"
            }
          }
        }
      },
      "script": {
        "source": "(24 - hamming(params.queryVector, 'my_byte_dense_vector')) / 24", <1>
        "params": {
          "queryVector": [4, 3, 0]
        }
      }
    }
  }
}
```

1. Calculate the Hamming distance and normalize it by the bits to get a score between 0 and 1.



#### L2 distance (Euclidean distance) [vector-functions-l2]

The `l2norm` function calculates L2 distance (Euclidean distance) between a given query vector and document vectors.

```console
GET my-index-000001/_search
{
  "query": {
    "script_score": {
      "query" : {
        "bool" : {
          "filter" : {
            "term" : {
              "status" : "published"
            }
          }
        }
      },
      "script": {
        "source": "1 / (1 + l2norm(params.queryVector, 'my_dense_vector'))",
        "params": {
          "queryVector": [4, 3.4, -0.2]
        }
      }
    }
  }
}
```


#### Checking for missing values [vector-functions-missing-values]

If a document doesn’t have a value for a vector field on which a vector function is executed, an error will be thrown.

You can check if a document has a value for the field `my_vector` with `doc['my_vector'].size() == 0`. Your overall script can look like this:

```js
"source": "doc['my_vector'].size() == 0 ? 0 : cosineSimilarity(params.queryVector, 'my_vector')"
```
% NOTCONSOLE


#### Accessing vectors directly [vector-functions-accessing-vectors]

You can access vector values directly through the following functions:

* `doc[<field>].vectorValue` – returns a vector’s value as an array of floats

::::{note}
For `bit` vectors, it does return a `float[]`, where each element represents 8 bits.
::::


* `doc[<field>].magnitude` – returns a vector’s magnitude as a float (for vectors created prior to version 7.5 the magnitude is not stored. So this function calculates it anew every time it is called).

::::{note}
For `bit` vectors, this is just the square root of the sum of `1` bits.
::::


For example, the script below implements a cosine similarity using these two functions:

```console
GET my-index-000001/_search
{
  "query": {
    "script_score": {
      "query" : {
        "bool" : {
          "filter" : {
            "term" : {
              "status" : "published"
            }
          }
        }
      },
      "script": {
        "source": """
          float[] v = doc['my_dense_vector'].vectorValue;
          float vm = doc['my_dense_vector'].magnitude;
          float dotProduct = 0;
          for (int i = 0; i < v.length; i++) {
            dotProduct += v[i] * params.queryVector[i];
          }
          return dotProduct / (vm * (float) params.queryVectorMag);
        """,
        "params": {
          "queryVector": [4, 3.4, -0.2],
          "queryVectorMag": 5.25357
        }
      }
    }
  }
}
```


#### Bit vectors and vector functions [vector-functions-bit-vectors]

When using `bit` vectors, not all the vector functions are available. The supported functions are:

* [`hamming`](#vector-functions-hamming) – calculates Hamming distance, the sum of the bitwise XOR of the two vectors
* [`l1norm`](#vector-functions-l1) – calculates L1 distance, this is simply the `hamming` distance
* [`l2norm`](#vector-functions-l2) - calculates L2 distance, this is the square root of the `hamming` distance
* [`dotProduct`](#vector-functions-dot-product) – calculates dot product. When comparing two `bit` vectors, this is the sum of the bitwise AND of the two vectors. If providing `float[]` or `byte[]`, who has `dims` number of elements, as a query vector, the `dotProduct` is the sum of the floating point values using the stored `bit` vector as a mask.

::::{note}
When comparing `floats` and `bytes` with `bit` vectors, the `bit` vector is treated as a mask in big-endian order. For example, if the `bit` vector is `10100001` (e.g. the single byte value `161`) and its compared with array of values `[1, 2, 3, 4, 5, 6, 7, 8]` the `dotProduct` will be `1 + 3 + 8 = 16`.
::::


Here is an example of using dot-product with bit vectors.

```console
PUT my-index-bit-vectors
{
  "mappings": {
    "properties": {
      "my_dense_vector": {
        "type": "dense_vector",
        "index": false,
        "element_type": "bit",
        "dims": 40 <1>
      }
    }
  }
}

PUT my-index-bit-vectors/_doc/1
{
  "my_dense_vector": [8, 5, -15, 1, -7] <2>
}

PUT my-index-bit-vectors/_doc/2
{
  "my_dense_vector": [-1, 115, -3, 4, -128]
}

PUT my-index-bit-vectors/_doc/3
{
  "my_dense_vector": [2, 18, -5, 0, -124]
}

POST my-index-bit-vectors/_refresh
```
% TEST[continued]

1. The number of dimensions or bits for the `bit` vector.
2. This vector represents 5 bytes, or `5 * 8 = 40` bits, which equals the configured dimensions


```console
GET my-index-bit-vectors/_search
{
  "query": {
    "script_score": {
      "query" : {
        "match_all": {}
      },
      "script": {
        "source": "dotProduct(params.query_vector, 'my_dense_vector')",
        "params": {
          "query_vector": [8, 5, -15, 1, -7] <1>
        }
      }
    }
  }
}
```
% TEST[continued]

1. This vector is 40 bits, and thus will compute a bitwise `&` operation with the stored vectors.


```console
GET my-index-bit-vectors/_search
{
  "query": {
    "script_score": {
      "query" : {
        "match_all": {}
      },
      "script": {
        "source": "dotProduct(params.query_vector, 'my_dense_vector')",
        "params": {
          "query_vector": [0.23, 1.45, 3.67, 4.89, -0.56, 2.34, 3.21, 1.78, -2.45, 0.98, -0.12, 3.45, 4.56, 2.78, 1.23, 0.67, 3.89, 4.12, -2.34, 1.56, 0.78, 3.21, 4.12, 2.45, -1.67, 0.34, -3.45, 4.56, -2.78, 1.23, -0.67, 3.89, -4.34, 2.12, -1.56, 0.78, -3.21, 4.45, 2.12, 1.67] <1>
        }
      }
    }
  }
}
```
% TEST[continued]

1. This vector is 40 individual dimensions, and thus will sum the floating point values using the stored `bit` vector as a mask.


Currently, the `cosineSimilarity` function is not supported for `bit` vectors.



### Explain request [score-explanation]

Using an [explain request](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-explain) provides an explanation of how the parts of a score were computed. The `script_score` query can add its own explanation by setting the `explanation` parameter:

```console
GET /my-index-000001/_explain/0
{
  "query": {
    "script_score": {
      "query": {
        "match": { "message": "elasticsearch" }
      },
      "script": {
        "source": """
          long count = doc['count'].value;
          double normalizedCount = count / 10;
          if (explanation != null) {
            explanation.set('normalized count = count / 10 = ' + count + ' / 10 = ' + normalizedCount);
          }
          return normalizedCount;
        """
      }
    }
  }
}
```
% TEST[setup:my_index]

Note that the `explanation` will be null when using in a normal `_search` request, so having a conditional guard is best practice.



