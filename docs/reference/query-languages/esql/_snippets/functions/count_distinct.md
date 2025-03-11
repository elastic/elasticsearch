## `COUNT_DISTINCT` [esql-count_distinct]

**Syntax**

:::{image} ../../../../../images/count_distinct.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Column or literal for which to count the number of distinct values.

`precision`
:   Precision threshold. Refer to [Counts are approximate](../../esql-functions-operators.md#esql-agg-count-distinct-approximate). The maximum supported value is 40000. Thresholds above this number will have the same effect as a threshold of 40000. The default value is 3000.

**Description**

Returns the approximate number of distinct values.

**Supported types**

| field | precision | result |
| --- | --- | --- |
| boolean | integer | long |
| boolean | long | long |
| boolean | unsigned_long | long |
| boolean |  | long |
| date | integer | long |
| date | long | long |
| date | unsigned_long | long |
| date |  | long |
| date_nanos | integer | long |
| date_nanos | long | long |
| date_nanos | unsigned_long | long |
| date_nanos |  | long |
| double | integer | long |
| double | long | long |
| double | unsigned_long | long |
| double |  | long |
| integer | integer | long |
| integer | long | long |
| integer | unsigned_long | long |
| integer |  | long |
| ip | integer | long |
| ip | long | long |
| ip | unsigned_long | long |
| ip |  | long |
| keyword | integer | long |
| keyword | long | long |
| keyword | unsigned_long | long |
| keyword |  | long |
| long | integer | long |
| long | long | long |
| long | unsigned_long | long |
| long |  | long |
| text | integer | long |
| text | long | long |
| text | unsigned_long | long |
| text |  | long |
| version | integer | long |
| version | long | long |
| version | unsigned_long | long |
| version |  | long |

**Examples**

```esql
FROM hosts
| STATS COUNT_DISTINCT(ip0), COUNT_DISTINCT(ip1)
```

| COUNT_DISTINCT(ip0):long | COUNT_DISTINCT(ip1):long |
| --- | --- |
| 7 | 8 |

With the optional second parameter to configure the precision threshold

```esql
FROM hosts
| STATS COUNT_DISTINCT(ip0, 80000), COUNT_DISTINCT(ip1, 5)
```

| COUNT_DISTINCT(ip0, 80000):long | COUNT_DISTINCT(ip1, 5):long |
| --- | --- |
| 7 | 9 |

The expression can use inline functions. This example splits a string into multiple values using the `SPLIT` function and counts the unique values

```esql
ROW words="foo;bar;baz;qux;quux;foo"
| STATS distinct_word_count = COUNT_DISTINCT(SPLIT(words, ";"))
```

| distinct_word_count:long |
| --- |
| 5 |


### Counts are approximate [esql-agg-count-distinct-approximate]

Computing exact counts requires loading values into a set and returning its size. This doesn’t scale when working on high-cardinality sets and/or large values as the required memory usage and the need to communicate those per-shard sets between nodes would utilize too many resources of the cluster.

This `COUNT_DISTINCT` function is based on the [HyperLogLog++](https://static.googleusercontent.com/media/research.google.com/fr//pubs/archive/40671.pdf) algorithm, which counts based on the hashes of the values with some interesting properties:

* configurable precision, which decides on how to trade memory for accuracy,
* excellent accuracy on low-cardinality sets,
* fixed memory usage: no matter if there are tens or billions of unique values, memory usage only depends on the configured precision.

For a precision threshold of `c`, the implementation that we are using requires about `c * 8` bytes.

The following chart shows how the error varies before and after the threshold:

![cardinality error](/images/cardinality_error.png "")

For all 3 thresholds, counts have been accurate up to the configured threshold. Although not guaranteed, this is likely to be the case. Accuracy in practice depends on the dataset in question. In general, most datasets show consistently good accuracy. Also note that even with a threshold as low as 100, the error remains very low (1-6% as seen in the above graph) even when counting millions of items.

The HyperLogLog++ algorithm depends on the leading zeros of hashed values, the exact distributions of hashes in a dataset can affect the accuracy of the cardinality.

The `COUNT_DISTINCT` function takes an optional second parameter to configure the precision threshold. The precision_threshold options allows to trade memory for accuracy, and defines a unique count below which counts are expected to be close to accurate. Above this value, counts might become a bit more fuzzy. The maximum supported value is 40000, thresholds above this number will have the same effect as a threshold of 40000. The default value is `3000`.


