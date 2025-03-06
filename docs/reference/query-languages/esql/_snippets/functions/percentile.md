## `PERCENTILE` [esql-percentile]

**Syntax**

:::{image} ../../../../../images/percentile.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

true
**Description**

Returns the value at which a certain percentage of observed values occur. For example, the 95th percentile is the value which is greater than 95% of the observed values and the 50th percentile is the `MEDIAN`.

**Supported types**

| number | percentile | result |
| --- | --- | --- |
| double | double | double |
| double | integer | double |
| double | long | double |
| integer | double | double |
| integer | integer | double |
| integer | long | double |
| long | double | double |
| long | integer | double |
| long | long | double |

**Examples**

```esql
FROM employees
| STATS p0 = PERCENTILE(salary,  0)
     , p50 = PERCENTILE(salary, 50)
     , p99 = PERCENTILE(salary, 99)
```

| p0:double | p50:double | p99:double |
| --- | --- | --- |
| 25324 | 47003 | 74970.29 |

The expression can use inline functions. For example, to calculate a percentile of the maximum values of a multivalued column, first use `MV_MAX` to get the maximum value per row, and use the result with the `PERCENTILE` function

```esql
FROM employees
| STATS p80_max_salary_change = PERCENTILE(MV_MAX(salary_change), 80)
```

| p80_max_salary_change:double |
| --- |
| 12.132 |


### `PERCENTILE` is (usually) approximate [esql-percentile-approximate]

There are many different algorithms to calculate percentiles. The naive implementation simply stores all the values in a sorted array. To find the 50th percentile, you simply find the value that is at `my_array[count(my_array) * 0.5]`.

Clearly, the naive implementation does not scale — the sorted array grows linearly with the number of values in your dataset. To calculate percentiles across potentially billions of values in an Elasticsearch cluster, *approximate* percentiles are calculated.

The algorithm used by the `percentile` metric is called TDigest (introduced by Ted Dunning in [Computing Accurate Quantiles using T-Digests](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf)).

When using this metric, there are a few guidelines to keep in mind:

* Accuracy is proportional to `q(1-q)`. This means that extreme percentiles (e.g. 99%) are more accurate than less extreme percentiles, such as the median
* For small sets of values, percentiles are highly accurate (and potentially 100% accurate if the data is small enough).
* As the quantity of values in a bucket grows, the algorithm begins to approximate the percentiles. It is effectively trading accuracy for memory savings. The exact level of inaccuracy is difficult to generalize, since it depends on your data distribution and volume of data being aggregated

The following chart shows the relative error on a uniform distribution depending on the number of collected values and the requested percentile:

![percentiles error](../../../../../images/percentiles_error.png "")

It shows how precision is better for extreme percentiles. The reason why error diminishes for large number of values is that the law of large numbers makes the distribution of values more and more uniform and the t-digest tree can do a better job at summarizing it. It would not be the case on more skewed distributions.

::::{warning}
`PERCENTILE` is also [non-deterministic](https://en.wikipedia.org/wiki/Nondeterministic_algorithm). This means you can get slightly different results using the same data.

::::



