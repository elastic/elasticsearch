There are many different algorithms to calculate percentiles. The naive implementation simply stores all the values in a sorted array. To find the 50th percentile, you simply find the value that is at `my_array[count(my_array) * 0.5]`.

Clearly, the naive implementation does not scale — the sorted array grows linearly with the number of values in your dataset. To calculate percentiles across potentially billions of values in an Elasticsearch cluster, *approximate* percentiles are calculated.

The algorithm used by the `percentile` metric is called TDigest (introduced by Ted Dunning in [Computing Accurate Quantiles using T-Digests](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf)).

When using this metric, there are a few guidelines to keep in mind:

* Accuracy is proportional to `q(1-q)`. This means that extreme percentiles (e.g. 99%) are more accurate than less extreme percentiles, such as the median
* For small sets of values, percentiles are highly accurate (and potentially 100% accurate if the data is small enough).
* As the quantity of values in a bucket grows, the algorithm begins to approximate the percentiles. It is effectively trading accuracy for memory savings. The exact level of inaccuracy is difficult to generalize, since it depends on your data distribution and volume of data being aggregated

The following chart shows the relative error on a uniform distribution depending on the number of collected values and the requested percentile:

![percentiles error](/reference/query-languages/images/percentiles_error.png "")

It shows how precision is better for extreme percentiles. The reason why error diminishes for large number of values is that the law of large numbers makes the distribution of values more and more uniform and the t-digest tree can do a better job at summarizing it. It would not be the case on more skewed distributions.
