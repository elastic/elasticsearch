* configurable precision, which decides on how to trade memory for accuracy,
* excellent accuracy on low-cardinality sets,
* fixed memory usage: no matter if there are tens or billions of unique values, memory usage only depends on the configured precision.

For a precision threshold of `c`, the implementation that we are using requires about `c * 8` bytes.

The following chart shows how the error varies before and after the threshold:

![cardinality error](/reference/query-languages/images/cardinality_error.png "")

For all 3 thresholds, counts have been accurate up to the configured threshold. Although not guaranteed,
this is likely to be the case. Accuracy in practice depends on the dataset in question. In general,
most datasets show consistently good accuracy. Also note that even with a threshold as low as 100,
the error remains very low (1-6% as seen in the above graph) even when counting millions of items.

The HyperLogLog++ algorithm depends on the leading zeros of hashed values, the exact distributions of
hashes in a dataset can affect the accuracy of the cardinality.
