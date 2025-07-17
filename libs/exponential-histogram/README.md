This library provides an implementation of merging and analysis algorithms for exponential histograms based on the [OpenTelemetry definition](https://opentelemetry.io/docs/specs/otel/metrics/data-model/#exponentialhistogram). It is designed as a complementary tool to the OpenTelemetry SDK, focusing specifically on efficient histogram merging and accurate percentile estimation.

## Overview

The library implements base-2 exponential histograms with perfect subsetting. The most imporant properties are:

* The histogram has a scale parameter, which defines the accuracy. A higher scale implies a higher accuracy.
* The `base` for the buckets is defined as `base = 2^(2^-scale)`.
* The histogram bucket at index `i` has the range `(base^i, base^(i+1)]`
* Negative values are represented by a separate negative range of buckets with the boundaries `(-base^(i+1), -base^i]`
* Histograms are perfectly subsetting: increasing the scale by one merges each pair of neighboring buckets
* A special zero bucket with a zero-threshold is used to handle zero and close-to-zero values

For more details please refer to the [OpenTelemetry definition](https://opentelemetry.io/docs/specs/otel/metrics/data-model/#exponentialhistogram).

The library implements a sparse storage approach where only populated buckets consume memory and count towards the bucket limit. This differs from the OpenTelemetry implementation, which uses dense storage. While dense storage allows for O(1) time insertion of individual values, our sparse representation requires O(log m) time where m is the bucket capacity. However, the sparse representation enables more efficient storage and provides a simple merging algorithm with runtime linear in the number of populated buckets. Additionally, this library also provides an array-backed sparse storage, ensuring cache efficiency.

The sparse storage approach offers significant advantages for distributions with fewer distinct values than the bucket count, allowing the library to achieve representation of such distributions with an error so small that it won't be noticed in practice. This makes it suitable not only for exponential histograms but also as a universal solution for handling explicit bucket histograms.

## Merging Algorithm

The merging algorithm works similarly to the merge-step of merge sort. We simultaneously walk through the buckets of both histograms in order, merging them on the fly as needed. If the total number of buckets in the end would exceed the bucket limit, we scale down as needed.

Before we merge the buckets, we need to take care of the special zero-bucket and bring both histograms to the same scale.

For the zero-bucket, we merge the zero threshold from both histograms and collapse any overlapping buckets into the resulting new zero bucket.

In order to bring both histograms to the same scale, we can make adjustments in both directions: we can increase or decrease the scale of histograms as needed.

See the [upscaling section](#upscaling) for details on how the upscaling works. Upscaling helps prevent the precision of the result histogram merged from many histograms from being dragged down to the lowest scale of a potentially misconfigured input histogram. For example, if a histogram is recorded with a too low zero threshold, this can result in a degraded scale when using dense histogram storage, even if the histogram only contains two points.

### Upscaling

In general, we assume that all values in a bucket lie on a single point: the point of least relative error. This is the point `x` in the bucket such that:

```
(x - l) / l = (u - x) / u
```

Where `l` is the lower bucket boundary and `u` is the upper bucket boundary.

This assumption allows us to increase the scale of histograms without increasing the bucket count. Buckets are simply mapped to the ones in the new scale containing the point of least relative error of the original buckets.

This can introduce a small error, as the original center might be moved slightly. Therefore, we ensure that the upscaling happens at most once to prevent errors from adding up. The higher the amount of upscaling, the less the error (higher scale means smaller buckets, which in turn means we get a better fit around the original point of least relative error).

## Distributions with Few Distinct Values

The sparse storage model only requires memory linear to the total number of buckets, while dense storage needs to store the entire range of the smallest and biggest buckets.

This offers significant benefits for distributions with fewer distinct values:
If we have at least as many buckets as we have distinct values to store in the histogram, we can represent this distribution with a much smaller error than the dense representation.
This can be achieved by maintaining the scale at the maximum supported value (so the buckets become the smallest).
At the time of writing, the maximum scale is 38, so the relative distance between the lower and upper bucket boundaries is (2^2^(-38)).

The impact of the error is best shown with a concrete example:
If we store, for example, a duration value of 10^15 nanoseconds (= roughly 11.5 days), this value will be stored in a bucket that guarantees a relative error of at most 2^2^(-38), so roughly 2.5 microseconds in this case.
As long as the number of values we insert is lower than the bucket count, we are guaranteed that no down-scaling happens: In contrast to dense storage, the scale does not depend on the spread between the smallest and largest bucket index.

To clarify the difference between dense and sparse storage, let's assume that we have an empty histogram and the maximum scale is zero while the maximum bucket count is four.
The same logic applies to higher scales and bucket counts, but we use these values to get easier numbers for this example.
The scale of zero means that our bucket boundaries are `1, 2, 4, 8, 16, 32, 64, 128, 256, ...`.
We now want to insert the value `6` into the histogram. The dense storage works by storing an array for the bucket counts plus an initial offset.
This means that the first slot in the bucket counts array corresponds to the bucket with index `offset` and the last one to `offset + bucketCounts.length - 1`.
So if we add the value `6` to the histogram, it falls into the `(4,8]` bucket, which has the index `2`.

So our dense histogram looks like this:
```
offset = 2
bucketCounts = [1, 0, 0, 0] // represent bucket counts for bucket index 2 to 5
```

If we now insert the value `20` (`(16,32]`, bucket index 4), everything is still fine:
```
offset = 2
bucketCounts = [1, 0, 1, 0] // represent bucket counts for bucket index 2 to 5
```

However, we run into trouble if we insert the value `100`, which corresponds to index 6: That index is outside of the bounds of our array.
We can't just increase the `offset`, because the first bucket in our array is populated too.
We have no other option other than decreasing the scale of the histogram, to make sure that our values `6` and `100` fall in the range of four **consecutive** buckets due to the bucket count limit of the dense storage.

In contrast, a sparse histogram has no trouble storing this data while keeping the scale of zero:
```
bucketIndiciesToCounts: {
  "2" : 1,
  "4" : 1,
  "6" : 1
}
```

Downscaling on the sparse representation only happens if either:
 * The number of populated buckets would become bigger than our maximum bucket count. We have to downscale to make neighboring, populated buckets combine to a single bucket until we are below our limit again.
 * The highest or smallest indices require more bits to store than we allow. This does not happen in our implementation for normal inputs, because we allow up to 62 bits for index storage, which fits the entire numeric range of IEEE 754 double precision floats at our maximum scale.

### Handling Explicit Bucket Histograms

We can make use of this property to convert explicit bucket histograms (https://opentelemetry.io/docs/specs/otel/metrics/data-model/#histogram) to exponential ones by again assuming that all values in a bucket lie in a single point:
   * For each explicit bucket, we take its point of least relative error and add it to the corresponding exponential histogram bucket with the corresponding count.
   * The open, upper, and lower buckets, including infinity, will need special treatment, but these are not useful for percentile estimates anyway.

This gives us a great solution for universally dealing with histograms:
When merging exponential histograms generated from explicit ones, the scale is not decreased (and therefore the error not increased) as long as the number of distinct buckets from the original explicit bucket histograms does not exceed the exponential histogram bucket count. As a result, the computed percentiles will be precise with only the [relative error of the initial conversion](#distributions-with-few-distinct-values).
In addition, this allows us to compute percentiles on mixed explicit bucket histograms or even mix them with exponential ones by just using the exponential histogram algorithms.
