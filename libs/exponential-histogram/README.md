This library provides an implementation of merging and analysis algorithms for exponential histograms based on the [OpenTelemetry definition](https://opentelemetry.io/docs/specs/otel/metrics/data-model/#exponentialhistogram). It is designed as a complementary tool to the OpenTelemetry SDK, focusing specifically on efficient histogram merging and accurate percentile estimation.

## Overview

The library implements a sparse storage approach where only populated buckets consume memory and count towards the bucket limit. This differs from the OpenTelemetry implementation, which uses dense storage. While dense storage allows for O(1) time insertion of individual values, our sparse representation requires O(log m) time where m is the bucket capacity. However, the sparse representation enables more efficient storage and provides a simple merging algorithm with runtime linear in the number of populated buckets. In addition, this library also provides an array-backed sparse storage, ensuring cache efficiency.

The sparse storage approach offers significant advantages for [distributions with fewer distinct values](#distributions-with-few-distinct-values) than the bucket count, allowing the library to achieve near-exact representation of such distributions. This makes it suitable not only for exponential histograms but also as a universal solution for handling explicit bucket histograms.

## Merging Algorithm

The merging algorithm works similarly to the merge-step of merge sort.
We simultaneously walk through the buckets of both histograms in order, merging them on the fly as needed.
If the total number of buckets in the end would exceed the bucket limit, we scale down as needed.

Before we merge the buckets, we need to take care of the special zero-bucket and bring both histograms to the same scale.

For the zero-bucket, we merge the zero threshold from both histograms and collapse any overlapping buckets into the resulting new zero bucket.

In order to bring both histograms to the same scale, we can make adjustments in both directions:
We can increase or decrease the scale of histograms as needed.

See the [upscaling section](#upscaling) for details on how the upscaling works.
Upscaling helps prevent the precision of the result histogram merged from many histograms from being dragged down to the lowest scale of a potentially misconfigured input histogram. For example, if a histogram is recorded with a too low zero threshold, this can result in a degraded scale when using dense histogram storage, even if the histogram only contains two points.

### Upscaling

In general, we assume that all values in a bucket lie on a single point: the point of least relative error. This is the point `x` in the bucket such that:

```
(x - l) / l = (u - x) / u
```

Where `l` is the lower bucket boundary and `u` is the upper bucket boundary.

This assumption allows us to increase the scale of histograms without increasing the bucket count. Buckets are simply mapped to the ones in the new scale containing the point of least relative error of the original buckets.

This can introduce a small error, as the original center might be moved slightly. Therefore, we ensure that the upscaling happens at most once to prevent errors from adding up.
The higher the amount of upscaling, the less the error (higher scale means smaller buckets, which in turn means we get a better fit around the original point of least relative error).

## Distributions with few distinct values

The sparse storage model only requires memory linear to the total number of buckets, while dense storage needs to store the entire range of the smallest and biggest buckets.

This offers significant benefits for distributions with fewer distinct values:
If we have at least as many buckets as we have distinct values to store in the histogram, we can almost exactly represent this distribution.
This can be achieved by simply maintaining the scale at the maximum supported value (so the buckets become the smallest).
At the time of writing, the maximum scale is 38, so the relative distance between the lower and upper bucket boundaries is (2^2(-38)).

This is best explained with a concrete example:
If we store, for example, a duration value of 10^15 nano seconds (= roughly 11.5 days), this value will be stored in a bucket that guarantees a relative error of at most 2^2(-38), so 2.5 microseconds in this case.
As long as the number of values we insert is lower than the bucket count, we are guaranteed that no down-scaling happens: In contrast to dense storage, the scale does not depend on the spread between the smallest and largest bucket index.

### Handling Explicit Bucket Histograms

We can make use of this property to convert explicit bucket histograms (https://opentelemetry.io/docs/specs/otel/metrics/data-model/#histogram) to exponential ones by again assuming that all values in a bucket lie in a single point:
   * For each explicit bucket, we take its point of least relative error and add it to the corresponding exponential histogram bucket with the corresponding count
   * The open, upper, and lower buckets, including infinity, will need special treatment, but these are not useful for percentile estimates anyway

This gives us a great solution for universally dealing with histograms:
When merging exponential histograms generated from explicit ones, the result is exact as long as the number of distinct buckets from the original explicit bucket histograms does not exceed the exponential histogram bucket count. As a result, the computed percentiles will be exact with only the error of the original conversion.
In addition, this allows us to compute percentiles on mixed explicit bucket histograms or even mix them with exponential ones by just using the exponential histogram algorithms.
