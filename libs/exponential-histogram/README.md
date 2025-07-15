* Implementation of merging and analysis algorithms for exponential histograms based on the [OpenTelemetry definition](https://opentelemetry.io/docs/specs/otel/metrics/data-model/#exponentialhistogram)
* Uses a sparse storage: Only populated buckets consume memory and count towards the bucket limit
* In contrast, the OpenTelemetry implementation uses a dense storage
* Dense storage allows for O(1) time for insertion of individual values, sparse requires O( log m) where m is the bucket capacity
* Sparse representation allows a more efficient storage and also for a simple merging algorithm with a runtime linear in the number of populated buckets
* Sparse storage can almost exactly represent distributions with less distinct values than bucket count, which allows us to use this implementation also for explicit bucket histograms

# Merging algorithm

 * Works very similar to the merge-step of merge sort: We iterate simultaneously over buckets from both histograms, merging buckets as needed
 * If the merged buckets exceed the configurable bucket count limit, we scale down as needed
 * We respect the zero-threshold of the zero buckets. We merge the zero threshold from both histograms and collapse any overlapping buckets into the zero bucket
 * In addition to not have single, malformed histograms drag down the accuracy, we also increase the scale of the histogram to aggregate if necessary (link to upscaling section)

## Upscaling

 * We assume that all values in a bucket lie on a single point: the point of least relative error (TBD add definiton from code here)
 * This allows us to increase the scale of histograms without increasing the bucket count. Buckets are simply mapped to the ones in the new scale containing the point of least relative error of the original buckets
 * This can introduce a small error, as the original center might be moved a little, therefore we ensure that the upscaling happens at most once to not have the errors add-up
 * The higher the amount of upscaling, the less the error (higher scale means smaller buckets, which in turn means we get a better fit around the original point of least relative error)

# Distributions with fewer distinct values than the bucket count
* The sparse storage only requires memory linear to the total number of buckets, dense storage in needs to store the entire range of the smallest and biggest buckets.
* If we have at least as many buckets as we have distinct values to store in the histogram, we can almost exactly represent this distribution
* We can set the scale to the maximum supported value (so the buckets become the smallest)
* At the time of writing the maximum scale is 38, so the relative distance between the lower and upper bucket boundaries are (2^2(-38))
* In otherwords : If we store for example a duration value of 10^15 nano seconds (= roughly 11.5 days), this value will be stored in a bucket which guarantees a relative error of at most 2^2(-38), so 2.5 microseconds in this case
* We can make use of this property to convert explicit bucket histograms (https://opentelemetry.io/docs/specs/otel/metrics/data-model/#histogram) to exponential ones by again assuming that all values in a bucket lie in a single point:
   * For each explicit bucket, we take its point of least relative error and add it to the corresponding exponential histogram bucket with the corresponding count
   * The open, upper and lower buckets including infinity will need a special treatment, but these are not useful for percentile estimates anyway
* This gives us a great solution for universally dealing with histograms:
  * When merging exponential histograms generated from explicit ones, the result is exact as long as the number of distinct buckets from the original explicit bucket histograms does not exceed the exponential histogram bucket count
  * As a result, the computed percentiles will be exact with only the error of the original conversion
  * In addition this allows us to compute percentiles on mixed explicit bucket histograms or even mixing them with exponential ones by just using the exponential histogram algorithms
