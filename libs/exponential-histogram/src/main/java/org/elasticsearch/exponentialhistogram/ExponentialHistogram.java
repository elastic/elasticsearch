/*
 * Copyright Elasticsearch B.V., and/or licensed to Elasticsearch B.V.
 * under one or more license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This file is based on a modification of https://github.com/open-telemetry/opentelemetry-java which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.exponentialhistogram;

import org.apache.lucene.util.Accountable;

import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;

/**
 * Interface for implementations of exponential histograms adhering to the
 * <a href="https://opentelemetry.io/docs/specs/otel/metrics/data-model/#exponentialhistogram">OpenTelemetry definition</a>.
 * This interface supports sparse implementations, allowing iteration over buckets without requiring direct index access.<br>
 * The most important properties are:
 * <ul>
 *     <li>The histogram has a scale parameter, which defines the accuracy. A higher scale implies a higher accuracy.
 *     The {@code base} for the buckets is defined as {@code base = 2^(2^-scale)}.</li>
 *     <li>The histogram bucket at index {@code i} has the range {@code (base^i, base^(i+1)]}</li>
 *     <li>Negative values are represented by a separate negative range of buckets with the boundaries {@code (-base^(i+1), -base^i]}</li>
 *     <li>Histograms are perfectly subsetting: increasing the scale by one merges each pair of neighboring buckets</li>
 *     <li>A special {@link ZeroBucket} is used to handle zero and close-to-zero values</li>
 * </ul>
 *
 * <br>
 * Additionally, all algorithms assume that samples within a bucket are located at a single point: the point of least relative error
 * (see {@link ExponentialScaleUtils#getPointOfLeastRelativeError(long, int)}).
 */
public interface ExponentialHistogram extends Accountable {

    // TODO(b/128622): Add special positive and negative infinity buckets
    // to allow representation of explicit bucket histograms with open boundaries.

    // A scale of 38 is the largest scale where we don't run into problems at the borders due to floating-point precision when computing
    // indices for double values.
    // Theoretically, a MAX_SCALE of 51 would work and would still cover the entire range of double values.
    // For that to work, the math for converting from double to indices and back would need to be reworked.
    // One option would be to use "Quadruple": https://github.com/m-vokhm/Quadruple
    int MAX_SCALE = 38;

    // At this scale, all double values fall into a single bucket.
    int MIN_SCALE = -11;

    // Only use 62 bits (plus the sign bit) at max to allow computing the difference between the smallest and largest index without causing
    // an overflow.
    // The extra bit also provides room for compact storage tricks.
    int MAX_INDEX_BITS = 62;
    long MAX_INDEX = (1L << MAX_INDEX_BITS) - 1;
    long MIN_INDEX = -MAX_INDEX;

    /**
     * The scale of the histogram. Higher scales result in higher accuracy but potentially more buckets.
     * Must be less than or equal to {@link #MAX_SCALE} and greater than or equal to {@link #MIN_SCALE}.
     *
     * @return the scale of the histogram
     */
    int scale();

    /**
     * @return the {@link ZeroBucket} representing the number of zero (or close-to-zero) values and its threshold
     */
    ZeroBucket zeroBucket();

    /**
     * @return a {@link Buckets} instance for the populated buckets covering the positive value range of this histogram.
     * The {@link BucketIterator#scale()} of iterators obtained via {@link Buckets#iterator()} must be the same as {@link #scale()}.
     */
    Buckets positiveBuckets();

    /**
     * @return a {@link Buckets} instance for the populated buckets covering the negative value range of this histogram.
     * The {@link BucketIterator#scale()} of iterators obtained via {@link Buckets#iterator()} must be the same as {@link #scale()}.
     */
    Buckets negativeBuckets();

    /**
     * Returns the sum of all values represented by this histogram.
     * Note that even if histograms are cumulative, the sum is not guaranteed to be monotonically increasing,
     * because histograms support negative values.
     *
     * @return the sum, guaranteed to be zero for empty histograms
     */
    double sum();

    /**
     * Returns the number of values represented by this histogram.
     * In other words, this is the sum of the counts of all buckets including the zero bucket.
     *
     * @return the value count, guaranteed to be zero for empty histograms
     */
    long valueCount();

    /**
     * Returns minimum of all values represented by this histogram.
     *
     * @return the minimum, NaN for empty histograms
     */
    double min();

    /**
     * Returns maximum of all values represented by this histogram.
     *
     * @return the maximum, NaN for empty histograms
     */
    double max();

    /**
     * Represents a bucket range of an {@link ExponentialHistogram}, either the positive or the negative range.
     */
    interface Buckets {

        /**
         * @return a {@link BucketIterator} for the populated buckets of this bucket range.
         * The {@link BucketIterator#scale()} of the returned iterator must be the same as {@link #scale()}.
         */
        CopyableBucketIterator iterator();

        /**
         * @return the highest populated bucket index, or an empty optional if no buckets are populated
         */
        OptionalLong maxBucketIndex();

        /**
         * @return the sum of the counts across all buckets of this range
         */
        long valueCount();

        /**
         * Returns the number of buckets. Note that this operation might require iterating over all buckets, and therefore is not cheap.
         * @return the number of buckets
         */
        default int bucketCount() {
            int count = 0;
            BucketIterator it = iterator();
            while (it.hasNext()) {
                count++;
                it.advance();
            }
            return count;
        }

    }

    /**
     * Value-based equality for exponential histograms.
     * @param a the first histogram (can be null)
     * @param b the second histogram (can be null)
     * @return true, if both histograms are equal
     */
    static boolean equals(ExponentialHistogram a, ExponentialHistogram b) {
        if (a == b) return true;
        if (a == null) return false;
        if (b == null) return false;

        return a.scale() == b.scale()
            && a.sum() == b.sum()
            && equalsIncludingNaN(a.min(), b.min())
            && equalsIncludingNaN(a.max(), b.max())
            && a.zeroBucket().equals(b.zeroBucket())
            && bucketIteratorsEqual(a.negativeBuckets().iterator(), b.negativeBuckets().iterator())
            && bucketIteratorsEqual(a.positiveBuckets().iterator(), b.positiveBuckets().iterator());
    }

    private static boolean equalsIncludingNaN(double a, double b) {
        return (a == b) || (Double.isNaN(a) && Double.isNaN(b));
    }

    private static boolean bucketIteratorsEqual(BucketIterator a, BucketIterator b) {
        if (a.scale() != b.scale()) {
            return false;
        }
        while (a.hasNext() && b.hasNext()) {
            if (a.peekIndex() != b.peekIndex() || a.peekCount() != b.peekCount()) {
                return false;
            }
            a.advance();
            b.advance();
        }
        return a.hasNext() == b.hasNext();
    }

    /**
     * Default hash code implementation to be used with {@link #equals(ExponentialHistogram, ExponentialHistogram)}.
     * @param histogram the histogram to hash
     * @return the hash code
     */
    static int hashCode(ExponentialHistogram histogram) {
        int hash = histogram.scale();
        hash = 31 * hash + Double.hashCode(histogram.sum());
        hash = 31 * hash + Long.hashCode(histogram.valueCount());
        hash = 31 * hash + Double.hashCode(histogram.min());
        hash = 31 * hash + Double.hashCode(histogram.max());
        hash = 31 * hash + histogram.zeroBucket().hashCode();
        // we intentionally don't include the hash of the buckets here, because that is likely expensive to compute
        // instead, we assume that the value count and sum are a good enough approximation in most cases to minimize collisions
        // the value count is typically available as a cached value and doesn't involve iterating over all buckets
        return hash;
    }

    static ExponentialHistogram empty() {
        return EmptyExponentialHistogram.INSTANCE;
    }

    /**
     * Create a builder for an exponential histogram with the given scale.
     * @param scale the scale of the histogram to build
     * @param breaker the circuit breaker to use
     * @return a new builder
     */
    static ExponentialHistogramBuilder builder(int scale, ExponentialHistogramCircuitBreaker breaker) {
        return new ExponentialHistogramBuilder(scale, breaker);
    }

    /**
     * Create a builder for an exponential histogram, which is initialized to copy the given histogram.
     * @param toCopy the histogram to copy
     * @param breaker the circuit breaker to use
     * @return a new builder
     */
    static ExponentialHistogramBuilder builder(ExponentialHistogram toCopy, ExponentialHistogramCircuitBreaker breaker) {
        return new ExponentialHistogramBuilder(toCopy, breaker);
    }

    /**
     * Creates a histogram representing the distribution of the given values with at most the given number of buckets.
     * If the given {@code maxBucketCount} is greater than or equal to the number of values, the resulting histogram will have a
     * relative error of less than {@code 2^(2^-MAX_SCALE) - 1}.
     *
     * @param maxBucketCount the maximum number of buckets
     * @param breaker the circuit breaker to use to limit memory allocations
     * @param values the values to be added to the histogram
     * @return a new {@link ReleasableExponentialHistogram}
     */
    static ReleasableExponentialHistogram create(int maxBucketCount, ExponentialHistogramCircuitBreaker breaker, double... values) {
        try (ExponentialHistogramGenerator generator = ExponentialHistogramGenerator.create(maxBucketCount, breaker)) {
            for (double val : values) {
                generator.add(val);
            }
            return generator.getAndClear();
        }
    }

    /**
     * Merges the provided exponential histograms to a new, single histogram with at most the given amount of buckets.
     *
     * @param maxBucketCount the maximum number of buckets the result histogram is allowed to have
     * @param breaker the circuit breaker to use to limit memory allocations
     * @param histograms the histograms to merge
     * @return the merged histogram
     */
    static ReleasableExponentialHistogram merge(
        int maxBucketCount,
        ExponentialHistogramCircuitBreaker breaker,
        Iterator<? extends ExponentialHistogram> histograms
    ) {
        try (ExponentialHistogramMerger merger = ExponentialHistogramMerger.create(maxBucketCount, breaker)) {
            while (histograms.hasNext()) {
                merger.add(histograms.next());
            }
            return merger.getAndClear();
        }
    }

    /**
     * Merges the provided exponential histograms to a new, single histogram with at most the given amount of buckets.
     *
     * @param maxBucketCount the maximum number of buckets the result histogram is allowed to have
     * @param breaker the circuit breaker to use to limit memory allocations
     * @param histograms the histograms to merge
     * @return the merged histogram
     */
    static ReleasableExponentialHistogram merge(
        int maxBucketCount,
        ExponentialHistogramCircuitBreaker breaker,
        ExponentialHistogram... histograms
    ) {
        return merge(maxBucketCount, breaker, List.of(histograms).iterator());
    }

}
