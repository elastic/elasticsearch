/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.exponentialhistogram;

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
public interface ExponentialHistogram {

    // TODO(b/128622): support min/max/sum/count storage and merging
    // TODO(b/128622): Add special positive and negative infinity buckets to allow representation of explicit bucket histograms with open
    // boundaries

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

    }

}
