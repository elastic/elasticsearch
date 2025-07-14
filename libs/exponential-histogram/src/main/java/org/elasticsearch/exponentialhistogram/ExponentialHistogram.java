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
 * Interface for implementations of exponential histograms adhering to the <a href="https://opentelemetry.io/docs/specs/otel/metrics/data-model/#exponentialhistogram">opentelemetry definition</a>.
 * This interface explicitly allows for sparse implementation: It does not offer to directly access buckets by index, instead it
 * is only possible to iterate over the buckets.<br>
 * The most important properties are:
 * <ul>
 *     <li>The histogram has a scale parameter, which defines the accuracy. The <code>base</code> for the buckets is defined as <code>base = 2^(2^-scale)</code></li>
 *     <li>The histogram bucket at index <code>i</code> has the range <code>(base^i, base^(i+1)]</code> </li>
 *     <li>Negative values are represented by a separate negative range of buckets with the boundaries <code>(-base^(i+1), -base^i]</code></li>
 *     <li>histograms are perfectly subsetting: Increasing the scale by one exactly merges each pair of neighbouring buckets</li>
 *     <li>a special {@link ZeroBucket} is used to handle zero and close to zero values</li>
 * </ul>
 *
 * <br>
 * In addition, in all algorithms we make a central assumption about the distribution of samples within each bucket:
 * We assume they all lie on the single point of least error relative to the bucket boundaries (see {@link ExponentialScaleUtils#getPointOfLeastRelativeError(long, int)}).
 */
public interface ExponentialHistogram {

    //TODO: support min/max/sum/count storage and merging
    //TODO: Add special positive and negative infinity buckets to allow representation of explicit bucket histograms with open boundaries

    // scale of 38 is the largest scale where at the borders we don't run into problems due to floating point precision when computing
    // indices for double values
    // Theoretically, a MAX_SCALE of 51 would work and would still cover the entire range of double values
    // For that to work, we'll have to rework the math of converting from double to indices and back
    // One option would be to use "Quadruple": https://github.com/m-vokhm/Quadruple
    int MAX_SCALE = 38;

    // Add this scale all double values already fall into a single bucket
    int MIN_SCALE = -11;

    // Only use 62 bit at max to allow to compute the difference between the smallest and largest index without causing overflow
    // Also the extra bit gives us room for some tricks for compact storage
    int MAX_INDEX_BITS = 62;
    long MAX_INDEX = (1L << MAX_INDEX_BITS) - 1;
    long MIN_INDEX = -MAX_INDEX;

    /**
     * The scale of the histogram. Higher scales result in higher accuracy, but potentially higher bucket count.
     * Must be less than or equal to {@link #MAX_SCALE} and greater than or equal to {@link #MIN_SCALE}.
     */
    int scale();

    /**
     * @return the {@link ZeroBucket} representing the number of zero (or close to zero) values and its threshold
     */
    ZeroBucket zeroBucket();

    /**
     * @return a {@link BucketIterator} for the populated, positive buckets of this histogram. {@link BucketIterator#scale()} of the return value must return the same value as {@link #scale()}.
     */
    CopyableBucketIterator positiveBuckets();

    /**
     * @return a {@link BucketIterator} for the populated, negative buckets of this histogram. {@link BucketIterator#scale()} of the return value must return the same value as {@link #scale()}.
     */
    CopyableBucketIterator negativeBuckets();

    /**
     * Returns the highest populated bucket index, taking both negative and positive buckets into account.
     * If there are neither positive nor negative buckets populated, an empty optional is returned.
     */
    OptionalLong maximumBucketIndex();

    /**
     * Iterator over non-empty buckets of the histogram. Can represent either the positive or negative histogram range.
     *  <ul>
     *      <li>The iterator always iterates from the lowest bucket index to the highest</li>
     *      <li>The iterator never returns duplicate buckets (buckets with the same index) </li>
     *      <li>The iterator never returns empty buckets ({@link #peekCount() is never zero}</li>
     *  </ul>
     */
    interface BucketIterator {
        /**
         * Checks if there are any buckets remaining to be visited by this iterator.
         * If the end has been reached, it is illegal to call {@link #peekCount()}, {@link #peekIndex()} or {@link #advance()}.
         *
         * @return <code>false</code>, if the end has been reached, <code>true</code> otherwise.
         */
        boolean hasNext();

        /**
         * The number of items in the bucket this iterator currently points at. Does not advance the iterator by itself and therefore can be called repeatedly to return the same value.
         * Must not be called if {@link #hasNext()} returns <code>false</code>.
         *
         * @return the number of items in the bucket, always greater than zero
         */
        long peekCount();

        /**
         * The index of the bucket this iterator currently points at. Does not advance the iterator by itself and therefore can be called repeatedly to return the same value.
         * Must not be called if {@link #hasNext()} returns <code>false</code>.
         *
         * @return the index of the bucket, guaranteed to be in the range [{@link #MIN_INDEX}, {@link #MAX_INDEX}]
         */
        long peekIndex();

        /**
         * Moves the iterator to the next, non-empty bucket.
         * If {@link #hasNext()} is <code>true</code> after {@link #advance()}, {@link #peekIndex()} is guaranteed to return a value bigger than prior to the {@link #advance()} call.
         */
        void advance();

        /**
         * Provides the scale that can be used to convert indices returned by {@link #peekIndex()} to the bucket boundaries,
         * e.g. via {@link ExponentialScaleUtils#getLowerBucketBoundary(long, int)}.
         *
         * @return the scale, which is guaranteed to be constant over the lifetime of this iterator.
         */
        int scale();

        BucketIterator copy();
    }

    /**
     * A {@link BucketIterator} which can be copied.
     */
    interface CopyableBucketIterator extends BucketIterator {

        /**
         * Provides a bucket iterator pointing at the same bucket of the same range of buckets as this iterator.
         * Calling {@link #advance()} on the copied iterator does not affect <code>this</code> and vice-versa.
         */
        CopyableBucketIterator copy();
    }

}
