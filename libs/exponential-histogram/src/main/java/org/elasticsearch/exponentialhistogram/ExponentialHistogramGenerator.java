/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.exponentialhistogram;

import java.util.Arrays;
import java.util.stream.DoubleStream;

import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.computeIndex;

/**
 * A class for accumulating raw values into an {@link ExponentialHistogram} with a given maximum number of buckets.
 *
 * If the number of values is less than or equal to the bucket capacity, the resulting histogram is guaranteed
 * to represent the exact raw values with a relative error less than {@code 2^(2^-MAX_SCALE) - 1}.
 */
public class ExponentialHistogramGenerator {

    // Merging individual values into a histogram would be way too slow with our sparse, array-backed histogram representation.
    // Therefore, for a bucket capacity of c, we first buffer c raw values to be inserted.
    // We then turn those into an "exact" histogram, which in turn we merge with our actual result accumulator.
    // This yields an amortized runtime of O(log(c)).
    private final double[] rawValueBuffer;
    int valueCount;

    private final ExponentialHistogramMerger resultMerger;
    private final FixedCapacityExponentialHistogram valueBuffer;

    private boolean isFinished = false;

    /**
     * Creates a new instance with the specified maximum number of buckets.
     *
     * @param maxBucketCount the maximum number of buckets for the generated histogram
     */
    public ExponentialHistogramGenerator(int maxBucketCount) {
        rawValueBuffer = new double[maxBucketCount];
        valueCount = 0;
        valueBuffer = new FixedCapacityExponentialHistogram(maxBucketCount);
        resultMerger = new ExponentialHistogramMerger(maxBucketCount);
    }

    /**
     * Adds the given value to the histogram.
     * Must not be called after {@link #get()} has been called.
     *
     * @param value the value to add
     */
    public void add(double value) {
        if (isFinished) {
            throw new IllegalStateException("get() has already been called");
        }
        if (valueCount == rawValueBuffer.length) {
            mergeValuesToHistogram();
        }
        rawValueBuffer[valueCount] = value;
        valueCount++;
    }

    /**
     * Returns the histogram representing the distribution of all accumulated values.
     *
     * @return the histogram representing the distribution of all accumulated values
     */
    public ExponentialHistogram get() {
        isFinished = true;
        mergeValuesToHistogram();
        return resultMerger.get();
    }

    /**
     * Creates a histogram representing the distribution of the given values.
     * The histogram will have a bucket count of at most the length of the provided array
     * and will have a relative error less than {@code 2^(2^-MAX_SCALE) - 1}.
     *
     * @param values the values to be added to the histogram
     * @return a new {@link ExponentialHistogram}
     */
    public static ExponentialHistogram createFor(double... values) {
        return createFor(values.length, Arrays.stream(values));
    }

    /**
     * Creates a histogram representing the distribution of the given values with at most the given number of buckets.
     * If the given bucketCount is greater than or equal to the number of values, the resulting histogram will have a
     * relative error of less than {@code 2^(2^-MAX_SCALE) - 1}.
     *
     * @param bucketCount the maximum number of buckets
     * @param values a stream of values to be added to the histogram
     * @return a new {@link ExponentialHistogram}
     */
    public static ExponentialHistogram createFor(int bucketCount, DoubleStream values) {
        ExponentialHistogramGenerator generator = new ExponentialHistogramGenerator(bucketCount);
        values.forEach(generator::add);
        return generator.get();
    }

    private void mergeValuesToHistogram() {
        if (valueCount == 0) {
            return;
        }
        Arrays.sort(rawValueBuffer, 0, valueCount);
        int negativeValuesCount = 0;
        while (negativeValuesCount < valueCount && rawValueBuffer[negativeValuesCount] < 0) {
            negativeValuesCount++;
        }

        valueBuffer.reset();
        int scale = valueBuffer.scale();

        for (int i = negativeValuesCount - 1; i >= 0; i--) {
            long count = 1;
            long index = computeIndex(rawValueBuffer[i], scale);
            while ((i - 1) >= 0 && computeIndex(rawValueBuffer[i - 1], scale) == index) {
                i--;
                count++;
            }
            valueBuffer.tryAddBucket(index, count, false);
        }

        int zeroCount = 0;
        while ((negativeValuesCount + zeroCount) < valueCount && rawValueBuffer[negativeValuesCount + zeroCount] == 0) {
            zeroCount++;
        }
        valueBuffer.setZeroBucket(ZeroBucket.minimalWithCount(zeroCount));
        for (int i = negativeValuesCount + zeroCount; i < valueCount; i++) {
            long count = 1;
            long index = computeIndex(rawValueBuffer[i], scale);
            while ((i + 1) < valueCount && computeIndex(rawValueBuffer[i + 1], scale) == index) {
                i++;
                count++;
            }
            valueBuffer.tryAddBucket(index, count, true);
        }

        resultMerger.add(valueBuffer);
        valueCount = 0;
    }

}
