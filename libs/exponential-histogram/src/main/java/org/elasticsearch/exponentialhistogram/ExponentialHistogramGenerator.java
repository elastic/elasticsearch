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

import java.util.Arrays;

import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.computeIndex;

/**
 * Only intended for use in tests currently.
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

        // Buckets must be provided with their indices in ascending order.
        // For the negative range, higher bucket indices correspond to bucket boundaries closer to -INF
        // and smaller bucket indices correspond to bucket boundaries closer to zero.
        // therefore we have to iterate the negative values in the sorted rawValueBuffer reverse order,
        // from the value closest to -INF to the value closest to zero.
        // not that i here is the index of the value in the rawValueBuffer array
        // and is unrelated to the histogram bucket index for the value.
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
