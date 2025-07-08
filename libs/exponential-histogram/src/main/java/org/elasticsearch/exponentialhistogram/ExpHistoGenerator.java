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

import static org.elasticsearch.exponentialhistogram.ExponentialHistogramUtils.computeIndex;

/**
 * Class for generating a histogram from raw values.
 */
public class ExpHistoGenerator {

    private final double[] rawValueBuffer;
    int valueCount;

    private final ExponentialHistogramMerger resultMerger;
    private final FixedSizeExponentialHistogram valueBuffer;

    private boolean isFinished = false;

    public ExpHistoGenerator(int numBuckets) {
        rawValueBuffer = new double[numBuckets];
        valueCount = 0;
        valueBuffer = new FixedSizeExponentialHistogram(numBuckets);
        resultMerger = new ExponentialHistogramMerger(numBuckets);
    }

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

    public ExponentialHistogram get() {
        if (isFinished) {
            throw new IllegalStateException("get() has already been called");
        }
        isFinished = true;
        mergeValuesToHistogram();
        return resultMerger.get();
    }

    public static ExponentialHistogram createFor(double... values) {
        return createFor(values.length, Arrays.stream(values));
    }

    public static ExponentialHistogram createFor(int bucketCount, DoubleStream values) {
        ExpHistoGenerator generator = new ExpHistoGenerator(bucketCount);
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
            while ((i-1) >= 0 && computeIndex(rawValueBuffer[i-1] , scale) == index) {
                i--;
                count++;
            }
            valueBuffer.tryAddBucket(index, count, false);
        }

        int zeroCount = 0;
        while((negativeValuesCount + zeroCount) < valueCount && rawValueBuffer[negativeValuesCount+zeroCount] == 0) {
            zeroCount++;
        }
        valueBuffer.setZeroBucket(ZeroBucket.minimalWithCount(zeroCount));
        for (int i= negativeValuesCount + zeroCount; i < valueCount; i++) {
            long count = 1;
            long index = computeIndex(rawValueBuffer[i], scale);
            while ((i+1) < valueCount && computeIndex(rawValueBuffer[i+1] , scale) == index) {
                i++;
                count++;
            }
            valueBuffer.tryAddBucket(index, count, true);
        }

        resultMerger.add(valueBuffer);
        valueCount = 0;
    }


}
