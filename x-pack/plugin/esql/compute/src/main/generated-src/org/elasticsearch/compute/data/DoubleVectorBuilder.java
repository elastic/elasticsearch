/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import java.util.Arrays;

/**
 * Builder for {@link DoubleVector}s that grows as needed.
 * This class is generated. Do not edit it.
 */
final class DoubleVectorBuilder extends AbstractVectorBuilder implements DoubleVector.Builder {

    private double[] values;

    DoubleVectorBuilder(int estimatedSize, BlockFactory blockFactory) {
        super(blockFactory);
        int initialSize = Math.max(estimatedSize, 2);
        adjustBreaker(initialSize);
        values = new double[Math.max(estimatedSize, 2)];
    }

    @Override
    public DoubleVectorBuilder appendDouble(double value) {
        ensureCapacity();
        values[valueCount] = value;
        valueCount++;
        return this;
    }

    @Override
    protected int elementSize() {
        return Double.BYTES;
    }

    @Override
    protected int valuesLength() {
        return values.length;
    }

    @Override
    protected void growValuesArray(int newSize) {
        values = Arrays.copyOf(values, newSize);
    }

    @Override
    public DoubleVector build() {
        finish();
        DoubleVector vector;
        if (valueCount == 1) {
            vector = new ConstantDoubleVector(values[0], 1, blockFactory);
        } else {
            if (values.length - valueCount > 1024 || valueCount < (values.length / 2)) {
                values = Arrays.copyOf(values, valueCount);
            }
            vector = new DoubleArrayVector(values, valueCount, blockFactory);
        }
        /*
         * Update the breaker with the actual bytes used.
         * We pass false below even though we've used the bytes. That's weird,
         * but if we break here we will throw away the used memory, letting
         * it be deallocated. The exception will bubble up and the builder will
         * still technically be open, meaning the calling code should close it
         * which will return all used memory to the breaker.
         */
        blockFactory.adjustBreaker(vector.ramBytesUsed() - estimatedBytes, false);
        built();
        return vector;
    }
}
