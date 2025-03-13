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
 * This class is generated. Edit {@code X-VectorBuilder.java.st} instead.
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
            vector = blockFactory.newConstantDoubleBlockWith(values[0], 1, estimatedBytes).asVector();
        } else {
            if (values.length - valueCount > 1024 || valueCount < (values.length / 2)) {
                values = Arrays.copyOf(values, valueCount);
            }
            vector = blockFactory.newDoubleArrayVector(values, valueCount, estimatedBytes);
        }
        built();
        return vector;
    }
}
