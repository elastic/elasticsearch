/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import java.util.Arrays;

/**
 * Builder for {@link IntVector}s that grows as needed.
 * This class is generated. Edit {@code X-VectorBuilder.java.st} instead.
 */
final class IntVectorBuilder extends AbstractVectorBuilder implements IntVector.Builder {

    private int[] values;

    IntVectorBuilder(int estimatedSize, BlockFactory blockFactory) {
        super(blockFactory);
        int initialSize = Math.max(estimatedSize, 2);
        adjustBreaker(initialSize);
        values = new int[Math.max(estimatedSize, 2)];
    }

    @Override
    public IntVectorBuilder appendInt(int value) {
        ensureCapacity();
        values[valueCount] = value;
        valueCount++;
        return this;
    }

    @Override
    protected int elementSize() {
        return Integer.BYTES;
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
    public IntVector build() {
        finish();
        IntVector vector;
        if (valueCount == 1) {
            vector = blockFactory.newConstantIntBlockWith(values[0], 1, estimatedBytes).asVector();
        } else {
            if (values.length - valueCount > 1024 || valueCount < (values.length / 2)) {
                values = Arrays.copyOf(values, valueCount);
            }
            vector = blockFactory.newIntArrayVector(values, valueCount, estimatedBytes);
        }
        built();
        return vector;
    }
}
