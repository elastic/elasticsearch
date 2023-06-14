/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import java.util.Arrays;

/**
 * Block build of IntBlocks.
 * This class is generated. Do not edit it.
 */
final class IntVectorBuilder extends AbstractVectorBuilder implements IntVector.Builder {

    private int[] values;

    IntVectorBuilder(int estimatedSize) {
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
    protected int valuesLength() {
        return values.length;
    }

    @Override
    protected void growValuesArray(int newSize) {
        values = Arrays.copyOf(values, newSize);
    }

    @Override
    public IntVector build() {
        if (valueCount == 1) {
            return new ConstantIntVector(values[0], 1);
        }
        if (values.length - valueCount > 1024 || valueCount < (values.length / 2)) {
            values = Arrays.copyOf(values, valueCount);
        }
        return new IntArrayVector(values, valueCount);
    }
}
