/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import java.util.Arrays;

/**
 * Block build of BooleanBlocks.
 * This class is generated. Do not edit it.
 */
final class BooleanVectorBuilder extends AbstractVectorBuilder implements BooleanVector.Builder {

    private boolean[] values;

    BooleanVectorBuilder(int estimatedSize) {
        values = new boolean[Math.max(estimatedSize, 2)];
    }

    @Override
    public BooleanVectorBuilder appendBoolean(boolean value) {
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
    public BooleanVector build() {
        if (valueCount == 1) {
            return new ConstantBooleanVector(values[0], 1);
        }
        if (values.length - valueCount > 1024 || valueCount < (values.length / 2)) {
            values = Arrays.copyOf(values, valueCount);
        }
        return new BooleanArrayVector(values, valueCount);
    }
}
