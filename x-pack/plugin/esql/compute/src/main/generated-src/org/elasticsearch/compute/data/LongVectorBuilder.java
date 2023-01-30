/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import java.util.Arrays;

/**
 * Block build of LongBlocks.
 * This class is generated. Do not edit it.
 */
final class LongVectorBuilder extends AbstractVectorBuilder implements LongVector.Builder {

    private long[] values;

    LongVectorBuilder(int estimatedSize) {
        values = new long[Math.max(estimatedSize, 2)];
    }

    @Override
    public LongVectorBuilder appendLong(long value) {
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
    public LongArrayVector build() {
        // TODO: may wanna trim the array, if there N% unused tail space
        return new LongArrayVector(values, valueCount);
    }
}
