/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import java.util.Arrays;

final class LongBlockBuilder extends AbstractBlockBuilder implements LongBlock.Builder {

    private long[] values;

    LongBlockBuilder(int estimatedSize) {
        values = new long[Math.max(estimatedSize, 2)];
    }

    @Override
    public LongBlockBuilder appendLong(long value) {
        ensureCapacity();
        values[valueCount] = value;
        hasNonNullValue = true;
        valueCount++;
        updatePosition();
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
    public LongBlockBuilder appendNull() {
        super.appendNull();
        return this;
    }

    @Override
    public LongBlockBuilder beginPositionEntry() {
        super.beginPositionEntry();
        return this;
    }

    @Override
    public LongBlockBuilder endPositionEntry() {
        super.endPositionEntry();
        return this;
    }

    @Override
    public LongBlock build() {
        if (positionEntryIsOpen) {
            endPositionEntry();
        }
        if (hasNonNullValue && positionCount == 1) {
            return new ConstantLongVector(values[0], 1).asBlock();
        } else {
            // TODO: may wanna trim the array, if there N% unused tail space
            if (isDense() && singleValued()) {
                return new LongArrayVector(values, positionCount).asBlock();
            } else {
                if (firstValueIndexes != null) {
                    firstValueIndexes[positionCount] = valueCount;  // TODO remove hack
                }
                return new LongArrayBlock(values, positionCount, firstValueIndexes, nullsMask);
            }
        }
    }
}
