/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import java.util.Arrays;

final class DoubleBlockBuilder extends AbstractBlockBuilder implements DoubleBlock.Builder {

    private double[] values;

    DoubleBlockBuilder(int estimatedSize) {
        values = new double[Math.max(estimatedSize, 2)];
    }

    @Override
    public DoubleBlockBuilder appendDouble(double value) {
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

    public DoubleBlockBuilder appendNull() {
        super.appendNull();
        return this;
    }

    @Override
    public DoubleBlockBuilder beginPositionEntry() {
        super.beginPositionEntry();
        return this;
    }

    @Override
    public DoubleBlockBuilder endPositionEntry() {
        super.endPositionEntry();
        return this;
    }

    @Override
    public DoubleBlock build() {
        if (positionEntryIsOpen) {
            endPositionEntry();
        }
        if (hasNonNullValue && positionCount == 1) {
            return new ConstantDoubleVector(values[0], 1).asBlock();
        } else {
            // TODO: may wanna trim the array, if there N% unused tail space
            if (isDense() && singleValued()) {
                return new DoubleArrayVector(values, positionCount).asBlock();
            } else {
                if (firstValueIndexes != null) {
                    firstValueIndexes[positionCount] = valueCount;  // TODO remove hack
                }
                return new DoubleArrayBlock(values, positionCount, firstValueIndexes, nullsMask);
            }
        }
    }
}
