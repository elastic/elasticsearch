/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import java.util.Arrays;

final class DoubleBlockBuilder extends AbstractBlockBuilder {

    private double[] values;

    DoubleBlockBuilder(int estimatedSize) {
        values = new double[Math.max(estimatedSize, 2)];
    }

    @Override
    public BlockBuilder appendDouble(double value) {
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
    public Block build() {
        if (positionEntryIsOpen) {
            endPositionEntry();
        }
        if (hasNonNullValue == false) {
            return new ConstantNullBlock(positionCount);
        } else if (positionCount == 1) {
            return new VectorBlock(new ConstantDoubleVector(values[0], 1));
        } else {
            // TODO: may wanna trim the array, if there N% unused tail space
            if (isDense() && singleValued()) {
                return new VectorBlock(new DoubleVector(values, positionCount));
            } else {
                if (firstValueIndexes != null) {
                    firstValueIndexes[positionCount] = valueCount;  // TODO remove hack
                }
                return new DoubleBlock(values, positionCount, firstValueIndexes, nullsMask);
            }
        }
    }
}
