/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import java.util.Arrays;

/**
 * Builder for {@link PointVector}s that grows as needed.
 * This class is generated. Do not edit it.
 */
final class PointVectorBuilder extends AbstractVectorBuilder implements PointVector.Builder {

    private double[] xValues, yValues;

    PointVectorBuilder(int estimatedSize, BlockFactory blockFactory) {
        super(blockFactory);
        int initialSize = Math.max(estimatedSize, 2);
        adjustBreaker(initialSize);
        xValues = new double[Math.max(estimatedSize, 2)];
        yValues = new double[Math.max(estimatedSize, 2)];
    }

    @Override
    public PointVectorBuilder appendPoint(double x, double y) {
        ensureCapacity();
        xValues[valueCount] = x;
        yValues[valueCount] = y;
        valueCount++;
        return this;
    }

    @Override
    protected int elementSize() {
        return 16;
    }

    @Override
    protected int valuesLength() {
        return xValues.length;
    }

    @Override
    protected void growValuesArray(int newSize) {
        xValues = Arrays.copyOf(xValues, newSize);
        yValues = Arrays.copyOf(yValues, newSize);
    }

    @Override
    public PointVector build() {
        finish();
        PointVector vector;
        if (valueCount == 1) {
            vector = blockFactory.newConstantPointBlockWith(xValues[0], yValues[0], 1, estimatedBytes).asVector();
        } else {
            if (valuesLength() - valueCount > 1024 || valueCount < (valuesLength() / 2)) {
                growValuesArray(valueCount);
            }
            vector = blockFactory.newPointArrayVector(xValues, yValues, valueCount, estimatedBytes);
        }
        built();
        return vector;
    }
}
