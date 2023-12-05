/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.geo.SpatialPoint;

import java.util.Arrays;
/**
 * Builder for {@link PointVector}s that grows as needed.
 * This class is generated. Do not edit it.
 */
final class PointVectorBuilder extends AbstractVectorBuilder implements PointVector.Builder {

    private SpatialPoint[] values;

    PointVectorBuilder(int estimatedSize, BlockFactory blockFactory) {
        super(blockFactory);
        int initialSize = Math.max(estimatedSize, 2);
        adjustBreaker(initialSize);
        values = new SpatialPoint[Math.max(estimatedSize, 2)];
    }

    @Override
    public PointVectorBuilder appendPoint(SpatialPoint value) {
        ensureCapacity();
        values[valueCount] = value;
        valueCount++;
        return this;
    }

    @Override
    protected int elementSize() {
        return 16;
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
    public PointVector build() {
        finish();
        PointVector vector;
        if (valueCount == 1) {
            vector = blockFactory.newConstantPointBlockWith(values[0], 1, estimatedBytes).asVector();
        } else {
            if (values.length - valueCount > 1024 || valueCount < (values.length / 2)) {
                values = Arrays.copyOf(values, valueCount);
            }
            vector = blockFactory.newPointArrayVector(values, valueCount, estimatedBytes);
        }
        built();
        return vector;
    }
}
