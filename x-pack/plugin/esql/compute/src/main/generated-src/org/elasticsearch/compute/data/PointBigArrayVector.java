/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.core.Releasable;

/**
 * Vector implementation that defers to an enclosed PointArray.
 * This class is generated. Do not edit it.
 */
public final class PointBigArrayVector extends AbstractVector implements PointVector, Releasable {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(PointBigArrayVector.class);

    private final DoubleArray xValues, yValues;

    private final PointBlock block;

    public PointBigArrayVector(DoubleArray xValues, DoubleArray yValues, int positionCount) {
        this(xValues, yValues, positionCount, BlockFactory.getNonBreakingInstance());
    }

    public PointBigArrayVector(DoubleArray xValues, DoubleArray yValues, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.xValues = xValues;
        this.yValues = yValues;
        this.block = new PointVectorBlock(this);
    }

    @Override
    public PointBlock asBlock() {
        return block;
    }

    @Override
    public double getX(int position) {
        return xValues.get(position);
    }

    @Override
    public double getY(int position) {
        return yValues.get(position);
    }

    @Override
    public ElementType elementType() {
        return ElementType.POINT;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(xValues) * 2;
    }

    @Override
    public PointVector filter(int... positions) {
        var blockFactory = blockFactory();
        final DoubleArray xFiltered = blockFactory.bigArrays().newDoubleArray(positions.length);
        final DoubleArray yFiltered = blockFactory.bigArrays().newDoubleArray(positions.length);
        for (int i = 0; i < positions.length; i++) {
            xFiltered.set(i, xValues.get(positions[i]));
            yFiltered.set(i, yValues.get(positions[i]));
        }
        return new PointBigArrayVector(xFiltered, yFiltered, positions.length, blockFactory);
    }

    @Override
    public void close() {
        if (released) {
            throw new IllegalStateException("can't release already released vector [" + this + "]");
        }
        released = true;
        xValues.close();
        yValues.close();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PointVector that) {
            return PointVector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return PointVector.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", x=" + xValues + ", y=" + yValues + ']';
    }
}
