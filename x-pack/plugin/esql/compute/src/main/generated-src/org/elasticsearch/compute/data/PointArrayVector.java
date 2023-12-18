/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;

import java.util.Arrays;

/**
 * Vector implementation that stores an array of Point values.
 * This class is generated. Do not edit it.
 */
public final class PointArrayVector extends AbstractVector implements PointVector {

    static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(PointArrayVector.class);

    private final double[] xValues, yValues;

    private final PointBlock block;

    public PointArrayVector(double[] xValues, double[] yValues, int positionCount) {
        this(xValues, yValues, positionCount, BlockFactory.getNonBreakingInstance());
    }

    public PointArrayVector(double[] xValues, double[] yValues, int positionCount, BlockFactory blockFactory) {
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
        return xValues[position];
    }

    @Override
    public double getY(int position) {
        return yValues[position];
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
    public PointVector filter(int... positions) {
        try (PointVector.Builder builder = blockFactory().newPointVectorBuilder(positions.length)) {
            for (int pos : positions) {
                builder.appendPoint(xValues[pos], yValues[pos]);
            }
            return builder.build();
        }
    }

    public static long ramBytesEstimated(double[] xValues, double[] yValues) {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(xValues) * 2;
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesEstimated(xValues, yValues);
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
        return getClass().getSimpleName()
            + "[positions="
            + getPositionCount()
            + ", x="
            + Arrays.toString(xValues)
            + ", y="
            + Arrays.toString(yValues)
            + ']';
    }

}
