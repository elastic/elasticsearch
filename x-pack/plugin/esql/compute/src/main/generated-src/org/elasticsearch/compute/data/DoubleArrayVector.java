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
 * Vector implementation that stores an array of double values.
 * This class is generated. Do not edit it.
 */
public final class DoubleArrayVector extends AbstractVector implements DoubleVector {

    static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DoubleArrayVector.class);

    private final double[] values;

    private final DoubleBlock block;

    public DoubleArrayVector(double[] values, int positionCount) {
        this(values, positionCount, BlockFactory.getNonBreakingInstance());
    }

    public DoubleArrayVector(double[] values, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.values = values;
        this.block = new DoubleVectorBlock(this);
    }

    @Override
    public DoubleBlock asBlock() {
        return block;
    }

    @Override
    public double getDouble(int position) {
        return values[position];
    }

    @Override
    public ElementType elementType() {
        return ElementType.DOUBLE;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public DoubleVector filter(int... positions) {
        try (DoubleVector.Builder builder = blockFactory.newDoubleVectorBuilder(positions.length)) {
            for (int pos : positions) {
                builder.appendDouble(values[pos]);
            }
            return builder.build();
        }
    }

    public static long ramBytesEstimated(double[] values) {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(values);
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesEstimated(values);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DoubleVector that) {
            return DoubleVector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return DoubleVector.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + Arrays.toString(values) + ']';
    }

}
