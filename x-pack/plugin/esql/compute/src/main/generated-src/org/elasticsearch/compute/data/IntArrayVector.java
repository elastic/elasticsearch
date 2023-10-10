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
 * Vector implementation that stores an array of int values.
 * This class is generated. Do not edit it.
 */
public final class IntArrayVector extends AbstractVector implements IntVector {

    static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(IntArrayVector.class);

    private final int[] values;

    private final IntBlock block;

    public IntArrayVector(int[] values, int positionCount) {
        this(values, positionCount, BlockFactory.getNonBreakingInstance());
    }

    public IntArrayVector(int[] values, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.values = values;
        this.block = new IntVectorBlock(this);
    }

    @Override
    public IntBlock asBlock() {
        return block;
    }

    @Override
    public int getInt(int position) {
        return values[position];
    }

    @Override
    public ElementType elementType() {
        return ElementType.INT;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public IntVector filter(int... positions) {
        try (IntVector.Builder builder = blockFactory.newIntVectorBuilder(positions.length)) {
            for (int pos : positions) {
                builder.appendInt(values[pos]);
            }
            return builder.build();
        }
    }

    public static long ramBytesEstimated(int[] values) {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(values);
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesEstimated(values);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof IntVector that) {
            return IntVector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return IntVector.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + Arrays.toString(values) + ']';
    }

}
