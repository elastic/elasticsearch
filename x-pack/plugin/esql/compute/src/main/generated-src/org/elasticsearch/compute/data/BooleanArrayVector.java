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
 * Vector implementation that stores an array of boolean values.
 * This class is generated. Do not edit it.
 */
public final class BooleanArrayVector extends AbstractVector implements BooleanVector {

    static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(BooleanArrayVector.class);

    private final boolean[] values;

    private final BooleanBlock block;

    public BooleanArrayVector(boolean[] values, int positionCount) {
        this(values, positionCount, BlockFactory.getNonBreakingInstance());
    }

    public BooleanArrayVector(boolean[] values, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.values = values;
        this.block = new BooleanVectorBlock(this);
    }

    @Override
    public BooleanBlock asBlock() {
        return block;
    }

    @Override
    public boolean getBoolean(int position) {
        return values[position];
    }

    @Override
    public ElementType elementType() {
        return ElementType.BOOLEAN;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public BooleanVector filter(int... positions) {
        try (BooleanVector.Builder builder = blockFactory.newBooleanVectorBuilder(positions.length)) {
            for (int pos : positions) {
                builder.appendBoolean(values[pos]);
            }
            return builder.build();
        }
    }

    public static long ramBytesEstimated(boolean[] values) {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(values);
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesEstimated(values);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BooleanVector that) {
            return BooleanVector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return BooleanVector.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + Arrays.toString(values) + ']';
    }

}
