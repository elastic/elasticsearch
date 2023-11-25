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
 * Vector implementation that stores an array of long values.
 * This class is generated. Do not edit it.
 */
public final class LongArrayVector extends AbstractVector implements LongVector {

    static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(LongArrayVector.class);

    private final long[] values;

    private final LongBlock block;

    public LongArrayVector(long[] values, int positionCount) {
        this(values, positionCount, BlockFactory.getNonBreakingInstance());
    }

    public LongArrayVector(long[] values, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.values = values;
        this.block = new LongVectorBlock(this);
    }

    @Override
    public LongBlock asBlock() {
        return block;
    }

    @Override
    public long getLong(int position) {
        return values[position];
    }

    @Override
    public ElementType elementType() {
        return ElementType.LONG;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public LongVector filter(int... positions) {
        try (LongVector.Builder builder = blockFactory.newLongVectorBuilder(positions.length)) {
            for (int pos : positions) {
                builder.appendLong(values[pos]);
            }
            return builder.build();
        }
    }

    public static long ramBytesEstimated(long[] values) {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(values);
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesEstimated(values);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LongVector that) {
            return LongVector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return LongVector.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + Arrays.toString(values) + ']';
    }

}
