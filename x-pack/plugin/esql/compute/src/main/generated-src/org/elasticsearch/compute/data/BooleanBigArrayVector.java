/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.core.Releasable;

/**
 * Vector implementation that defers to an enclosed BooleanArray.
 * This class is generated. Do not edit it.
 */
public final class BooleanBigArrayVector extends AbstractVector implements BooleanVector, Releasable {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(BooleanBigArrayVector.class);

    private final BitArray values;

    private final BooleanBlock block;

    public BooleanBigArrayVector(BitArray values, int positionCount) {
        this(values, positionCount, BlockFactory.getNonBreakingInstance());
    }

    public BooleanBigArrayVector(BitArray values, int positionCount, BlockFactory blockFactory) {
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
        return values.get(position);
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
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(values);
    }

    @Override
    public BooleanVector filter(int... positions) {
        final BitArray filtered = new BitArray(positions.length, blockFactory.bigArrays());
        for (int i = 0; i < positions.length; i++) {
            if (values.get(positions[i])) {
                filtered.set(i);
            }
        }
        return new BooleanBigArrayVector(filtered, positions.length, blockFactory);
    }

    @Override
    public void close() {
        if (released) {
            throw new IllegalStateException("can't release already released vector [" + this + "]");
        }
        released = true;
        values.close();
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
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + values + ']';
    }
}
