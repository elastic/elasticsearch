/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;

import java.util.Objects;

final class IntRangeVector extends AbstractVector implements IntVector {
    static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(IntRangeVector.class);

    private final int startInclusive;
    private final int endExclusive;

    IntRangeVector(BlockFactory blockFactory, int startInclusive, int endExclusive) {
        super(computePositionCount(startInclusive, endExclusive), blockFactory);
        this.startInclusive = startInclusive;
        this.endExclusive = endExclusive;
    }

    private static int computePositionCount(int startInclusive, int endExclusive) {
        if (endExclusive < startInclusive) {
            throw new IllegalArgumentException(
                "startInclusive must not be greater than endExclusive; got [" + startInclusive + ", " + endExclusive + ")"
            );
        }
        return endExclusive - startInclusive;
    }

    @Override
    public int getInt(int position) {
        Objects.checkIndex(position, getPositionCount());
        return startInclusive + position;
    }

    @Override
    public IntBlock asBlock() {
        return new IntVectorBlock(this);
    }

    @Override
    public IntVector filter(boolean mayContainDuplicates, int... positions) {
        try (var builder = blockFactory().newIntVectorFixedBuilder(positions.length)) {
            for (int i = 0; i < positions.length; i++) {
                int p = positions[i];
                builder.appendInt(i, getInt(p));
            }
            return builder.build();
        }
    }

    @Override
    public IntBlock keepMask(BooleanVector mask) {
        if (getPositionCount() == 0) {
            incRef();
            return new IntVectorBlock(this);
        }
        if (mask.isConstant()) {
            if (mask.getBoolean(0)) {
                incRef();
                return new IntVectorBlock(this);
            }
            return (IntBlock) blockFactory().newConstantNullBlock(getPositionCount());
        }
        try (var builder = blockFactory().newIntBlockBuilder(getPositionCount())) {
            // TODO if X-ArrayBlock used BooleanVector for it's null mask then we could shuffle references here.
            for (int p = 0; p < getPositionCount(); p++) {
                if (mask.getBoolean(p)) {
                    builder.appendInt(getInt(p));
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    @Override
    public ReleasableIterator<? extends IntBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return new IntLookup(asBlock(), positions, targetBlockSize);
    }

    @Override
    public int min() {
        return getPositionCount() == 0 ? Integer.MAX_VALUE : startInclusive;
    }

    @Override
    public int max() {
        return getPositionCount() == 0 ? Integer.MIN_VALUE : endExclusive - 1;
    }

    @Override
    public ElementType elementType() {
        return ElementType.INT;
    }

    @Override
    public boolean isConstant() {
        return getPositionCount() == 1;
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED;
    }

    @Override
    public String toString() {
        return "IntRangeVector{" + "startInclusive=" + startInclusive + ", endExclusive=" + endExclusive + '}';
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
}
