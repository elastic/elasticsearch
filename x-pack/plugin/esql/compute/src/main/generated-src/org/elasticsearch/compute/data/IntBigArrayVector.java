/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;

import java.io.IOException;

/**
 * Vector implementation that defers to an enclosed {@link IntArray}.
 * Does not take ownership of the array and does not adjust circuit breakers to account for it.
 * This class is generated. Do not edit it.
 */
public final class IntBigArrayVector extends AbstractVector implements IntVector, Releasable {

    private static final long BASE_RAM_BYTES_USED = 0; // FIXME

    private final IntArray values;

    /**
     * The minimum value in the block.
     */
    private Integer min;

    /**
     * The minimum value in the block.
     */
    private Integer max;

    public IntBigArrayVector(IntArray values, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.values = values;
    }

    static IntBigArrayVector readArrayVector(int positions, StreamInput in, BlockFactory blockFactory) throws IOException {
        IntArray values = blockFactory.bigArrays().newIntArray(positions, false);
        boolean success = false;
        try {
            values.fillWith(in);
            IntBigArrayVector vector = new IntBigArrayVector(values, positions, blockFactory);
            blockFactory.adjustBreaker(vector.ramBytesUsed() - RamUsageEstimator.sizeOf(values));
            success = true;
            return vector;
        } finally {
            if (success == false) {
                values.close();
            }
        }
    }

    void writeArrayVector(int positions, StreamOutput out) throws IOException {
        values.writeTo(out);
    }

    @Override
    public IntBlock asBlock() {
        return new IntVectorBlock(this);
    }

    @Override
    public int getInt(int position) {
        return values.get(position);
    }

    /**
     * The minimum value in the block.
     */
    @Override
    public int min() {
        if (min == null) {
            int v = values.get(0);
            for (int i = 1; i < getPositionCount(); i++) {
                v = Math.min(v, values.get(i));
            }
            min = v;
        }
        return min;
    }

    /**
     * The maximum value in the block.
     */
    @Override
    public int max() {
        if (max == null) {
            int v = values.get(0);
            for (int i = 1; i < getPositionCount(); i++) {
                v = Math.max(v, values.get(i));
            }
            max = v;
        }
        return max;
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
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(values);
    }

    @Override
    public IntVector filter(int... positions) {
        var blockFactory = blockFactory();
        final IntArray filtered = blockFactory.bigArrays().newIntArray(positions.length);
        for (int i = 0; i < positions.length; i++) {
            filtered.set(i, values.get(positions[i]));
        }
        return new IntBigArrayVector(filtered, positions.length, blockFactory);
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
        try (IntBlock.Builder builder = blockFactory().newIntBlockBuilder(getPositionCount())) {
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
    public ReleasableIterator<IntBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return new IntLookup(asBlock(), positions, targetBlockSize);
    }

    @Override
    public void closeInternal() {
        // The circuit breaker that tracks the values {@link IntArray} is adjusted outside
        // of this class.
        values.close();
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
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + values + ']';
    }
}
