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
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;

import java.io.IOException;

/**
 * Vector implementation that defers to an enclosed {@link BitArray}.
 * Does not take ownership of the array and does not adjust circuit breakers to account for it.
 * This class is generated. Do not edit it.
 */
public final class BooleanBigArrayVector extends AbstractVector implements BooleanVector, Releasable {

    private static final long BASE_RAM_BYTES_USED = 0; // FIXME

    private final BitArray values;

    public BooleanBigArrayVector(BitArray values, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.values = values;
    }

    static BooleanBigArrayVector readArrayVector(int positions, StreamInput in, BlockFactory blockFactory) throws IOException {
        BitArray values = new BitArray(blockFactory.bigArrays(), true, in);
        boolean success = false;
        try {
            BooleanBigArrayVector vector = new BooleanBigArrayVector(values, positions, blockFactory);
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
    public BooleanBlock asBlock() {
        return new BooleanVectorBlock(this);
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
        var blockFactory = blockFactory();
        final BitArray filtered = new BitArray(positions.length, blockFactory.bigArrays());
        for (int i = 0; i < positions.length; i++) {
            if (values.get(positions[i])) {
                filtered.set(i);
            }
        }
        return new BooleanBigArrayVector(filtered, positions.length, blockFactory);
    }

    @Override
    public BooleanBlock keepMask(BooleanVector mask) {
        if (getPositionCount() == 0) {
            incRef();
            return new BooleanVectorBlock(this);
        }
        if (mask.isConstant()) {
            if (mask.getBoolean(0)) {
                incRef();
                return new BooleanVectorBlock(this);
            }
            return (BooleanBlock) blockFactory().newConstantNullBlock(getPositionCount());
        }
        try (BooleanBlock.Builder builder = blockFactory().newBooleanBlockBuilder(getPositionCount())) {
            // TODO if X-ArrayBlock used BooleanVector for it's null mask then we could shuffle references here.
            for (int p = 0; p < getPositionCount(); p++) {
                if (mask.getBoolean(p)) {
                    builder.appendBoolean(getBoolean(p));
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    @Override
    public ReleasableIterator<BooleanBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return new BooleanLookup(asBlock(), positions, targetBlockSize);
    }

    @Override
    public void closeInternal() {
        // The circuit breaker that tracks the values {@link BitArray} is adjusted outside
        // of this class.
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
