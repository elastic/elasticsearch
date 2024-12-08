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
import org.elasticsearch.common.util.FloatArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;

import java.io.IOException;

/**
 * Vector implementation that defers to an enclosed {@link FloatArray}.
 * Does not take ownership of the array and does not adjust circuit breakers to account for it.
 * This class is generated. Do not edit it.
 */
public final class FloatBigArrayVector extends AbstractVector implements FloatVector, Releasable {

    private static final long BASE_RAM_BYTES_USED = 0; // FIXME

    private final FloatArray values;

    public FloatBigArrayVector(FloatArray values, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.values = values;
    }

    static FloatBigArrayVector readArrayVector(int positions, StreamInput in, BlockFactory blockFactory) throws IOException {
        throw new UnsupportedOperationException();
    }

    void writeArrayVector(int positions, StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FloatBlock asBlock() {
        return new FloatVectorBlock(this);
    }

    @Override
    public float getFloat(int position) {
        return values.get(position);
    }

    @Override
    public ElementType elementType() {
        return ElementType.FLOAT;
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
    public FloatVector filter(int... positions) {
        var blockFactory = blockFactory();
        final FloatArray filtered = blockFactory.bigArrays().newFloatArray(positions.length);
        for (int i = 0; i < positions.length; i++) {
            filtered.set(i, values.get(positions[i]));
        }
        return new FloatBigArrayVector(filtered, positions.length, blockFactory);
    }

    @Override
    public FloatBlock keepMask(BooleanVector mask) {
        if (getPositionCount() == 0) {
            incRef();
            return new FloatVectorBlock(this);
        }
        if (mask.isConstant()) {
            if (mask.getBoolean(0)) {
                incRef();
                return new FloatVectorBlock(this);
            }
            return (FloatBlock) blockFactory().newConstantNullBlock(getPositionCount());
        }
        try (FloatBlock.Builder builder = blockFactory().newFloatBlockBuilder(getPositionCount())) {
            // TODO if X-ArrayBlock used BooleanVector for it's null mask then we could shuffle references here.
            for (int p = 0; p < getPositionCount(); p++) {
                if (mask.getBoolean(p)) {
                    builder.appendFloat(getFloat(p));
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    @Override
    public ReleasableIterator<FloatBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return new FloatLookup(asBlock(), positions, targetBlockSize);
    }

    @Override
    public void closeInternal() {
        // The circuit breaker that tracks the values {@link FloatArray} is adjusted outside
        // of this class.
        values.close();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FloatVector that) {
            return FloatVector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return FloatVector.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + values + ']';
    }
}
