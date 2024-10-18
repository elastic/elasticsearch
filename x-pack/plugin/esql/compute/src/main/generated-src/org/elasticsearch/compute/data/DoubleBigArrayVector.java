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
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;

import java.io.IOException;

/**
 * Vector implementation that defers to an enclosed {@link DoubleArray}.
 * Does not take ownership of the array and does not adjust circuit breakers to account for it.
 * This class is generated. Do not edit it.
 */
public final class DoubleBigArrayVector extends AbstractVector implements DoubleVector, Releasable {

    private static final long BASE_RAM_BYTES_USED = 0; // FIXME

    private final DoubleArray values;

    public DoubleBigArrayVector(DoubleArray values, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.values = values;
    }

    static DoubleBigArrayVector readArrayVector(int positions, StreamInput in, BlockFactory blockFactory) throws IOException {
        DoubleArray values = blockFactory.bigArrays().newDoubleArray(positions, false);
        boolean success = false;
        try {
            values.fillWith(in);
            DoubleBigArrayVector vector = new DoubleBigArrayVector(values, positions, blockFactory);
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
    public DoubleBlock asBlock() {
        return new DoubleVectorBlock(this);
    }

    @Override
    public double getDouble(int position) {
        return values.get(position);
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
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(values);
    }

    @Override
    public DoubleVector filter(int... positions) {
        var blockFactory = blockFactory();
        final DoubleArray filtered = blockFactory.bigArrays().newDoubleArray(positions.length);
        for (int i = 0; i < positions.length; i++) {
            filtered.set(i, values.get(positions[i]));
        }
        return new DoubleBigArrayVector(filtered, positions.length, blockFactory);
    }

    @Override
    public DoubleBlock keepMask(BooleanVector mask) {
        if (getPositionCount() == 0) {
            incRef();
            return new DoubleVectorBlock(this);
        }
        if (mask.isConstant()) {
            if (mask.getBoolean(0)) {
                incRef();
                return new DoubleVectorBlock(this);
            }
            return (DoubleBlock) blockFactory().newConstantNullBlock(getPositionCount());
        }
        try (DoubleBlock.Builder builder = blockFactory().newDoubleBlockBuilder(getPositionCount())) {
            // TODO if X-ArrayBlock used BooleanVector for it's null mask then we could shuffle references here.
            for (int p = 0; p < getPositionCount(); p++) {
                if (mask.getBoolean(p)) {
                    builder.appendDouble(getDouble(p));
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    @Override
    public ReleasableIterator<DoubleBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return new DoubleLookup(asBlock(), positions, targetBlockSize);
    }

    @Override
    public void closeInternal() {
        // The circuit breaker that tracks the values {@link DoubleArray} is adjusted outside
        // of this class.
        values.close();
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
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + values + ']';
    }
}
