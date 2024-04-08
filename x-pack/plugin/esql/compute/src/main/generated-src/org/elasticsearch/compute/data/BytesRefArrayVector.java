/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.core.Releasables;

import java.io.IOException;

/**
 * Vector implementation that stores an array of BytesRef values.
 * Does not take ownership of the given {@link BytesRefArray} and does not adjust circuit breakers to account for it.
 * This class is generated. Do not edit it.
 */
final class BytesRefArrayVector extends AbstractVector implements BytesRefVector {

    static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(BytesRefArrayVector.class)
        // TODO: remove these extra bytes once `asBlock` returns a block with a separate reference to the vector.
        + RamUsageEstimator.shallowSizeOfInstance(BytesRefVectorBlock.class);

    private final BytesRefArray values;

    BytesRefArrayVector(BytesRefArray values, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.values = values;
    }

    static BytesRefArrayVector readArrayVector(int positions, StreamInput in, BlockFactory blockFactory) throws IOException {
        final BytesRefArray values = new BytesRefArray(in, blockFactory.bigArrays());
        boolean success = false;
        try {
            final var block = new BytesRefArrayVector(values, positions, blockFactory);
            blockFactory.adjustBreaker(block.ramBytesUsed() - values.bigArraysRamBytesUsed());
            success = true;
            return block;
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
    public BytesRefBlock asBlock() {
        return new BytesRefVectorBlock(this);
    }

    @Override
    public BytesRef getBytesRef(int position, BytesRef dest) {
        return values.get(position, dest);
    }

    @Override
    public ElementType elementType() {
        return ElementType.BYTES_REF;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public BytesRefVector filter(int... positions) {
        final var scratch = new BytesRef();
        try (BytesRefVector.Builder builder = blockFactory().newBytesRefVectorBuilder(positions.length)) {
            for (int pos : positions) {
                builder.appendBytesRef(values.get(pos, scratch));
            }
            return builder.build();
        }
    }

    public static long ramBytesEstimated(BytesRefArray values) {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(values);
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesEstimated(values);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BytesRefVector that) {
            return BytesRefVector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return BytesRefVector.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ']';
    }

    @Override
    public void closeInternal() {
        // The circuit breaker that tracks the values {@link BytesRefArray} is adjusted outside
        // of this class.
        blockFactory().adjustBreaker(-ramBytesUsed() + values.bigArraysRamBytesUsed());
        Releasables.closeExpectNoException(values);
    }
}
