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

import java.io.IOException;
import java.util.Arrays;

/**
 * Vector implementation that stores an array of boolean values.
 * This class is generated. Do not edit it.
 */
final class BooleanArrayVector extends AbstractVector implements BooleanVector {

    static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(BooleanArrayVector.class)
        // TODO: remove these extra bytes once `asBlock` returns a block with a separate reference to the vector.
        + RamUsageEstimator.shallowSizeOfInstance(BooleanVectorBlock.class);

    private final boolean[] values;

    BooleanArrayVector(boolean[] values, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.values = values;
    }

    static BooleanArrayVector readArrayVector(int positions, StreamInput in, BlockFactory blockFactory) throws IOException {
        final long preAdjustedBytes = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) positions * Byte.BYTES;
        blockFactory.adjustBreaker(preAdjustedBytes);
        boolean success = false;
        try {
            boolean[] values = new boolean[positions];
            for (int i = 0; i < positions; i++) {
                values[i] = in.readBoolean();
            }
            final var block = new BooleanArrayVector(values, positions, blockFactory);
            blockFactory.adjustBreaker(block.ramBytesUsed() - preAdjustedBytes);
            success = true;
            return block;
        } finally {
            if (success == false) {
                blockFactory.adjustBreaker(-preAdjustedBytes);
            }
        }
    }

    void writeArrayVector(int positions, StreamOutput out) throws IOException {
        // TODO: One bit for each boolean
        for (int i = 0; i < positions; i++) {
            out.writeBoolean(values[i]);
        }
    }

    @Override
    public BooleanBlock asBlock() {
        return new BooleanVectorBlock(this);
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
        try (BooleanVector.Builder builder = blockFactory().newBooleanVectorBuilder(positions.length)) {
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
