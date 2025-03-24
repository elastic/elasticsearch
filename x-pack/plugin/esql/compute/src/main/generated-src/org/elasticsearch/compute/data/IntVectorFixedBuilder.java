/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;

/**
 * Builder for {@link IntVector}s that never grows. Prefer this to
 * {@link IntVectorBuilder} if you know the precise size up front because
 * it's faster.
 * This class is generated. Edit {@code X-VectorFixedBuilder.java.st} instead.
 */
public final class IntVectorFixedBuilder implements IntVector.FixedBuilder {
    private final BlockFactory blockFactory;
    private final int[] values;
    private final long preAdjustedBytes;
    /**
     * The next value to write into. {@code -1} means the vector has already
     * been built.
     */
    private int nextIndex;

    private boolean closed;

    IntVectorFixedBuilder(int size, BlockFactory blockFactory) {
        preAdjustedBytes = ramBytesUsed(size);
        blockFactory.adjustBreaker(preAdjustedBytes);
        this.blockFactory = blockFactory;
        this.values = new int[size];
    }

    @Override
    public IntVectorFixedBuilder appendInt(int value) {
        values[nextIndex++] = value;
        return this;
    }

    @Override
    public IntVectorFixedBuilder appendInt(int idx, int value) {
        values[idx] = value;
        return this;
    }

    private static long ramBytesUsed(int size) {
        return size == 1
            ? ConstantIntVector.RAM_BYTES_USED
            : IntArrayVector.BASE_RAM_BYTES_USED + RamUsageEstimator.alignObjectSize(
                (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + size * Integer.BYTES
            );
    }

    @Override
    public long estimatedBytes() {
        return ramBytesUsed(values.length);
    }

    @Override
    public IntVector build() {
        if (closed) {
            throw new IllegalStateException("already closed");
        }
        closed = true;
        IntVector vector;
        if (values.length == 1) {
            vector = blockFactory.newConstantIntBlockWith(values[0], 1, preAdjustedBytes).asVector();
        } else {
            vector = blockFactory.newIntArrayVector(values, values.length, preAdjustedBytes);
        }
        assert vector.ramBytesUsed() == preAdjustedBytes : "fixed Builders should estimate the exact ram bytes used";
        return vector;
    }

    @Override
    public void close() {
        if (closed == false) {
            // If nextIndex < 0 we've already built the vector
            closed = true;
            blockFactory.adjustBreaker(-preAdjustedBytes);
        }
    }

    public boolean isReleased() {
        return closed;
    }
}
