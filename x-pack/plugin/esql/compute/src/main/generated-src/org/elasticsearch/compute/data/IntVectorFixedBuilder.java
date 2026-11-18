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
    private final int size;
    private final long preAdjustedBytes;
    /**
     * The next value to write into. {@code -1} means the vector has already
     * been built.
     */
    private int nextIndex;

    private boolean closed;

    private final boolean enablePooling;

    IntVectorFixedBuilder(int size, BlockFactory blockFactory, boolean enablePooling) {
        this.blockFactory = blockFactory;
        this.size = size;
        this.enablePooling = enablePooling;
        if (enablePooling) {
            // a pooled array can be longer than size and is not zeroed
            values = blockFactory.acquireInts(size);
            preAdjustedBytes = LocalArrayPool.usedBytesForInts(values.length);
        } else {
            preAdjustedBytes = ramBytesUsed(size);
            blockFactory.adjustBreaker(preAdjustedBytes);
            values = new int[size];
        }
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
                (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) size * Integer.BYTES
            );
    }

    @Override
    public long estimatedBytes() {
        return ramBytesUsed(size);
    }

    @Override
    public IntVector build() {
        if (closed) {
            throw new IllegalStateException("already closed");
        }
        IntVector vector;
        if (size == 1) {
            vector = blockFactory.newConstantIntVector(values[0], 1, preAdjustedBytes);
        } else {
            vector = blockFactory.newIntArrayVector(values, size, preAdjustedBytes);
        }
        closed = true;
        if (enablePooling) {
            vector.enablePooling();
            return vector;
        }
        assert vector.ramBytesUsed() == preAdjustedBytes : "fixed Builders should estimate the exact ram bytes used";
        return vector;
    }

    @Override
    public void close() {
        if (closed == false) {
            // If nextIndex < 0 we've already built the vector
            closed = true;
            if (enablePooling) {
                blockFactory.recycleInts(values);
                return;
            }
            blockFactory.adjustBreaker(-preAdjustedBytes);
        }
    }

    @Override
    public boolean isReleased() {
        return closed;
    }
}
