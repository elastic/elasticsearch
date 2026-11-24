/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;

/**
 * Builder for {@link LongVector}s that never grows. Prefer this to
 * {@link LongVectorBuilder} if you know the precise size up front because
 * it's faster.
 * This class is generated. Edit {@code X-VectorFixedBuilder.java.st} instead.
 */
public final class LongVectorFixedBuilder implements LongVector.FixedBuilder {
    private final BlockFactory blockFactory;
    private final long[] values;
    private final int size;
    private final long preAdjustedBytes;
    /**
     * The next value to write into. {@code -1} means the vector has already
     * been built.
     */
    private int nextIndex;

    private boolean closed;

    private final boolean enablePooling;

    LongVectorFixedBuilder(int size, BlockFactory blockFactory, boolean enablePooling) {
        this.blockFactory = blockFactory;
        this.size = size;
        this.enablePooling = enablePooling;
        if (enablePooling) {
            // a pooled array can be longer than size and is not zeroed
            values = blockFactory.acquireLongs(size);
            preAdjustedBytes = LocalArrayPool.usedBytesForLongs(values.length);
        } else {
            preAdjustedBytes = ramBytesUsed(size);
            blockFactory.adjustBreaker(preAdjustedBytes);
            values = new long[size];
        }
    }

    @Override
    public LongVectorFixedBuilder appendLong(long value) {
        values[nextIndex++] = value;
        return this;
    }

    @Override
    public LongVectorFixedBuilder appendLong(int idx, long value) {
        values[idx] = value;
        return this;
    }

    private static long ramBytesUsed(int size) {
        return size == 1
            ? ConstantLongVector.RAM_BYTES_USED
            : LongArrayVector.BASE_RAM_BYTES_USED + RamUsageEstimator.alignObjectSize(
                (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) size * Long.BYTES
            );
    }

    @Override
    public long estimatedBytes() {
        return ramBytesUsed(size);
    }

    @Override
    public LongVector build() {
        if (closed) {
            throw new IllegalStateException("already closed");
        }
        LongVector vector;
        if (size == 1) {
            vector = blockFactory.newConstantLongVector(values[0], 1, preAdjustedBytes);
        } else {
            vector = blockFactory.newLongArrayVector(values, size, preAdjustedBytes);
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
                blockFactory.recycleLongs(values);
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
