/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;

/**
 * Builder for {@link DoubleVector}s that never grows. Prefer this to
 * {@link DoubleVectorBuilder} if you know the precise size up front because
 * it's faster.
 * This class is generated. Edit {@code X-VectorFixedBuilder.java.st} instead.
 */
public final class DoubleVectorFixedBuilder implements DoubleVector.FixedBuilder {
    private final BlockFactory blockFactory;
    private final double[] values;
    private final int size;
    private final long preAdjustedBytes;
    /**
     * The next value to write into. {@code -1} means the vector has already
     * been built.
     */
    private int nextIndex;

    private boolean closed;

    private final boolean enablePooling;

    DoubleVectorFixedBuilder(int size, BlockFactory blockFactory, boolean enablePooling) {
        this.blockFactory = blockFactory;
        this.size = size;
        this.enablePooling = enablePooling;
        if (enablePooling) {
            // a pooled array can be longer than size and is not zeroed
            values = blockFactory.acquireDoubles(size);
            preAdjustedBytes = LocalArrayPool.usedBytesForDoubles(values.length);
        } else {
            preAdjustedBytes = ramBytesUsed(size);
            blockFactory.adjustBreaker(preAdjustedBytes);
            values = new double[size];
        }
    }

    @Override
    public DoubleVectorFixedBuilder appendDouble(double value) {
        values[nextIndex++] = value;
        return this;
    }

    @Override
    public DoubleVectorFixedBuilder appendDouble(int idx, double value) {
        values[idx] = value;
        return this;
    }

    private static long ramBytesUsed(int size) {
        return size == 1
            ? ConstantDoubleVector.RAM_BYTES_USED
            : DoubleArrayVector.BASE_RAM_BYTES_USED + RamUsageEstimator.alignObjectSize(
                (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) size * Double.BYTES
            );
    }

    @Override
    public long estimatedBytes() {
        return ramBytesUsed(size);
    }

    @Override
    public DoubleVector build() {
        if (closed) {
            throw new IllegalStateException("already closed");
        }
        DoubleVector vector;
        if (size == 1) {
            vector = blockFactory.newConstantDoubleVector(values[0], 1, preAdjustedBytes);
        } else {
            vector = blockFactory.newDoubleArrayVector(values, size, preAdjustedBytes);
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
                blockFactory.recycleDoubles(values);
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
