/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;

/**
 * Builder for {@link FloatVector}s that never grows. Prefer this to
 * {@link FloatVectorBuilder} if you know the precise size up front because
 * it's faster.
 * This class is generated. Edit {@code X-VectorFixedBuilder.java.st} instead.
 */
public final class FloatVectorFixedBuilder implements FloatVector.FixedBuilder {
    private final BlockFactory blockFactory;
    private final float[] values;
    private final long preAdjustedBytes;
    /**
     * The next value to write into. {@code -1} means the vector has already
     * been built.
     */
    private int nextIndex;

    private boolean closed;

    FloatVectorFixedBuilder(int size, BlockFactory blockFactory) {
        preAdjustedBytes = ramBytesUsed(size);
        blockFactory.adjustBreaker(preAdjustedBytes);
        this.blockFactory = blockFactory;
        this.values = new float[size];
    }

    @Override
    public FloatVectorFixedBuilder appendFloat(float value) {
        values[nextIndex++] = value;
        return this;
    }

    @Override
    public FloatVectorFixedBuilder appendFloat(int idx, float value) {
        values[idx] = value;
        return this;
    }

    private static long ramBytesUsed(int size) {
        return size == 1
            ? ConstantFloatVector.RAM_BYTES_USED
            : FloatArrayVector.BASE_RAM_BYTES_USED + RamUsageEstimator.alignObjectSize(
                (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + size * Float.BYTES
            );
    }

    @Override
    public long estimatedBytes() {
        return ramBytesUsed(values.length);
    }

    @Override
    public FloatVector build() {
        if (closed) {
            throw new IllegalStateException("already closed");
        }
        closed = true;
        FloatVector vector;
        if (values.length == 1) {
            vector = blockFactory.newConstantFloatBlockWith(values[0], 1, preAdjustedBytes).asVector();
        } else {
            vector = blockFactory.newFloatArrayVector(values, values.length, preAdjustedBytes);
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
