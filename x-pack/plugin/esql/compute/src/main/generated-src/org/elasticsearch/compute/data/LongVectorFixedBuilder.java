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
 * This class is generated. Do not edit it.
 */
final class LongVectorFixedBuilder implements LongVector.FixedBuilder {
    private final BlockFactory blockFactory;
    private final long[] values;
    /**
     * The next value to write into. {@code -1} means the vector has already
     * been built.
     */
    private int nextIndex;

    LongVectorFixedBuilder(int size, BlockFactory blockFactory) {
        blockFactory.adjustBreaker(ramBytesUsed(size), false);
        this.blockFactory = blockFactory;
        this.values = new long[size];
    }

    @Override
    public LongVectorFixedBuilder appendLong(long value) {
        values[nextIndex++] = value;
        return this;
    }

    private static long ramBytesUsed(int size) {
        return size == 1
            ? ConstantLongVector.RAM_BYTES_USED
            : LongArrayVector.BASE_RAM_BYTES_USED + RamUsageEstimator.alignObjectSize(
                (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + size * Long.BYTES
            );
    }

    @Override
    public LongVector build() {
        if (nextIndex < 0) {
            throw new IllegalStateException("already closed");
        }
        if (nextIndex != values.length) {
            throw new IllegalStateException("expected to write [" + values.length + "] entries but wrote [" + nextIndex + "]");
        }
        nextIndex = -1;
        if (values.length == 1) {
            return new ConstantLongVector(values[0], 1, blockFactory);
        }
        return new LongArrayVector(values, values.length, blockFactory);
    }

    @Override
    public void close() {
        if (nextIndex >= 0) {
            // If nextIndex < 0 we've already built the vector
            blockFactory.adjustBreaker(-ramBytesUsed(values.length), false);
        }
    }
}
