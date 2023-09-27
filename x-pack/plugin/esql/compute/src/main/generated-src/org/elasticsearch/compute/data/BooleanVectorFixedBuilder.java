/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;

/**
 * Builder for {@link BooleanVector}s that never grows. Prefer this to
 * {@link BooleanVectorBuilder} if you know the precise size up front because
 * it's faster.
 * This class is generated. Do not edit it.
 */
final class BooleanVectorFixedBuilder implements BooleanVector.FixedBuilder {
    private final BlockFactory blockFactory;
    private final boolean[] values;
    /**
     * The next byte to write into. {@code -1} means the vector has already
     * been built.
     */
    private int i;

    BooleanVectorFixedBuilder(int size, BlockFactory blockFactory) {
        blockFactory.adjustBreaker(size(size), false);
        this.blockFactory = blockFactory;
        this.values = new boolean[size];
    }

    @Override
    public BooleanVectorFixedBuilder appendBoolean(boolean value) {
        values[i++] = value;
        return this;
    }

    private static long size(int size) {
        return size == 1
            ? ConstantBooleanVector.RAM_BYTES_USED
            : BooleanArrayVector.BASE_RAM_BYTES_USED + RamUsageEstimator.alignObjectSize(
                (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + size * Byte.BYTES
            );
    }

    @Override
    public BooleanVector build() {
        if (i < 0) {
            throw new IllegalStateException("already closed");
        }
        if (i != values.length) {
            throw new IllegalStateException("expected to write [" + values.length + "] entries but wrote [" + i + "]");
        }
        i = -1;
        if (values.length == 1) {
            return new ConstantBooleanVector(values[0], 1, blockFactory);
        }
        return new BooleanArrayVector(values, values.length, blockFactory);
    }
}
