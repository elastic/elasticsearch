/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.geo.SpatialPoint;

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF;

/**
 * Builder for {@link PointVector}s that never grows. Prefer this to
 * {@link PointVectorBuilder} if you know the precise size up front because
 * it's faster.
 * This class is generated. Do not edit it.
 */
final class PointVectorFixedBuilder implements PointVector.FixedBuilder {
    private final BlockFactory blockFactory;
    private final SpatialPoint[] values;
    private final long preAdjustedBytes;
    /**
     * The next value to write into. {@code -1} means the vector has already
     * been built.
     */
    private int nextIndex;

    PointVectorFixedBuilder(int size, BlockFactory blockFactory) {
        preAdjustedBytes = ramBytesUsed(size);
        blockFactory.adjustBreaker(preAdjustedBytes, false);
        this.blockFactory = blockFactory;
        this.values = new SpatialPoint[size];
    }

    @Override
    public PointVectorFixedBuilder appendPoint(SpatialPoint value) {
        values[nextIndex++] = value;
        return this;
    }

    private static long ramBytesUsed(int size) {
        return size == 1
            ? ConstantPointVector.RAM_BYTES_USED
            : PointArrayVector.BASE_RAM_BYTES_USED + RamUsageEstimator.alignObjectSize(
                (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) size * (16 + NUM_BYTES_OBJECT_REF)
            );
    }

    @Override
    public PointVector build() {
        if (nextIndex < 0) {
            throw new IllegalStateException("already closed");
        }
        if (nextIndex != values.length) {
            throw new IllegalStateException("expected to write [" + values.length + "] entries but wrote [" + nextIndex + "]");
        }
        nextIndex = -1;
        PointVector vector;
        if (values.length == 1) {
            vector = blockFactory.newConstantPointBlockWith(values[0], 1, preAdjustedBytes).asVector();
        } else {
            vector = blockFactory.newPointArrayVector(values, values.length, preAdjustedBytes);
        }
        assert vector.ramBytesUsed() == preAdjustedBytes : "fixed Builders should estimate the exact ram bytes used";
        return vector;
    }

    @Override
    public void close() {
        if (nextIndex >= 0) {
            // If nextIndex < 0 we've already built the vector
            nextIndex = -1;
            blockFactory.adjustBreaker(-preAdjustedBytes, false);
        }
    }

    boolean isReleased() {
        return nextIndex < 0;
    }
}
