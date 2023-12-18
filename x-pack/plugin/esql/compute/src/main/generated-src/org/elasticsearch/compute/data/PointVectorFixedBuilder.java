/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;

/**
 * Builder for {@link PointVector}s that never grows. Prefer this to
 * {@link PointVectorBuilder} if you know the precise size up front because
 * it's faster.
 * This class is generated. Do not edit it.
 */
final class PointVectorFixedBuilder implements PointVector.FixedBuilder {
    private final BlockFactory blockFactory;
    private final double[] xValues, yValues;
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
        this.xValues = new double[size];
        this.yValues = new double[size];
    }

    @Override
    public PointVectorFixedBuilder appendPoint(double x, double y) {
        xValues[nextIndex++] = x;
        yValues[nextIndex++] = y;
        return this;
    }

    private static long ramBytesUsed(int size) {
        return size == 1
            ? ConstantPointVector.RAM_BYTES_USED
            : PointArrayVector.BASE_RAM_BYTES_USED + RamUsageEstimator.alignObjectSize(
                (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + size * 16
            );
    }

    private int valuesLength() {
        return xValues.length;
    }

    @Override
    public PointVector build() {
        if (nextIndex < 0) {
            throw new IllegalStateException("already closed");
        }
        if (nextIndex != valuesLength()) {
            throw new IllegalStateException("expected to write [" + valuesLength() + "] entries but wrote [" + nextIndex + "]");
        }
        nextIndex = -1;
        PointVector vector;
        if (valuesLength() == 1) {
            vector = blockFactory.newConstantPointBlockWith(xValues[0], yValues[0], 1, preAdjustedBytes).asVector();
        } else {
            vector = blockFactory.newPointArrayVector(xValues, yValues, valuesLength(), preAdjustedBytes);
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
