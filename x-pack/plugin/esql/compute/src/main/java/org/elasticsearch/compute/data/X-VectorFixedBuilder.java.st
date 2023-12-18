/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;

/**
 * Builder for {@link $Type$Vector}s that never grows. Prefer this to
 * {@link $Type$VectorBuilder} if you know the precise size up front because
 * it's faster.
 * This class is generated. Do not edit it.
 */
final class $Type$VectorFixedBuilder implements $Type$Vector.FixedBuilder {
    private final BlockFactory blockFactory;
    private final $type$[] $arrayValues$;
    private final long preAdjustedBytes;
    /**
     * The next value to write into. {@code -1} means the vector has already
     * been built.
     */
    private int nextIndex;

    $Type$VectorFixedBuilder(int size, BlockFactory blockFactory) {
        preAdjustedBytes = ramBytesUsed(size);
        blockFactory.adjustBreaker(preAdjustedBytes, false);
        this.blockFactory = blockFactory;
$if(Point)$
        this.xValues = new $type$[size];
        this.yValues = new $type$[size];
$else$
        this.values = new $type$[size];
$endif$
    }

    @Override
    public $Type$VectorFixedBuilder append$Type$($typeValue$) {
$if(Point)$
        xValues[nextIndex++] = x;
        yValues[nextIndex++] = y;
$else$
        values[nextIndex++] = value;
$endif$
        return this;
    }

    private static long ramBytesUsed(int size) {
        return size == 1
            ? Constant$Type$Vector.RAM_BYTES_USED
            : $Type$ArrayVector.BASE_RAM_BYTES_USED + RamUsageEstimator.alignObjectSize(
                (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + size * $BYTES$
            );
    }

    private int valuesLength() {
$if(Point)$
        return xValues.length;
$else$
        return values.length;
$endif$
    }

    @Override
    public $Type$Vector build() {
        if (nextIndex < 0) {
            throw new IllegalStateException("already closed");
        }
        if (nextIndex != valuesLength()) {
            throw new IllegalStateException("expected to write [" + valuesLength() + "] entries but wrote [" + nextIndex + "]");
        }
        nextIndex = -1;
        $Type$Vector vector;
        if (valuesLength() == 1) {
            vector = blockFactory.newConstant$Type$BlockWith($if(Point)$xValues[0], yValues[0]$else$values[0]$endif$, 1, preAdjustedBytes).asVector();
        } else {
            vector = blockFactory.new$Type$ArrayVector($arrayValues$, valuesLength(), preAdjustedBytes);
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
