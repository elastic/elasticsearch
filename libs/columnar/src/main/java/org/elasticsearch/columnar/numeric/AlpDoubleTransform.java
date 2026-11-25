/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import java.io.IOException;

/**
 * ALP (Adaptive Lossless floating-Point) transform for double fields. Encodes each value as an
 * integer mantissa using a per-block exponent pair {@code (e, f)} chosen to minimize bit width;
 * values that fail to round-trip are stored as exceptions in params. Not thread-safe; each
 * pipeline must own its own instance. Frozen id {@code 0x04}.
 */
public final class AlpDoubleTransform implements BlockTransform {

    static final byte ID = 4;

    private final int[] efOut = new int[2];
    private final int[] candCounts = new int[AlpDoubleUtils.CAND_POOL_SIZE];
    private final int[] excPositions;
    private final long[] excValues;
    private final long[] sortableScratch;
    private final boolean[] nearConstStrideOut = new boolean[1];
    private int cachedE = -1;
    private int cachedF = -1;
    private int cachedMaxAllowed = -1;

    /**
     * Creates a transform sized for the given block size. Each pipeline must own its own instance.
     *
     * @param blockSize the number of values per block; must be at least one
     */
    public AlpDoubleTransform(int blockSize) {
        if (blockSize < 1) {
            throw new IllegalArgumentException("blockSize must be at least 1, got: " + blockSize);
        }
        this.excPositions = new int[blockSize];
        this.excValues = new long[blockSize];
        this.sortableScratch = new long[blockSize];
    }

    /** {@inheritDoc} */
    @Override
    public byte id() {
        return ID;
    }

    /** {@inheritDoc} */
    @Override
    public boolean tryEncode(long[] block, int valueCount, MetadataWriter params) throws IOException {
        assert valueCount >= 1 : "valueCount must be at least 1";
        assert valueCount <= excPositions.length
            : "valueCount (" + valueCount + ") must not exceed blockSize (" + excPositions.length + ")";

        if (cachedE >= 0) {
            System.arraycopy(block, 0, sortableScratch, 0, valueCount);
            nearConstStrideOut[0] = false;
            final int excCount = AlpDoubleUtils.alpTransformBlock(
                block,
                valueCount,
                cachedE,
                cachedF,
                excPositions,
                excValues,
                nearConstStrideOut
            );
            if (nearConstStrideOut[0]) {
                System.arraycopy(sortableScratch, 0, block, 0, valueCount);
                cachedE = -1;
                cachedF = -1;
                cachedMaxAllowed = -1;
                return false;
            }
            final int cacheMaxAllowed = (valueCount * AlpDoubleUtils.CACHE_VALIDATION_THRESHOLD) / 100;
            if (excCount <= cacheMaxAllowed && excCount <= cachedMaxAllowed) {
                writeAlpMetadata(excCount, cachedE, cachedF, params);
                return true;
            }
            System.arraycopy(sortableScratch, 0, block, 0, valueCount);
        }

        if (AlpDoubleUtils.hasNearConstantStride(block, valueCount)) {
            return false;
        }

        final int bestExceptions = AlpDoubleUtils.findBestEFForBlock(block, valueCount, efOut, candCounts);
        final int bestE = efOut[0];
        final int bestF = efOut[1];

        final int bitsSaved = AlpDoubleUtils.computeBitSavings(block, valueCount, bestE, bestF);
        if (bitsSaved <= 0) {
            return false;
        }
        final int maxAllowed = AlpDoubleUtils.maxExceptions(bitsSaved, valueCount, AlpDoubleUtils.DOUBLE_EXCEPTION_COST);
        if (bestExceptions > maxAllowed) {
            return false;
        }

        cachedE = bestE;
        cachedF = bestF;
        cachedMaxAllowed = maxAllowed;

        writeAlpBlock(block, valueCount, bestE, bestF, params);
        return true;
    }

    private void writeAlpBlock(final long[] block, final int valueCount, final int e, final int f, final MetadataWriter params)
        throws IOException {
        final int excCount = AlpDoubleUtils.alpTransformBlock(block, valueCount, e, f, excPositions, excValues, null);
        writeAlpMetadata(excCount, e, f, params);
    }

    private void writeAlpMetadata(final int excCount, final int e, final int f, final MetadataWriter params) throws IOException {
        params.writeByte((byte) e);
        params.writeByte((byte) f);
        params.writeVInt(excCount);
        for (int i = 0; i < excCount; i++) {
            params.writeVInt(excPositions[i]);
            params.writeLong(excValues[i]);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void decode(long[] block, int valueCount, MetadataReader params) throws IOException {
        assert valueCount >= 1 : "valueCount must be at least 1";
        final int e = params.readByte() & 0xFF;
        final int f = params.readByte() & 0xFF;
        final int excCount = params.readVInt();

        final double decodeMul = AlpDoubleUtils.POWERS_OF_TEN[f] * AlpDoubleUtils.NEG_POWERS_OF_TEN[e];
        for (int i = 0; i < valueCount; i++) {
            final long bits = Double.doubleToRawLongBits(block[i] * decodeMul);
            block[i] = bits ^ ((bits >> 63) >>> 1);
        }

        for (int i = 0; i < excCount; i++) {
            block[params.readVInt()] = params.readLong();
        }
    }
}
