/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.MetadataReader;
import org.elasticsearch.index.codec.tsdb.pipeline.MetadataWriter;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecStage;

import java.io.IOException;

public final class PatchedPForCodecStage implements NumericCodecStage {

    private static final int DEFAULT_MAX_EXCEPTION_PERCENT = 10;
    private static final int MIN_EXCEPTIONS = 2;
    private static final int MAX_EXCEPTIONS = 32;

    public static final PatchedPForCodecStage INSTANCE = new PatchedPForCodecStage();

    private final int maxExceptionPercent;

    public PatchedPForCodecStage() {
        this(DEFAULT_MAX_EXCEPTION_PERCENT);
    }

    public PatchedPForCodecStage(int maxExceptionPercent) {
        assert maxExceptionPercent >= 1 && maxExceptionPercent <= 50 : "maxExceptionPercent must be in [1, 50]: " + maxExceptionPercent;
        this.maxExceptionPercent = maxExceptionPercent;
    }

    @Override
    public byte id() {
        return StageId.PATCHED_PFOR.id;
    }

    @Override
    public String name() {
        return "patched-pfor";
    }

    @Override
    public int encode(long[] values, int valueCount, EncodingContext context) throws IOException {
        if (valueCount == 0) {
            return valueCount;
        }

        int maxExceptions = computeMaxExceptions(valueCount);

        int maxBitsNeeded = 0;
        for (int i = 0; i < valueCount; i++) {
            if (values[i] < 0) {
                return valueCount;
            }
            int bits = bitsRequired(values[i]);
            maxBitsNeeded = Math.max(maxBitsNeeded, bits);
        }

        int optimalBits = findOptimalBitWidth(values, valueCount, maxBitsNeeded, maxExceptions);

        if (optimalBits >= maxBitsNeeded) {
            // NOTE: Skip PatchedPFor if no bit-width reduction possible.
            // Metadata overhead (optimalBits + exceptions) would exceed any savings.
            return valueCount;
        }

        long maxForOptimalBits = (1L << optimalBits) - 1;

        int numExceptions = 0;
        for (int i = 0; i < valueCount; i++) {
            if (values[i] > maxForOptimalBits) {
                numExceptions++;
            }
        }

        if (numExceptions > maxExceptions) {
            // NOTE: Each exception stores position (VInt) + value (VLong).
            // Too many exceptions negates the bit-width reduction savings.
            return valueCount;
        }

        if (numExceptions == 0) {
            return valueCount;
        }

        final MetadataWriter meta = context.metadata();
        meta.writeVInt(optimalBits);
        meta.writeVInt(numExceptions);

        for (int i = 0; i < valueCount; i++) {
            if (values[i] > maxForOptimalBits) {
                meta.writeVInt(i);
                meta.writeVLong(values[i]);
                // NOTE: Replace exception with previous value (delta-friendly placeholder).
                // This keeps the sequence monotonic for downstream stages like Delta,
                // resulting in zero deltas at exception positions.
                values[i] = (i > 0) ? values[i - 1] : 0;
            }
        }

        return valueCount;
    }

    @Override
    public int decode(long[] values, int valueCount, DecodingContext context) throws IOException {
        final MetadataReader meta = context.metadata();
        @SuppressWarnings("unused")
        int bitWidth = meta.readVInt();
        int numExceptions = meta.readVInt();
        for (int i = 0; i < numExceptions; i++) {
            int pos = meta.readVInt();
            long value = meta.readVLong();
            values[pos] = value;
        }
        return valueCount;
    }

    private int computeMaxExceptions(int valueCount) {
        int computed = (valueCount * maxExceptionPercent) / 100;
        return Math.max(MIN_EXCEPTIONS, Math.min(MAX_EXCEPTIONS, computed));
    }

    private int findOptimalBitWidth(long[] values, int valueCount, int maxBits, int maxExceptions) {
        int[] histogram = new int[65];
        for (int i = 0; i < valueCount; i++) {
            int bits = bitsRequired(values[i]);
            histogram[bits]++;
        }

        int valuesAtOrBelow = 0;
        for (int bits = 0; bits <= maxBits; bits++) {
            valuesAtOrBelow += histogram[bits];
            int exceptions = valueCount - valuesAtOrBelow;
            if (exceptions <= maxExceptions && valuesAtOrBelow > 0) {
                return bits;
            }
        }

        return maxBits;
    }

    private static int bitsRequired(long value) {
        if (value == 0) {
            return 0;
        }
        return 64 - Long.numberOfLeadingZeros(value);
    }
}
