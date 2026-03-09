/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.elasticsearch.index.codec.tsdb.DocValuesForUtil;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.MetadataWriter;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.TransformEncoder;

import java.io.IOException;

public final class PatchedPForEncodeStage implements TransformEncoder {

    private static final int DEFAULT_MAX_EXCEPTION_PERCENT = 10;
    private static final int HISTOGRAM_SIZE = 65;
    private static final int MIN_EXCEPTIONS = 2;

    private final int maxExceptionPercent;
    private final long[] scratchValues = new long[HISTOGRAM_SIZE];

    public PatchedPForEncodeStage() {
        this(DEFAULT_MAX_EXCEPTION_PERCENT);
    }

    public PatchedPForEncodeStage(int maxExceptionPercent) {
        assert maxExceptionPercent >= 1 && maxExceptionPercent <= 50 : "maxExceptionPercent must be in [1, 50]: " + maxExceptionPercent;
        this.maxExceptionPercent = maxExceptionPercent;
    }

    @Override
    public byte id() {
        return StageId.PATCHED_PFOR.id;
    }

    @Override
    public int maxMetadataBytes(int blockSize) {
        return 10 + computeMaxExceptions(blockSize) * 15;
    }

    @Override
    public int encode(final long[] values, int valueCount, final EncodingContext context) throws IOException {
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

        int optimalBits = findOptimalBitWidth(values, valueCount, maxBitsNeeded, maxExceptions, scratchValues);

        // NOTE: BitPack rounds bit widths to encoding-friendly buckets via roundBits()
        // (e.g. 25-32 all round to 32). We must compare rounded widths to determine
        // actual payload savings — raw bit widths can differ while producing identical
        // packed output, making PFor metadata pure overhead.
        final int roundedMaxBits = DocValuesForUtil.roundBits(maxBitsNeeded);
        final int roundedOptimalBits = DocValuesForUtil.roundBits(optimalBits);

        if (roundedOptimalBits >= roundedMaxBits) {
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
            return valueCount;
        }

        if (numExceptions == 0) {
            return valueCount;
        }

        // NOTE: Skip when patching doesn't clearly save bytes. Savings come from
        // reducing bitpack width from roundedMaxBits to roundedOptimalBits across all
        // values. Cost is the PFor header (2 bytes) plus ~5 bytes per exception (VInt
        // delta position + VLong value). We require savings >= 2x cost to account for
        // estimation error and the decode-time penalty (each exception is a branch
        // that breaks vectorization in the decoder).
        final int savedBytes = (roundedMaxBits - roundedOptimalBits) * valueCount / 8;
        final int patchCost = 2 + numExceptions * 5;
        if (savedBytes < patchCost * 2) {
            return valueCount;
        }

        // NOTE: Metadata layout: [optimalBits: VInt] [numExceptions: VInt]
        // followed by delta-encoded exception pairs: [delta-position: VInt] [value: VLong].
        // All values are non-negative (negative blocks are skipped), so VLong is safe.
        final MetadataWriter meta = context.metadata();
        meta.writeVInt(optimalBits);
        meta.writeVInt(numExceptions);

        // NOTE: Delta-encode exception positions for compactness. Exceptions are
        // written in ascending order (loop iterates i = 0..valueCount-1), so deltas
        // are always non-negative.
        int prevExcPos = 0;
        for (int i = 0; i < valueCount; i++) {
            if (values[i] > maxForOptimalBits) {
                meta.writeVInt(i - prevExcPos);
                meta.writeVLong(values[i]);
                prevExcPos = i;
                // NOTE: Replace exception with previous value (delta-friendly placeholder).
                // This keeps the sequence monotonic for downstream stages like Delta,
                // resulting in zero deltas at exception positions.
                values[i] = (i > 0) ? values[i - 1] : 0;
            }
        }

        return valueCount;
    }

    public static int encodeStatic(final PatchedPForEncodeStage stage, final long[] values, int valueCount, final EncodingContext context)
        throws IOException {
        return stage.encode(values, valueCount, context);
    }

    private int computeMaxExceptions(int valueCount) {
        int hardCap = valueCount >>> 3;
        int computed = (valueCount * maxExceptionPercent) / 100;
        return Math.max(MIN_EXCEPTIONS, Math.min(hardCap, computed));
    }

    // NOTE: Uses first 65 slots of the scratch array as a bit-width histogram (widths 0..64).
    private static int findOptimalBitWidth(final long[] values, int valueCount, int maxBits, int maxExceptions, final long[] scratch) {
        for (int i = 0; i < HISTOGRAM_SIZE; i++) {
            scratch[i] = 0;
        }
        for (int i = 0; i < valueCount; i++) {
            scratch[bitsRequired(values[i])]++;
        }

        int valuesAtOrBelow = 0;
        for (int bits = 0; bits <= maxBits; bits++) {
            valuesAtOrBelow += (int) scratch[bits];
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

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof PatchedPForEncodeStage that && maxExceptionPercent == that.maxExceptionPercent);
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(maxExceptionPercent);
    }

    @Override
    public String toString() {
        return "PatchedPForEncodeStage{maxExceptionPercent=" + maxExceptionPercent + "}";
    }
}
