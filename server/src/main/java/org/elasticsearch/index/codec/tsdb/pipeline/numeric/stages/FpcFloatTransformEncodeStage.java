/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.TransformEncoder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public final class FpcFloatTransformEncodeStage implements TransformEncoder {

    public static final int DEFAULT_TABLE_SIZE = 1024;

    private final long[] fcmTable;
    private final long[] dfcmTable;
    private final int tableMask;
    private final byte[] selectors;
    private final double quantizeStep;

    public FpcFloatTransformEncodeStage(int blockSize) {
        this(blockSize, DEFAULT_TABLE_SIZE, 0.0);
    }

    public FpcFloatTransformEncodeStage(int blockSize, int tableSize) {
        this(blockSize, tableSize, 0.0);
    }

    public FpcFloatTransformEncodeStage(int blockSize, int tableSize, double maxError) {
        assert (tableSize & (tableSize - 1)) == 0 : "tableSize must be a power of 2: " + tableSize;
        assert maxError >= 0 : "maxError must be non-negative: " + maxError;
        this.fcmTable = new long[tableSize];
        this.dfcmTable = new long[tableSize];
        this.tableMask = tableSize - 1;
        this.selectors = new byte[(blockSize + 7) >>> 3];
        this.quantizeStep = maxError > 0 ? 2.0 * maxError : 0.0;
    }

    @Override
    public byte id() {
        return StageId.FPC_FLOAT_STAGE.id;
    }

    @Override
    public int maxMetadataBytes(int blockSize) {
        return Integer.BYTES + ((blockSize + 7) >>> 3);
    }

    // NOTE: Metadata layout: [4-byte firstValue][ceil(valueCount/8) selector bytes].
    // Same algorithm as FpcDoubleTransformEncodeStage but with float-appropriate
    // hash shifts: FCM uses actual >>> 16 and DFCM uses stride >>> 20.
    @Override
    public int encode(final long[] values, int valueCount, final EncodingContext context) throws IOException {
        assert valueCount > 0 : "valueCount must be positive";

        if (quantizeStep > 0) {
            QuantizeUtils.quantizeFloats(values, valueCount, (float) quantizeStep);
        }

        // NOTE: convert sortable-ints to raw IEEE-754 float bits so that XOR
        // produces small residuals for consecutive similar floats.
        // Mask to 32 bits to prevent sign-extension into the upper long bits.
        for (int i = 0; i < valueCount; i++) {
            values[i] = NumericUtils.sortableFloatBits((int) values[i]) & 0xFFFFFFFFL;
        }

        if (shouldSkip(values, valueCount)) {
            // NOTE: convert back — skip means no transform, downstream expects sortable ints
            for (int i = 0; i < valueCount; i++) {
                values[i] = NumericUtils.floatToSortableInt(Float.intBitsToFloat((int) values[i])) & 0xFFFFFFFFL;
            }
            return valueCount;
        }

        Arrays.fill(fcmTable, 0);
        Arrays.fill(dfcmTable, 0);

        final int selectorByteCount = (valueCount + 7) >>> 3;
        Arrays.fill(selectors, 0, selectorByteCount, (byte) 0);

        final long firstValue = values[0];
        final var metadata = context.metadata();
        metadata.writeByte((byte) (firstValue >>> 24));
        metadata.writeByte((byte) (firstValue >>> 16));
        metadata.writeByte((byte) (firstValue >>> 8));
        metadata.writeByte((byte) firstValue);
        values[0] = 0;

        int fcmHash = 0;
        int dfcmHash = 0;
        long lastValue = firstValue;

        for (int i = 1; i < valueCount; i++) {
            final long actual = values[i];
            final long fcmXor = actual ^ fcmTable[fcmHash];
            final long dfcmXor = actual ^ (lastValue + dfcmTable[dfcmHash]);

            final boolean useDfcm = Long.compareUnsigned(dfcmXor, fcmXor) < 0;
            if (useDfcm) {
                selectors[i >>> 3] |= (byte) (1 << (i & 7));
            }
            values[i] = useDfcm ? dfcmXor : fcmXor;

            fcmTable[fcmHash] = actual;
            fcmHash = ((fcmHash << 6) ^ (int) (actual >>> 16)) & tableMask;
            final long stride = actual - lastValue;
            dfcmTable[dfcmHash] = stride;
            dfcmHash = (int) (stride >>> 20) & tableMask;
            lastValue = actual;
        }

        metadata.writeBytes(selectors, 0, selectorByteCount);
        return valueCount;
    }

    // NOTE: If consecutive XOR doesn't reduce bit-width, FPC's hash-table
    // predictors won't do better. Two branchless loops for SIMD auto-vectorization.
    private static boolean shouldSkip(final long[] values, int valueCount) {
        long rawOr = 0;
        for (int i = 0; i < valueCount; i++) {
            rawOr |= values[i];
        }
        long xorOr = 0;
        for (int i = 1; i < valueCount; i++) {
            xorOr |= values[i] ^ values[i - 1];
        }
        return 64 - Long.numberOfLeadingZeros(xorOr) >= 64 - Long.numberOfLeadingZeros(rawOr);
    }

    public static int encodeStatic(
        final FpcFloatTransformEncodeStage stage,
        final long[] values,
        int valueCount,
        final EncodingContext context
    ) throws IOException {
        return stage.encode(values, valueCount, context);
    }

    int tableSize() {
        return tableMask + 1;
    }

    @Override
    public boolean equals(Object o) {
        return this == o
            || (o instanceof FpcFloatTransformEncodeStage that
                && tableMask == that.tableMask
                && Double.compare(quantizeStep, that.quantizeStep) == 0);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableMask, quantizeStep);
    }

    @Override
    public String toString() {
        return "FpcFloatTransformEncodeStage{tableSize=" + (tableMask + 1) + ", quantizeStep=" + quantizeStep + "}";
    }
}
