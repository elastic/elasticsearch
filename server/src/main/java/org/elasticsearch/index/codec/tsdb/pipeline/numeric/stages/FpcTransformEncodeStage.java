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

public final class FpcTransformEncodeStage implements TransformEncoder {

    public static final int DEFAULT_TABLE_SIZE = 1024;

    private final long[] fcmTable;
    private final long[] dfcmTable;
    private final int tableMask;
    private final byte[] selectors;
    private final double quantizeStep;
    private final boolean isFloat;

    public FpcTransformEncodeStage(int blockSize) {
        this(blockSize, DEFAULT_TABLE_SIZE, 0.0, false);
    }

    public FpcTransformEncodeStage(int blockSize, int tableSize) {
        this(blockSize, tableSize, 0.0, false);
    }

    public FpcTransformEncodeStage(int blockSize, int tableSize, double maxError) {
        this(blockSize, tableSize, maxError, false);
    }

    public FpcTransformEncodeStage(int blockSize, int tableSize, double maxError, boolean isFloat) {
        assert (tableSize & (tableSize - 1)) == 0 : "tableSize must be a power of 2: " + tableSize;
        assert maxError >= 0 : "maxError must be non-negative: " + maxError;
        this.fcmTable = new long[tableSize];
        this.dfcmTable = new long[tableSize];
        this.tableMask = tableSize - 1;
        this.selectors = new byte[(blockSize + 7) >>> 3];
        this.quantizeStep = maxError > 0 ? 2.0 * maxError : 0.0;
        this.isFloat = isFloat;
    }

    @Override
    public byte id() {
        return StageId.FPC_STAGE.id;
    }

    @Override
    public int maxMetadataBytes(int blockSize) {
        return (blockSize + 7) >>> 3;
    }

    // NOTE: Metadata layout: [ceil(valueCount/8) selector bytes].
    // For each value, one bit indicates whether FCM (0) or DFCM (1) prediction
    // was used. The decoder maintains its own FCM/DFCM tables and uses selector
    // bits to pick the same prediction. Values are XORed with their prediction
    // in-place, producing small residuals for downstream bit-packing.
    @Override
    public int encode(final long[] values, int valueCount, final EncodingContext context) throws IOException {
        assert valueCount > 0 : "valueCount must be positive";

        if (quantizeStep > 0) {
            QuantizeUtils.quantizeDoubles(values, valueCount, quantizeStep);
        }

        // NOTE: convert sortable representation to raw IEEE bits so that XOR
        // produces small residuals for consecutive similar values.
        toRawBits(values, valueCount);

        if (shouldSkip(values, valueCount)) {
            // NOTE: convert back — skip means no transform, downstream expects sortable representation
            toSortableBits(values, valueCount);
            return valueCount;
        }

        Arrays.fill(fcmTable, 0);
        Arrays.fill(dfcmTable, 0);

        final int selectorByteCount = (valueCount + 7) >>> 3;
        Arrays.fill(selectors, 0, selectorByteCount, (byte) 0);

        int fcmHash = 0;
        int dfcmHash = 0;
        long lastValue = 0;

        for (int i = 0; i < valueCount; i++) {
            final long actual = values[i];
            final long fcmXor = actual ^ fcmTable[fcmHash];
            final long dfcmXor = actual ^ (lastValue + dfcmTable[dfcmHash]);

            final boolean useDfcm = Long.compareUnsigned(dfcmXor, fcmXor) < 0;
            if (useDfcm) {
                selectors[i >>> 3] |= (byte) (1 << (i & 7));
            }
            values[i] = useDfcm ? dfcmXor : fcmXor;

            fcmTable[fcmHash] = actual;
            fcmHash = ((fcmHash << 6) ^ (int) (actual >>> 48)) & tableMask;
            final long stride = actual - lastValue;
            dfcmTable[dfcmHash] = stride;
            dfcmHash = ((dfcmHash << 2) ^ (int) (stride >>> 40)) & tableMask;
            lastValue = actual;
        }

        context.metadata().writeBytes(selectors, 0, selectorByteCount);
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

    private void toRawBits(final long[] values, int valueCount) {
        if (isFloat) {
            // NOTE: mask to 32 bits to prevent sign-extension into the upper long bits
            for (int i = 0; i < valueCount; i++) {
                values[i] = NumericUtils.sortableFloatBits((int) values[i]) & 0xFFFFFFFFL;
            }
        } else {
            for (int i = 0; i < valueCount; i++) {
                values[i] = NumericUtils.sortableDoubleBits(values[i]);
            }
        }
    }

    private void toSortableBits(final long[] values, int valueCount) {
        if (isFloat) {
            // NOTE: mask to 32 bits to prevent sign-extension into the upper long bits
            for (int i = 0; i < valueCount; i++) {
                values[i] = NumericUtils.floatToSortableInt(Float.intBitsToFloat((int) values[i])) & 0xFFFFFFFFL;
            }
        } else {
            for (int i = 0; i < valueCount; i++) {
                values[i] = NumericUtils.doubleToSortableLong(Double.longBitsToDouble(values[i]));
            }
        }
    }

    public static int encodeStatic(final FpcTransformEncodeStage stage, final long[] values, int valueCount, final EncodingContext context)
        throws IOException {
        return stage.encode(values, valueCount, context);
    }

    int tableSize() {
        return tableMask + 1;
    }

    boolean isFloat() {
        return isFloat;
    }

    @Override
    public boolean equals(Object o) {
        return this == o
            || (o instanceof FpcTransformEncodeStage that
                && tableMask == that.tableMask
                && Double.compare(quantizeStep, that.quantizeStep) == 0
                && isFloat == that.isFloat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableMask, quantizeStep, isFloat);
    }

    @Override
    public String toString() {
        return "FpcTransformEncodeStage{tableSize=" + (tableMask + 1) + ", quantizeStep=" + quantizeStep + ", isFloat=" + isFloat + "}";
    }
}
