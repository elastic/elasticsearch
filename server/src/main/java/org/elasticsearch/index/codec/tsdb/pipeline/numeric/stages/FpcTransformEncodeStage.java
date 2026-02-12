/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.TransformEncoder;

import java.io.IOException;
import java.util.Arrays;

public final class FpcTransformEncodeStage implements TransformEncoder {

    static final int DEFAULT_TABLE_SIZE = 1024;

    private final long[] fcmTable;
    private final long[] dfcmTable;
    private final int tableMask;

    public FpcTransformEncodeStage(int blockSize) {
        this(blockSize, DEFAULT_TABLE_SIZE);
    }

    public FpcTransformEncodeStage(int blockSize, int tableSize) {
        assert (tableSize & (tableSize - 1)) == 0 : "tableSize must be a power of 2: " + tableSize;
        this.fcmTable = new long[tableSize];
        this.dfcmTable = new long[tableSize];
        this.tableMask = tableSize - 1;
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

        Arrays.fill(fcmTable, 0);
        Arrays.fill(dfcmTable, 0);

        final var metadata = context.metadata();
        final int selectorByteCount = (valueCount + 7) >>> 3;
        final byte[] selectors = new byte[selectorByteCount];

        int fcmHash = 0;
        int dfcmHash = 0;
        long lastValue = 0;
        int i = 0;

        final int fullBytes = valueCount >>> 3;
        for (int byteIdx = 0; byteIdx < fullBytes; byteIdx++) {
            int sel = 0;
            for (int bit = 0; bit < 8; bit++, i++) {
                final long actual = values[i];
                final long fcmPred = fcmTable[fcmHash];
                final long dfcmPred = lastValue + dfcmTable[dfcmHash];
                final long fcmXor = actual ^ fcmPred;
                final long dfcmXor = actual ^ dfcmPred;

                final boolean useDfcm = Long.compareUnsigned(dfcmXor, fcmXor) < 0;
                if (useDfcm) {
                    sel |= (1 << bit);
                }
                values[i] = useDfcm ? dfcmXor : fcmXor;

                fcmTable[fcmHash] = actual;
                fcmHash = ((fcmHash << 6) ^ (int) (actual >>> 48)) & tableMask;
                final long stride = actual - lastValue;
                dfcmTable[dfcmHash] = stride;
                dfcmHash = ((dfcmHash << 2) ^ (int) (stride >>> 40)) & tableMask;
                lastValue = actual;
            }
            selectors[byteIdx] = (byte) sel;
        }

        if (i < valueCount) {
            int sel = 0;
            for (int bit = 0; i < valueCount; bit++, i++) {
                final long actual = values[i];
                final long fcmPred = fcmTable[fcmHash];
                final long dfcmPred = lastValue + dfcmTable[dfcmHash];
                final long fcmXor = actual ^ fcmPred;
                final long dfcmXor = actual ^ dfcmPred;

                final boolean useDfcm = Long.compareUnsigned(dfcmXor, fcmXor) < 0;
                if (useDfcm) {
                    sel |= (1 << bit);
                }
                values[i] = useDfcm ? dfcmXor : fcmXor;

                fcmTable[fcmHash] = actual;
                fcmHash = ((fcmHash << 6) ^ (int) (actual >>> 48)) & tableMask;
                final long stride = actual - lastValue;
                dfcmTable[dfcmHash] = stride;
                dfcmHash = ((dfcmHash << 2) ^ (int) (stride >>> 40)) & tableMask;
                lastValue = actual;
            }
            selectors[fullBytes] = (byte) sel;
        }

        metadata.writeBytes(selectors, 0, selectorByteCount);
        return valueCount;
    }

    public static int encodeStatic(final FpcTransformEncodeStage stage, final long[] values, int valueCount, final EncodingContext context)
        throws IOException {
        return stage.encode(values, valueCount, context);
    }

    int tableSize() {
        return tableMask + 1;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof FpcTransformEncodeStage that && tableMask == that.tableMask);
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(tableMask);
    }

    @Override
    public String toString() {
        return "FpcTransformEncodeStage{tableSize=" + (tableMask + 1) + "}";
    }
}
