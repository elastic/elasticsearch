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
import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.TransformDecoder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public final class FpcTransformDecodeStage implements TransformDecoder {

    private final long[] fcmTable;
    private final long[] dfcmTable;
    private final int tableMask;
    private final byte[] selectors;
    private final boolean isFloat;

    public FpcTransformDecodeStage(int blockSize) {
        this(blockSize, FpcTransformEncodeStage.DEFAULT_TABLE_SIZE, false);
    }

    public FpcTransformDecodeStage(int blockSize, int tableSize) {
        this(blockSize, tableSize, false);
    }

    public FpcTransformDecodeStage(int blockSize, int tableSize, boolean isFloat) {
        assert (tableSize & (tableSize - 1)) == 0 : "tableSize must be a power of 2: " + tableSize;
        this.fcmTable = new long[tableSize];
        this.dfcmTable = new long[tableSize];
        this.tableMask = tableSize - 1;
        this.selectors = new byte[(blockSize + 7) >>> 3];
        this.isFloat = isFloat;
    }

    @Override
    public byte id() {
        return StageId.FPC_STAGE.id;
    }

    // NOTE: Metadata layout: [ceil(valueCount/8) selector bytes].
    // Each bit selects FCM (0) or DFCM (1) prediction. The decoder maintains
    // its own FCM/DFCM tables (identical to the encoder's) and uses the selector
    // bits to pick the same prediction, then XORs to recover the original value.
    @Override
    public int decode(final long[] values, int valueCount, final DecodingContext context) throws IOException {
        Arrays.fill(fcmTable, 0);
        Arrays.fill(dfcmTable, 0);

        final var metadata = context.metadata();
        final int selectorByteCount = (valueCount + 7) >>> 3;
        metadata.readBytes(selectors, 0, selectorByteCount);

        int fcmHash = 0;
        int dfcmHash = 0;
        long lastValue = 0;

        for (int i = 0; i < valueCount; i++) {
            final boolean useDfcm = (selectors[i >>> 3] & (1 << (i & 7))) != 0;
            final long pred = useDfcm ? lastValue + dfcmTable[dfcmHash] : fcmTable[fcmHash];
            final long actual = values[i] ^ pred;
            values[i] = actual;

            fcmTable[fcmHash] = actual;
            fcmHash = ((fcmHash << 6) ^ (int) (actual >>> 48)) & tableMask;
            final long stride = actual - lastValue;
            dfcmTable[dfcmHash] = stride;
            dfcmHash = ((dfcmHash << 2) ^ (int) (stride >>> 40)) & tableMask;
            lastValue = actual;
        }

        // NOTE: convert raw IEEE bits back to sortable representation
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

        return valueCount;
    }

    public static int decodeStatic(final FpcTransformDecodeStage stage, final long[] values, int valueCount, final DecodingContext context)
        throws IOException {
        return stage.decode(values, valueCount, context);
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof FpcTransformDecodeStage that && tableMask == that.tableMask && isFloat == that.isFloat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableMask, isFloat);
    }

    @Override
    public String toString() {
        return "FpcTransformDecodeStage{tableSize=" + (tableMask + 1) + ", isFloat=" + isFloat + "}";
    }
}
