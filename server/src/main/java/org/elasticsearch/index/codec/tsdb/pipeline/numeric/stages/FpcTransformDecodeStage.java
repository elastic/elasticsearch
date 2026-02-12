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
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.TransformDecoder;

import java.io.IOException;
import java.util.Arrays;

public final class FpcTransformDecodeStage implements TransformDecoder {

    private final long[] fcmTable;
    private final long[] dfcmTable;
    private final int tableMask;

    public FpcTransformDecodeStage() {
        this(FpcTransformEncodeStage.DEFAULT_TABLE_SIZE);
    }

    public FpcTransformDecodeStage(int tableSize) {
        assert (tableSize & (tableSize - 1)) == 0 : "tableSize must be a power of 2: " + tableSize;
        this.fcmTable = new long[tableSize];
        this.dfcmTable = new long[tableSize];
        this.tableMask = tableSize - 1;
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
        final byte[] selectors = new byte[selectorByteCount];
        metadata.readBytes(selectors);

        int fcmHash = 0;
        int dfcmHash = 0;
        long lastValue = 0;
        int i = 0;

        final int fullBytes = valueCount >>> 3;
        for (int byteIdx = 0; byteIdx < fullBytes; byteIdx++) {
            int sel = selectors[byteIdx] & 0xFF;
            for (int bit = 0; bit < 8; bit++, i++) {
                final long fcmPred = fcmTable[fcmHash];
                final long dfcmPred = lastValue + dfcmTable[dfcmHash];

                final long actual = values[i] ^ ((sel & 1) != 0 ? dfcmPred : fcmPred);
                values[i] = actual;
                sel >>>= 1;

                fcmTable[fcmHash] = actual;
                fcmHash = ((fcmHash << 6) ^ (int) (actual >>> 48)) & tableMask;
                final long stride = actual - lastValue;
                dfcmTable[dfcmHash] = stride;
                dfcmHash = ((dfcmHash << 2) ^ (int) (stride >>> 40)) & tableMask;
                lastValue = actual;
            }
        }

        if (i < valueCount) {
            int sel = selectors[fullBytes] & 0xFF;
            while (i < valueCount) {
                final long fcmPred = fcmTable[fcmHash];
                final long dfcmPred = lastValue + dfcmTable[dfcmHash];

                final long actual = values[i] ^ ((sel & 1) != 0 ? dfcmPred : fcmPred);
                values[i] = actual;
                sel >>>= 1;
                i++;

                fcmTable[fcmHash] = actual;
                fcmHash = ((fcmHash << 6) ^ (int) (actual >>> 48)) & tableMask;
                final long stride = actual - lastValue;
                dfcmTable[dfcmHash] = stride;
                dfcmHash = ((dfcmHash << 2) ^ (int) (stride >>> 40)) & tableMask;
                lastValue = actual;
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
        return this == o || (o instanceof FpcTransformDecodeStage that && tableMask == that.tableMask);
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(tableMask);
    }

    @Override
    public String toString() {
        return "FpcTransformDecodeStage{tableSize=" + (tableMask + 1) + "}";
    }
}
