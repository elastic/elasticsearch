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

    private static final int DEFAULT_TABLE_SIZE = 1024;

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
        return blockSize * Long.BYTES;
    }

    // NOTE: Metadata layout: [valueCount × prediction: Long (8 bytes each)].
    // For each value, the best prediction (FCM or DFCM, whichever has more
    // leading zeros in the XOR residual) is written to metadata. Values are
    // XORed with their prediction in-place, producing small residuals for
    // downstream bit-packing.
    @Override
    public int encode(final long[] values, int valueCount, final EncodingContext context) throws IOException {
        assert valueCount > 0 : "valueCount must be positive";

        Arrays.fill(fcmTable, 0);
        Arrays.fill(dfcmTable, 0);

        final var metadata = context.metadata();
        int fcmHash = 0;
        int dfcmHash = 0;
        long lastValue = 0;

        for (int i = 0; i < valueCount; i++) {
            final long actual = values[i];

            final long fcmPred = fcmTable[fcmHash];
            final long dfcmPred = lastValue + dfcmTable[dfcmHash];

            final long fcmXor = actual ^ fcmPred;
            final long dfcmXor = actual ^ dfcmPred;

            // NOTE: Pick the prediction with the smaller unsigned XOR residual (more leading zeros).
            final long bestPred = Long.compareUnsigned(fcmXor, dfcmXor) <= 0 ? fcmPred : dfcmPred;
            metadata.writeLong(bestPred);
            values[i] = actual ^ bestPred;

            fcmTable[fcmHash] = actual;
            fcmHash = ((fcmHash << 6) ^ (int) (actual >>> 48)) & tableMask;

            final long stride = actual - lastValue;
            dfcmTable[dfcmHash] = stride;
            dfcmHash = ((dfcmHash << 2) ^ (int) (stride >>> 40)) & tableMask;

            lastValue = actual;
        }

        return valueCount;
    }

    public static int encodeStatic(final FpcTransformEncodeStage stage, final long[] values, int valueCount, final EncodingContext context)
        throws IOException {
        return stage.encode(values, valueCount, context);
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
