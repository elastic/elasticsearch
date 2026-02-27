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

public final class FpcFloatTransformDecodeStage implements TransformDecoder {

    private final long[] fcmTable;
    private final long[] dfcmTable;
    private final int tableMask;
    private final byte[] selectors;

    public FpcFloatTransformDecodeStage(int blockSize) {
        this(blockSize, FpcFloatTransformEncodeStage.DEFAULT_TABLE_SIZE);
    }

    public FpcFloatTransformDecodeStage(int blockSize, int tableSize) {
        assert (tableSize & (tableSize - 1)) == 0 : "tableSize must be a power of 2: " + tableSize;
        this.fcmTable = new long[tableSize];
        this.dfcmTable = new long[tableSize];
        this.tableMask = tableSize - 1;
        this.selectors = new byte[(blockSize + 7) >>> 3];
    }

    @Override
    public byte id() {
        return StageId.FPC_FLOAT_STAGE.id;
    }

    // NOTE: Metadata layout: [4-byte firstValue][ceil(valueCount/8) selector bytes].
    // Mirrors FpcFloatTransformEncodeStage with float-appropriate hash shifts.
    @Override
    public int decode(final long[] values, int valueCount, final DecodingContext context) throws IOException {
        Arrays.fill(fcmTable, 0);
        Arrays.fill(dfcmTable, 0);

        final var metadata = context.metadata();
        final long firstValue = ((metadata.readByte() & 0xFFL) << 24) | ((metadata.readByte() & 0xFFL) << 16) | ((metadata.readByte()
            & 0xFFL) << 8) | (metadata.readByte() & 0xFFL);
        final int selectorByteCount = (valueCount + 7) >>> 3;
        metadata.readBytes(selectors, 0, selectorByteCount);

        int fcmHash = 0;
        int dfcmHash = 0;
        long lastValue = firstValue;

        for (int i = 1; i < valueCount; i++) {
            final boolean useDfcm = (selectors[i >>> 3] & (1 << (i & 7))) != 0;
            final long pred = useDfcm ? lastValue + dfcmTable[dfcmHash] : fcmTable[fcmHash];
            final long actual = values[i] ^ pred;
            values[i] = actual;

            fcmTable[fcmHash] = actual;
            fcmHash = ((fcmHash << 6) ^ (int) (actual >>> 16)) & tableMask;
            final long stride = actual - lastValue;
            dfcmTable[dfcmHash] = stride;
            dfcmHash = (int) (stride >>> 20) & tableMask;
            lastValue = actual;
        }

        values[0] = firstValue;

        // NOTE: convert raw IEEE-754 float bits back to sortable-ints.
        // Mask to 32 bits to prevent sign-extension into the upper long bits.
        for (int i = 0; i < valueCount; i++) {
            values[i] = NumericUtils.floatToSortableInt(Float.intBitsToFloat((int) values[i])) & 0xFFFFFFFFL;
        }

        return valueCount;
    }

    public static int decodeStatic(
        final FpcFloatTransformDecodeStage stage,
        final long[] values,
        int valueCount,
        final DecodingContext context
    ) throws IOException {
        return stage.decode(values, valueCount, context);
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof FpcFloatTransformDecodeStage that && tableMask == that.tableMask);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableMask);
    }

    @Override
    public String toString() {
        return "FpcFloatTransformDecodeStage{tableSize=" + (tableMask + 1) + "}";
    }
}
