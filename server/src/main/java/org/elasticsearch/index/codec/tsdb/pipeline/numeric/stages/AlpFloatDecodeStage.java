/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.apache.lucene.store.DataInput;
import org.elasticsearch.index.codec.tsdb.DocValuesForUtil;
import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.PayloadDecoder;

import java.io.IOException;
import java.util.Arrays;

public final class AlpFloatDecodeStage implements PayloadDecoder {

    static final byte MODE_RAW = 0x00;
    static final byte MODE_ALP = 0x01;

    private final int blockSize;
    private final DocValuesForUtil forUtil;

    public AlpFloatDecodeStage(int blockSize) {
        this.blockSize = blockSize;
        this.forUtil = new DocValuesForUtil(blockSize);
    }

    @Override
    public byte id() {
        return StageId.ALP_FLOAT.id;
    }

    // NOTE: Payload layout (mode dispatch):
    // MODE_RAW (0x00): [mode: byte] [bitsPerValue: VInt] [packed data via DocValuesForUtil].
    // MODE_ALP (0x01): [mode: byte] [e: byte] [f: byte] [excCount: VInt]
    // [bitsPerValue: VInt] [packed encoded longs] then [excCount × (position: VInt, value: Int)].
    // Same as AlpDoubleDecodeStage but exception values are 32-bit (Int, not Long).
    @Override
    public int decode(final long[] values, final DataInput in, final DecodingContext context) throws IOException {
        final int mode = in.readByte() & 0xFF;

        if (mode == MODE_RAW) {
            return decodeRaw(values, in);
        } else if (mode == MODE_ALP) {
            return decodeAlp(values, in);
        } else {
            throw new IOException("Unknown AlpFloat mode: 0x" + Integer.toHexString(mode));
        }
    }

    private int decodeRaw(final long[] values, final DataInput in) throws IOException {
        final int bitsPerValue = in.readVInt();
        if (bitsPerValue > 0) {
            forUtil.decode(bitsPerValue, in, values);
        } else {
            Arrays.fill(values, 0, blockSize, 0L);
        }
        return blockSize;
    }

    private int decodeAlp(final long[] values, final DataInput in) throws IOException {
        final int e = in.readByte() & 0xFF;
        final int f = in.readByte() & 0xFF;
        final int exceptionCount = in.readVInt();

        final int bitsPerValue = in.readVInt();
        if (bitsPerValue > 0) {
            forUtil.decode(bitsPerValue, in, values);
        } else {
            Arrays.fill(values, 0, blockSize, 0L);
        }
        final float decodeMul = AlpFloatUtils.POWERS_OF_TEN_FLOAT[f] * AlpFloatUtils.NEG_POWERS_OF_TEN_FLOAT[e];
        for (int i = 0; i < blockSize; i++) {
            final int bits = Float.floatToRawIntBits((float) values[i] * decodeMul);
            values[i] = bits ^ (bits >> 31) & 0x7fffffff;
        }

        for (int i = 0; i < exceptionCount; i++) {
            values[in.readVInt()] = in.readInt();
        }

        return blockSize;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof AlpFloatDecodeStage that && blockSize == that.blockSize);
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(blockSize);
    }

    @Override
    public String toString() {
        return "AlpFloatDecodeStage{blockSize=" + blockSize + "}";
    }
}
