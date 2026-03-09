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
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.codec.tsdb.DocValuesForUtil;
import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.PayloadDecoder;

import java.io.IOException;
import java.util.Arrays;

public final class AlpRdDoubleDecodeStage implements PayloadDecoder {

    static final byte MODE_RAW = 0x00;
    static final byte MODE_DECIMAL = 0x01;
    static final byte MODE_ALP_RD = 0x02;

    private final int blockSize;
    private final DocValuesForUtil forUtil;

    public AlpRdDoubleDecodeStage(int blockSize) {
        this.blockSize = blockSize;
        this.forUtil = new DocValuesForUtil(blockSize);
    }

    @Override
    public byte id() {
        return StageId.ALP_RD_DOUBLE.id;
    }

    // NOTE: Payload layout (mode dispatch):
    // MODE_RAW (0x00): [mode: byte] [bitsPerValue: VInt] [packed data via DocValuesForUtil].
    // MODE_DECIMAL (0x01): [mode: byte] [e: byte] [f: byte] [excCount: VInt]
    // [bitsPerValue: VInt] [packed encoded longs] then [excCount × (position: VInt, value: Long)].
    // MODE_ALP_RD (0x02): [mode: byte] [prefix: Long]
    // [bitsPerValue: VInt] [packed tail bits via DocValuesForUtil].
    // Reconstruction: sortableDoubleBits(prefix | tail) for each value.
    @Override
    public int decode(final long[] values, final DataInput in, final DecodingContext context) throws IOException {
        final int mode = in.readByte() & 0xFF;

        return switch (mode) {
            case MODE_RAW -> decodeRaw(values, in);
            case MODE_DECIMAL -> decodeDecimal(values, in);
            case MODE_ALP_RD -> decodeAlpRd(values, in);
            default -> throw new IOException("Unknown AlpRdDouble mode: 0x" + Integer.toHexString(mode));
        };
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

    private int decodeDecimal(final long[] values, final DataInput in) throws IOException {
        final int e = in.readByte() & 0xFF;
        final int f = in.readByte() & 0xFF;
        final int exceptionCount = in.readVInt();

        final int bitsPerValue = in.readVInt();
        if (bitsPerValue > 0) {
            forUtil.decode(bitsPerValue, in, values);
        } else {
            Arrays.fill(values, 0, blockSize, 0L);
        }
        final double decodeMul = AlpDoubleUtils.POWERS_OF_TEN[f] * AlpDoubleUtils.NEG_POWERS_OF_TEN[e];
        for (int i = 0; i < blockSize; i++) {
            final long bits = Double.doubleToRawLongBits(values[i] * decodeMul);
            values[i] = bits ^ (bits >> 63) & 0x7fffffffffffffffL;
        }

        for (int i = 0; i < exceptionCount; i++) {
            values[in.readVInt()] = in.readLong();
        }

        return blockSize;
    }

    private int decodeAlpRd(final long[] values, final DataInput in) throws IOException {
        final long prefix = in.readLong();

        final int bitsPerValue = in.readVInt();
        if (bitsPerValue > 0) {
            forUtil.decode(bitsPerValue, in, values);
        } else {
            Arrays.fill(values, 0, blockSize, 0L);
        }

        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.sortableDoubleBits(prefix | values[i]);
        }

        return blockSize;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof AlpRdDoubleDecodeStage that && blockSize == that.blockSize);
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(blockSize);
    }

    @Override
    public String toString() {
        return "AlpRdDoubleDecodeStage{blockSize=" + blockSize + "}";
    }
}
