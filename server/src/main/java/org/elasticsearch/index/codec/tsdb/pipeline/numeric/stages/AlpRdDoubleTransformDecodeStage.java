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
import org.elasticsearch.index.codec.tsdb.pipeline.MetadataReader;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.TransformDecoder;

import java.io.IOException;

public final class AlpRdDoubleTransformDecodeStage implements TransformDecoder {

    static final byte MODE_DECIMAL = 0x01;
    static final byte MODE_ALP_RD = 0x02;

    @Override
    public byte id() {
        return StageId.ALP_RD_DOUBLE_STAGE.id;
    }

    // NOTE: Metadata layout (mode dispatch):
    // MODE_DECIMAL (0x01): [mode: byte] [e: byte] [f: byte] [excCount: VInt]
    // then [excCount × (position: VInt, value: Long)].
    // MODE_ALP_RD (0x02): [mode: byte] [prefix: Long].
    // Reconstruction: sortableDoubleBits(prefix | tail) for each value.
    @Override
    public int decode(final long[] values, int valueCount, final DecodingContext context) throws IOException {
        final var metadata = context.metadata();
        final int mode = metadata.readByte() & 0xFF;

        return switch (mode) {
            case MODE_DECIMAL -> decodeDecimal(values, valueCount, metadata);
            case MODE_ALP_RD -> decodeAlpRd(values, valueCount, metadata);
            default -> throw new IOException("Unknown AlpRdDoubleTransformDecodeStage mode: 0x" + Integer.toHexString(mode));
        };
    }

    private static int decodeDecimal(final long[] values, int valueCount, final MetadataReader metadata) throws IOException {
        final int e = metadata.readByte() & 0xFF;
        final int f = metadata.readByte() & 0xFF;
        final int exceptionCount = metadata.readVInt();

        final double decodeMul = AlpDoubleUtils.POWERS_OF_TEN[f] * AlpDoubleUtils.NEG_POWERS_OF_TEN[e];
        for (int i = 0; i < valueCount; i++) {
            final long bits = Double.doubleToRawLongBits(values[i] * decodeMul);
            values[i] = bits ^ (bits >> 63) & 0x7fffffffffffffffL;
        }

        for (int i = 0; i < exceptionCount; i++) {
            values[metadata.readVInt()] = metadata.readLong();
        }

        return valueCount;
    }

    private static int decodeAlpRd(final long[] values, int valueCount, final MetadataReader metadata) throws IOException {
        final long prefix = metadata.readLong();

        for (int i = 0; i < valueCount; i++) {
            values[i] = NumericUtils.sortableDoubleBits(prefix | values[i]);
        }

        return valueCount;
    }

    public static int decodeStatic(
        final AlpRdDoubleTransformDecodeStage stage,
        final long[] values,
        int valueCount,
        final DecodingContext context
    ) throws IOException {
        return stage.decode(values, valueCount, context);
    }

    @Override
    public String toString() {
        return "AlpRdDoubleTransformDecodeStage";
    }
}
