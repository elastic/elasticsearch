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

public final class AlpDoubleTransformDecodeStage implements TransformDecoder {

    @Override
    public byte id() {
        return StageId.ALP_DOUBLE_STAGE.id;
    }

    // NOTE: Metadata layout: [e: byte] [f: byte] [excCount: VInt]
    // then [excCount × (position: VInt, value: Long)].
    // Inverse transform: decode = v × (10^f × 10^-e), then converts IEEE 754
    // bits back to sortable-long encoding. Exceptions overwrite specific positions.
    @Override
    public int decode(final long[] values, int valueCount, final DecodingContext context) throws IOException {
        final var metadata = context.metadata();
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

    public static int decodeStatic(
        final AlpDoubleTransformDecodeStage stage,
        final long[] values,
        int valueCount,
        final DecodingContext context
    ) throws IOException {
        return stage.decode(values, valueCount, context);
    }

    @Override
    public String toString() {
        return "AlpDoubleTransformDecodeStage";
    }
}
