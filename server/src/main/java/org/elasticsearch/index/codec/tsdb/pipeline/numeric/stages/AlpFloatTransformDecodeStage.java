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

public final class AlpFloatTransformDecodeStage implements TransformDecoder {

    @Override
    public byte id() {
        return StageId.ALP_FLOAT_STAGE.id;
    }

    // NOTE: Metadata layout: [e: byte] [f: byte] [excCount: VInt]
    // then [excCount × (position: VInt, value: ZInt)].
    // Inverse transform with 32-bit float precision. Same as
    // AlpDoubleTransformDecodeStage but exception values are zigzag-encoded (ZInt).
    @Override
    public int decode(final long[] values, int valueCount, final DecodingContext context) throws IOException {
        final var metadata = context.metadata();
        final int e = metadata.readByte() & 0xFF;
        final int f = metadata.readByte() & 0xFF;
        final int exceptionCount = metadata.readVInt();

        final float decodeMul = AlpFloatUtils.POWERS_OF_TEN_FLOAT[f] * AlpFloatUtils.NEG_POWERS_OF_TEN_FLOAT[e];
        for (int i = 0; i < valueCount; i++) {
            final int bits = Float.floatToRawIntBits((float) values[i] * decodeMul);
            values[i] = bits ^ (bits >> 31) & 0x7fffffff;
        }

        for (int i = 0; i < exceptionCount; i++) {
            values[metadata.readVInt()] = metadata.readZInt();
        }

        return valueCount;
    }

    public static int decodeStatic(
        final AlpFloatTransformDecodeStage stage,
        final long[] values,
        int valueCount,
        final DecodingContext context
    ) throws IOException {
        return stage.decode(values, valueCount, context);
    }

    @Override
    public String toString() {
        return "AlpFloatTransformDecodeStage";
    }
}
