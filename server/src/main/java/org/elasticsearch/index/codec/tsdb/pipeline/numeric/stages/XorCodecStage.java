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
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.TransformDecoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.TransformEncoder;

import java.io.IOException;

public enum XorCodecStage implements TransformEncoder, TransformDecoder {
    INSTANCE;

    @Override
    public byte id() {
        return StageId.XOR.id;
    }

    @Override
    public int encode(final long[] values, int valueCount, final EncodingContext context) throws IOException {
        if (valueCount == 0) {
            return 0;
        }

        // NOTE: Metadata layout: [firstValue: Long (8 bytes, fixed-width)].
        // writeLong (not VLong) because the first value can be any 64-bit pattern.
        context.metadata().writeLong(values[0]);

        // NOTE: XOR each value against its predecessor (work backwards to avoid overwriting).
        for (int i = valueCount - 1; i > 0; i--) {
            values[i] = values[i] ^ values[i - 1];
        }

        // NOTE: First value becomes 0 (XOR with itself).
        values[0] = 0L;

        return valueCount;
    }

    public static int encodeStatic(final XorCodecStage stage, final long[] values, int valueCount, final EncodingContext context)
        throws IOException {
        return stage.encode(values, valueCount, context);
    }

    public static int decodeStatic(final XorCodecStage stage, final long[] values, int valueCount, final DecodingContext context)
        throws IOException {
        return stage.decode(values, valueCount, context);
    }

    @Override
    public int decode(final long[] values, int valueCount, final DecodingContext context) throws IOException {
        if (valueCount == 0) {
            return 0;
        }

        // NOTE: Metadata layout: [firstValue: Long (8 bytes, fixed-width)].
        values[0] = context.metadata().readLong();

        // NOTE: XOR forward to recover original values.
        for (int i = 1; i < valueCount; i++) {
            values[i] = values[i] ^ values[i - 1];
        }

        return valueCount;
    }

    @Override
    public String toString() {
        return "XorCodecStage";
    }
}
