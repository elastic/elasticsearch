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

public enum FpcTransformDecodeStage implements TransformDecoder {
    INSTANCE;

    @Override
    public byte id() {
        return StageId.FPC_STAGE.id;
    }

    // NOTE: Metadata layout: [valueCount × prediction: Long (8 bytes each)].
    // Each value is XORed with its prediction to recover the original.
    // The decoder does not need FCM/DFCM tables — predictions are stored
    // explicitly in metadata, making decode a simple XOR loop.
    @Override
    public int decode(final long[] values, int valueCount, final DecodingContext context) throws IOException {
        final var metadata = context.metadata();
        for (int i = 0; i < valueCount; i++) {
            values[i] ^= metadata.readLong();
        }
        return valueCount;
    }

    public static int decodeStatic(final FpcTransformDecodeStage stage, final long[] values, int valueCount, final DecodingContext context)
        throws IOException {
        return stage.decode(values, valueCount, context);
    }

    @Override
    public String toString() {
        return "FpcTransformDecodeStage";
    }
}
