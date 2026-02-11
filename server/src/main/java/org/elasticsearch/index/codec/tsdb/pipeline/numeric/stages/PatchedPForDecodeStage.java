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
import org.elasticsearch.index.codec.tsdb.pipeline.MetadataReader;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.TransformDecoder;

import java.io.IOException;

public final class PatchedPForDecodeStage implements TransformDecoder {

    @Override
    public byte id() {
        return StageId.PATCHED_PFOR.id;
    }

    // NOTE: Metadata layout: [optimalBits: VInt] [numExceptions: VInt]
    // then [numExceptions × (delta-position: VInt, value: VLong)].
    // Exception positions are delta-encoded in ascending order. Values at
    // exception positions are overwritten in-place after bulk bit-unpack.
    @Override
    public int decode(final long[] values, int valueCount, final DecodingContext context) throws IOException {
        final MetadataReader meta = context.metadata();
        @SuppressWarnings("unused")
        int bitWidth = meta.readVInt();
        int numExceptions = meta.readVInt();
        int prevPos = 0;
        for (int i = 0; i < numExceptions; i++) {
            prevPos += meta.readVInt();
            values[prevPos] = meta.readVLong();
        }
        return valueCount;
    }

    public static int decodeStatic(final PatchedPForDecodeStage stage, final long[] values, int valueCount, final DecodingContext context)
        throws IOException {
        return stage.decode(values, valueCount, context);
    }

    @Override
    public String toString() {
        return "PatchedPForDecodeStage";
    }
}
