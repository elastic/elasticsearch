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

public final class ChimpDoubleTransformDecodeStage implements TransformDecoder {

    @Override
    public byte id() {
        return StageId.CHIMP_DOUBLE_STAGE.id;
    }

    // NOTE: Reads groupSize (1 byte) then one 8-byte group reference per group
    // from metadata. XORs all values in each group with the broadcast reference.
    // The inner loop has no loop-carried dependencies and no gathers — C2
    // auto-vectorizes it into SIMD.
    @Override
    public int decode(final long[] values, int valueCount, final DecodingContext context) throws IOException {
        final var metadata = context.metadata();
        final int groupSize = metadata.readByte() & 0xFF;

        for (int g = 0; g < valueCount; g += groupSize) {
            final long ref = metadata.readLong();
            final int end = Math.min(g + groupSize, valueCount);
            for (int i = g; i < end; i++) {
                values[i] ^= ref;
            }
        }

        // NOTE: convert raw IEEE-754 bits back to sortable-longs
        for (int i = 0; i < valueCount; i++) {
            values[i] = NumericUtils.doubleToSortableLong(Double.longBitsToDouble(values[i]));
        }

        return valueCount;
    }

    public static int decodeStatic(
        final ChimpDoubleTransformDecodeStage stage,
        final long[] values,
        int valueCount,
        final DecodingContext context
    ) throws IOException {
        return stage.decode(values, valueCount, context);
    }

    @Override
    public String toString() {
        return "ChimpDoubleTransformDecodeStage";
    }
}
