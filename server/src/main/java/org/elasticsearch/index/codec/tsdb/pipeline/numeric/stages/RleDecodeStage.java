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

// NOTE: RLE transform is broken — it reduces valueCount but the downstream BitPack stage
// always packs blockSize values (zero-filling the tail), so the payload size never decreases.
// RLE metadata is pure overhead. Kept for backward compatibility: existing segments encoded
// with RLE must still be decodable.
@Deprecated
public final class RleDecodeStage implements TransformDecoder {

    @Override
    public byte id() {
        return StageId.RLE.id;
    }

    // NOTE: Metadata layout: [runCount: VInt] [valueCount: VInt]
    // then [runCount × runLength: VInt] in reverse order.
    // Run lengths are stored in reverse so we can expand right-to-left in-place
    // without buffering. Each run length corresponds to values[r] where r
    // counts down from runCount-1, expanding from the end of the array backward.
    @Override
    public int decode(final long[] values, int valueCount, final DecodingContext context) throws IOException {
        final var meta = context.metadata();
        final int runCount = meta.readVInt();
        final int totalValues = meta.readVInt();

        int writePos = totalValues - 1;
        for (int r = runCount - 1; r >= 0; r--) {
            final int runLength = meta.readVInt();
            final long val = values[r];
            for (int j = 0; j < runLength; j++) {
                values[writePos--] = val;
            }
        }

        return totalValues;
    }

    public static int decodeStatic(final RleDecodeStage stage, final long[] values, int valueCount, final DecodingContext context)
        throws IOException {
        return stage.decode(values, valueCount, context);
    }

    @Override
    public String toString() {
        return "RleDecodeStage";
    }
}
