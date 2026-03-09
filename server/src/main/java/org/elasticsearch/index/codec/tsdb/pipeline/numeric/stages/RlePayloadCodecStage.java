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
import org.apache.lucene.store.DataOutput;
import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.PayloadDecoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.PayloadEncoder;

import java.io.IOException;

// NOTE: RLE payload codec is broken — it writes run-length encoded values as the terminal
// payload stage, but the run-length overhead (8 bytes per unique value + VInt per run) is
// only worthwhile for blocks with very few unique values. In practice, the standard BitPack
// payload almost always produces smaller output. Kept for backward compatibility with existing
// encoded segments.
@Deprecated
public final class RlePayloadCodecStage implements PayloadEncoder, PayloadDecoder {

    public static final RlePayloadCodecStage INSTANCE = new RlePayloadCodecStage();

    @Override
    public byte id() {
        return StageId.RLE_PAYLOAD.id;
    }

    // NOTE: Payload layout: [valueCount: VInt] [runCount: VInt]
    // then [runCount × (value: Long, runLength: VInt)].
    // Runs are written in forward order; decode expands them sequentially.
    @Override
    public void encode(final long[] values, int valueCount, final DataOutput out, final EncodingContext context) throws IOException {
        if (valueCount == 0) {
            out.writeVInt(0);
            out.writeVInt(0);
            return;
        }

        // NOTE: count runs first to write runCount header.
        int runCount = 1;
        for (int i = 1; i < valueCount; i++) {
            if (values[i] != values[i - 1]) {
                runCount++;
            }
        }

        out.writeVInt(valueCount);
        out.writeVInt(runCount);

        long currentValue = values[0];
        int currentRunLength = 1;
        for (int i = 1; i < valueCount; i++) {
            if (values[i] == currentValue) {
                currentRunLength++;
            } else {
                out.writeLong(currentValue);
                out.writeVInt(currentRunLength);
                currentValue = values[i];
                currentRunLength = 1;
            }
        }
        // NOTE: flush last run.
        out.writeLong(currentValue);
        out.writeVInt(currentRunLength);
    }

    // NOTE: Payload layout: [valueCount: VInt] [runCount: VInt]
    // then [runCount × (value: Long, runLength: VInt)].
    // Runs are expanded sequentially into the values array.
    @Override
    public int decode(final long[] values, final DataInput in, final DecodingContext context) throws IOException {
        final int valueCount = in.readVInt();
        final int runCount = in.readVInt();

        int writePos = 0;
        for (int r = 0; r < runCount; r++) {
            final long value = in.readLong();
            final int runLength = in.readVInt();
            for (int j = 0; j < runLength; j++) {
                values[writePos++] = value;
            }
        }

        return valueCount;
    }

    @Override
    public String toString() {
        return "RlePayloadCodecStage";
    }
}
