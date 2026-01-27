/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.PayloadCodecStage;

import java.io.IOException;

// NOTE: BlockFormat defines how each block of encoded values is written to the data file.
//
// Data file layout:
//
//   +------------------+----------------------------------------+
//   | Block 0          | [bitmap][payload][stage metadata]      |
//   | Block 1          | [bitmap][payload][stage metadata]      |
//   | ...              | ...                                    |
//   | Block N-1        | [bitmap][payload][stage metadata]      |
//   +------------------+----------------------------------------+
//   | Block Offsets    | DirectMonotonicWriter encoded offsets  |
//   +------------------+----------------------------------------+
//
// Each block contains:
//   - bitmap: 1 byte (<= 8 stages) or 2 bytes (> 8 stages) indicating which stages were applied
//   - payload: the encoded values written by the terminal PayloadCodecStage (BitPack or Zstd)
//   - stage metadata: per-stage metadata written by transformation stages (e.g., GCD divisor)
//
// The bitmap is written first so the decoder knows which stages to reverse. Stage metadata
// is written last because it's computed during encoding (e.g., the GCD is only known after
// analyzing all values in the block).
//
// See FieldDescriptor for the metadata file format that describes pipeline configuration.
public final class BlockFormat {

    private BlockFormat() {}

    public static void writeBlock(
        final DataOutput out,
        final long[] values,
        final PayloadCodecStage payloadStage,
        final EncodingContext context
    ) throws IOException {
        writeHeader(out, context);
        payloadStage.encode(values, context.valueCount(), out, context);
        context.writeStageMetadata(out);
    }

    public static int readBlock(
        final DataInput in,
        final long[] values,
        final PayloadCodecStage payloadStage,
        final DecodingContext context,
        int payloadPosition
    ) throws IOException {
        readHeader(in, context);
        if (context.isStageApplied(payloadPosition)) {
            return payloadStage.decode(values, in, context);
        }
        return 0;
    }

    static void writeHeader(final DataOutput out, final EncodingContext context) throws IOException {
        short bitmap = context.positionBitmap();
        int pipelineLength = context.pipelineLength();

        if (pipelineLength <= 8) {
            out.writeByte((byte) bitmap);
        } else {
            out.writeShort(bitmap);
        }
    }

    static void readHeader(final DataInput in, final DecodingContext context) throws IOException {
        int pipelineLength = context.pipelineLength();
        assert pipelineLength > 0 : "Pipeline must be set for decoding";

        short bitmap;
        if (pipelineLength <= 8) {
            bitmap = (short) (in.readByte() & 0xFF);
        } else {
            bitmap = in.readShort();
        }

        context.setPositionBitmap(bitmap);
    }
}
