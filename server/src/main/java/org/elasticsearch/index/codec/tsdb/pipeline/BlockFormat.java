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
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.PayloadDecoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.PayloadEncoder;

import java.io.IOException;

/**
 * Defines how each block of encoded values is written to the data file.
 *
 * <p>Data file layout:
 * <pre>
 *   +------------------+----------------------------------------+
 *   | Block 0          | [bitmap][payload][stage metadata]      |
 *   | Block 1          | [bitmap][payload][stage metadata]      |
 *   | ...              | ...                                    |
 *   | Block N-1        | [bitmap][payload][stage metadata]      |
 *   +------------------+----------------------------------------+
 *   | Block Offsets     | DirectMonotonicWriter encoded offsets |
 *   +------------------+----------------------------------------+
 * </pre>
 *
 * <p>Each block contains:
 * <ul>
 *   <li><strong>bitmap</strong>: 1 byte ({@code <= 8} stages) or 2 bytes ({@code > 8} stages)
 *       indicating which stages were applied</li>
 *   <li><strong>payload</strong>: the encoded values written by the terminal payload stage</li>
 *   <li><strong>stage metadata</strong>: per-stage metadata written by transformation stages
 *       (e.g., GCD divisor)</li>
 * </ul>
 *
 * <p>The layout is designed for sequential decoding: the bitmap comes first so the
 * decoder immediately knows which stages to reverse, followed by the payload and
 * then stage metadata in reverse stage order (see {@link EncodingContext#writeStageMetadata}).
 * This means the decoder can read every section in a single forward pass with no
 * seeking or buffering. See {@link FieldDescriptor} for the metadata file format
 * that describes pipeline configuration.
 */
public final class BlockFormat {

    private BlockFormat() {}

    /**
     * Writes a block of encoded values to the data output.
     *
     * @param out          the data output stream
     * @param values       the values to encode
     * @param payloadStage the terminal payload encoder
     * @param context      the encoding context with block metadata
     * @throws IOException if an I/O error occurs
     */
    public static void writeBlock(
        final DataOutput out,
        final long[] values,
        final PayloadEncoder payloadStage,
        final EncodingContext context
    ) throws IOException {
        writeHeader(out, context);
        payloadStage.encode(values, context.valueCount(), out, context);
        context.writeStageMetadata(out);
    }

    /**
     * Reads a block of encoded values from the data input.
     *
     * @param in              the data input stream
     * @param values          the output array to populate
     * @param payloadStage    the terminal payload decoder
     * @param context         the decoding context with block metadata
     * @param payloadPosition the pipeline position of the payload stage
     * @return the number of values decoded
     * @throws IOException if an I/O error occurs
     */
    public static int readBlock(
        final DataInput in,
        final long[] values,
        final PayloadDecoder payloadStage,
        final DecodingContext context,
        int payloadPosition
    ) throws IOException {
        readHeader(in, context);
        if (context.isStageApplied(payloadPosition) == false) {
            throw new IOException("Payload stage not applied - possible data corruption");
        }
        return payloadStage.decode(values, in, context);
    }

    static void writeHeader(final DataOutput out, final EncodingContext context) throws IOException {
        final short bitmap = context.positionBitmap();
        if (context.pipelineLength() <= 8) {
            out.writeByte((byte) bitmap);
        } else {
            out.writeShort(bitmap);
        }
    }

    static void readHeader(final DataInput in, final DecodingContext context) throws IOException {
        final int pipelineLength = context.pipelineLength();
        if (pipelineLength <= 0) {
            throw new IOException("Pipeline must be set for decoding");
        }

        final short bitmap;
        if (pipelineLength <= 8) {
            bitmap = (short) (in.readByte() & 0xFF);
        } else {
            bitmap = in.readShort();
        }
        context.setPositionBitmap(bitmap);
    }
}
