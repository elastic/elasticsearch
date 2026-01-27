/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

import java.io.IOException;

// NOTE: FieldDescriptor is NOT currently used by ES94. The POC uses a hardcoded pipeline
// configuration in ES94TSDBDocValuesFormat.createNumericCodec(), matching how ES819 works
// where the decoder implicitly knows the encoding format.
//
// This class exists to demonstrate the evolution path toward self-describing formats:
//
// Current approach (ES819-like):
//   - Pipeline is hardcoded: delta -> offset -> gcd -> bitPack
//   - Encoder and decoder must use identical pipeline configuration
//   - Changing the pipeline breaks compatibility with existing segments
//
// Future approach (using FieldDescriptor):
//   - Write PipelineDescriptor to metadata per-field via FieldDescriptor.write()
//   - Decoder reads FieldDescriptor and creates the correct pipeline dynamically
//   - Different fields can use different pipeline configurations
//   - Pipeline changes are backward compatible (old data still readable)
//
// Wire format with FieldDescriptor (per numeric field in metadata file):
//
//   +------------------+--------------------------------------------------+
//   | FIELD DESCRIPTOR |                                                  |
//   +------------------+--------------------------------------------------+
//   | formatVersion    | VInt - allows future format evolution            |
//   +------------------+--------------------------------------------------+
//   | PIPELINE DESCRIPTOR                                                 |
//   +------------------+--------------------------------------------------+
//   | stageCount       | VInt - number of stages in pipeline              |
//   | stageIds[]       | byte[] - stage IDs (e.g., 0x01=Delta, 0xA1=Pack) |
//   | blockShift       | byte - log2(blockSize), e.g., 7 for 128          |
//   +------------------+--------------------------------------------------+
//   | totalValueCount  | VLong - total values across all blocks           |
//   | blockCount       | VInt - number of encoded blocks                  |
//   | ...              | other fields                                     |
//   +------------------+--------------------------------------------------+
//
// See BlockFormat for the data file layout.
//
// We intentionally defer this wire format change to reduce POC scope and simplify review.
// The abstraction is ready; when we decide to make formats self-describing, we:
//   1. Call FieldDescriptor.write() in ES94TSDBDocValuesConsumer after encoding
//   2. Call FieldDescriptor.read() in ES94TSDBDocValuesProducer before decoding
//   3. Use the PipelineDescriptor from FieldDescriptor to create the decoder
//
// This separation keeps the POC focused on validating the pipeline abstraction itself,
// while preserving the option to evolve the wire format independently.
public final class FieldDescriptor {

    static final int CURRENT_FORMAT_VERSION = 1;

    private static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 4;

    private final DirectMonotonicReader.Meta offsetsMeta;
    private final PipelineDescriptor pipeline;
    private final long totalValueCount;
    private final int blockCount;
    private final long offsetsDataOffset;
    private final long offsetsDataLength;

    private FieldDescriptor(
        final PipelineDescriptor pipeline,
        long totalValueCount,
        int blockCount,
        final DirectMonotonicReader.Meta offsetsMeta,
        long offsetsDataOffset,
        long offsetsDataLength
    ) {
        this.pipeline = pipeline;
        this.totalValueCount = totalValueCount;
        this.blockCount = blockCount;
        this.offsetsMeta = offsetsMeta;
        this.offsetsDataOffset = offsetsDataOffset;
        this.offsetsDataLength = offsetsDataLength;
    }

    public PipelineDescriptor pipeline() {
        return pipeline;
    }

    public long totalValueCount() {
        return totalValueCount;
    }

    public int blockCount() {
        return blockCount;
    }

    public int blockSize() {
        return pipeline.blockSize();
    }

    public int lastBlockValueCount() {
        if (totalValueCount == 0) {
            return 0;
        }
        int remainder = (int) (totalValueCount % pipeline.blockSize());
        return remainder == 0 ? pipeline.blockSize() : remainder;
    }

    public DirectMonotonicReader getOffsetsReader(final IndexInput data) throws IOException {
        final RandomAccessInput slice = data.randomAccessSlice(offsetsDataOffset, offsetsDataLength);
        return DirectMonotonicReader.getInstance(offsetsMeta, slice);
    }

    public static void write(
        final IndexOutput meta,
        final IndexOutput data,
        final PipelineDescriptor pipeline,
        long totalValueCount,
        long[] blockOffsets
    ) throws IOException {
        meta.writeVInt(CURRENT_FORMAT_VERSION);

        pipeline.writeTo(meta);

        meta.writeVLong(totalValueCount);

        meta.writeVInt(blockOffsets.length);

        if (blockOffsets.length == 0) {
            meta.writeLong(0L);
            meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);
            meta.writeLong(0L);
            return;
        }

        long offsetsDataStart = data.getFilePointer();
        meta.writeLong(offsetsDataStart);
        meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);

        final DirectMonotonicWriter offsetsWriter = DirectMonotonicWriter.getInstance(
            meta,
            data,
            blockOffsets.length,
            DIRECT_MONOTONIC_BLOCK_SHIFT
        );

        for (long offset : blockOffsets) {
            offsetsWriter.add(offset);
        }
        offsetsWriter.finish();

        long offsetsDataEnd = data.getFilePointer();
        meta.writeLong(offsetsDataEnd - offsetsDataStart);
    }

    public static FieldDescriptor read(IndexInput meta) throws IOException {
        int formatVersion = meta.readVInt();

        return switch (formatVersion) {
            case 1 -> readVersion1(meta);
            default -> throw new IOException(
                "Unsupported FieldDescriptor format version: "
                    + formatVersion
                    + ". "
                    + "Maximum supported version is "
                    + CURRENT_FORMAT_VERSION
                    + ". "
                    + "This may indicate data written by a newer version of Elasticsearch."
            );
        };
    }

    private static FieldDescriptor readVersion1(final IndexInput meta) throws IOException {
        final PipelineDescriptor pipeline = PipelineDescriptor.readFrom(meta);

        long totalValueCount = meta.readVLong();

        int blockCount = meta.readVInt();

        long offsetsDataOffset = meta.readLong();
        int blockShift = meta.readVInt();

        DirectMonotonicReader.Meta offsetsMeta = null;
        if (blockCount > 0) {
            offsetsMeta = DirectMonotonicReader.loadMeta(meta, blockCount, blockShift);
        }

        long offsetsDataLength = meta.readLong();

        return new FieldDescriptor(pipeline, totalValueCount, blockCount, offsetsMeta, offsetsDataOffset, offsetsDataLength);
    }

    @Override
    public String toString() {
        return "FieldDescriptor{" + "pipeline=" + pipeline + ", totalValueCount=" + totalValueCount + ", blockCount=" + blockCount + '}';
    }
}
