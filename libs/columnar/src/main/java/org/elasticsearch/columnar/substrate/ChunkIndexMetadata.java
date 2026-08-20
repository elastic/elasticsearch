/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;

/**
 * Where a column's chunked byte stream lives and how to reopen it: the codec the chunks were stored with,
 * and the two tables mapping a chunk to its start in the uncompressed stream and to its position in the file.
 */
public record ChunkIndexMetadata(
    byte codecId,
    int numChunks,
    long uncompressedLength,
    long dataOffset,
    long startsDataOffset,
    long startsDataLength,
    byte[] startsMeta,
    long fileOffsetsDataOffset,
    long fileOffsetsDataLength,
    byte[] fileOffsetsMeta
) {
    private static final byte[] NONE = new byte[0];

    public static ChunkIndexMetadata of(ChunkedBytesWriter.Chunks chunks) {
        return new ChunkIndexMetadata(
            chunks.codecId(),
            chunks.numChunks(),
            chunks.uncompressedLength(),
            chunks.dataOffset(),
            chunks.starts().dataOffset(),
            chunks.starts().dataLength(),
            chunks.starts().meta(),
            chunks.fileOffsets().dataOffset(),
            chunks.fileOffsets().dataLength(),
            chunks.fileOffsets().meta()
        );
    }

    public static ChunkIndexMetadata empty() {
        return new ChunkIndexMetadata(ChunkCodec.IDENTITY_ID, 0, 0, 0, 0, 0, NONE, 0, 0, NONE);
    }

    public void writeTo(DataOutput out) throws IOException {
        out.writeByte(codecId);
        out.writeVInt(numChunks);
        if (numChunks == 0) {
            return;
        }
        out.writeVLong(uncompressedLength);
        out.writeVLong(dataOffset);
        writeTable(out, startsDataOffset, startsDataLength, startsMeta);
        writeTable(out, fileOffsetsDataOffset, fileOffsetsDataLength, fileOffsetsMeta);
    }

    public static ChunkIndexMetadata readFrom(DataInput in) throws IOException {
        final byte codecId = in.readByte();
        final int numChunks = in.readVInt();
        if (numChunks == 0) {
            return empty();
        }
        final long uncompressedLength = in.readVLong();
        final long dataOffset = in.readVLong();
        final long startsOffset = in.readVLong();
        final long startsLength = in.readVLong();
        final byte[] startsMeta = readBytes(in);
        final long offsetsOffset = in.readVLong();
        final long offsetsLength = in.readVLong();
        final byte[] offsetsMeta = readBytes(in);
        return new ChunkIndexMetadata(
            codecId,
            numChunks,
            uncompressedLength,
            dataOffset,
            startsOffset,
            startsLength,
            startsMeta,
            offsetsOffset,
            offsetsLength,
            offsetsMeta
        );
    }

    /** Opens the byte stream this describes over {@code data}. */
    public ChunkedBytesReader open(IndexInput data) throws IOException {
        if (numChunks == 0) {
            // No bytes were written, so there is no index to open; only zero-length reads can follow.
            return new ChunkedBytesReader(data, ChunkCodec.forId(codecId), 0, null, null, 0);
        }
        return new ChunkedBytesReader(
            data,
            ChunkCodec.forId(codecId),
            dataOffset,
            MonotonicReader.open(data, startsMeta, numChunks + 1L, startsDataOffset, startsDataLength),
            MonotonicReader.open(data, fileOffsetsMeta, numChunks + 1L, fileOffsetsDataOffset, fileOffsetsDataLength),
            numChunks
        );
    }

    private static void writeTable(DataOutput out, long offset, long length, byte[] meta) throws IOException {
        out.writeVLong(offset);
        out.writeVLong(length);
        out.writeVInt(meta.length);
        out.writeBytes(meta, 0, meta.length);
    }

    private static byte[] readBytes(DataInput in) throws IOException {
        final byte[] bytes = new byte[in.readVInt()];
        in.readBytes(bytes, 0, bytes.length);
        return bytes;
    }
}
