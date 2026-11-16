/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;

import java.io.IOException;

/**
 * Writes a column's byte stream as chunks: values are appended in order, and a chunk is emitted once it
 * reaches either of its {@link ChunkBounds}. A chunk is cut wherever the byte bound falls, including inside
 * a value, so no chunk is ever larger than the bound however large a single value is.
 *
 * <p>Two tables locate a value. Callers record each value's offset in the <em>uncompressed</em> stream
 * themselves, which is what {@link #uncompressedLength()} returns after each append; this class records
 * where each chunk starts in that stream and where it lands in the file.
 *
 * <p>The chunks go to the data and the two tables to the navigation, each as it is produced. Nothing on the
 * heap grows with the column: one chunk is buffered, and a table holds at most one block of its entries.
 */
public final class ChunkedBytesWriter {

    /** Where the chunks and their index landed, and what is needed to read them back. */
    public record Chunks(
        byte codecId,
        int numChunks,
        long uncompressedLength,
        long dataOffset,
        MonotonicWriter.Table starts,
        MonotonicWriter.Table fileOffsets
    ) {}

    private final ChunkCodec codec;
    private final ChunkCompressor compressor;
    private final ChunkBounds bounds;
    private final IndexOutput data;
    private final long dataOffset;

    /** Where each chunk starts in the uncompressed stream and in the file, plus a past-the-end entry. */
    private final MonotonicWriter starts;
    private final MonotonicWriter fileOffsets;

    private byte[] pending;
    private int pendingLength = 0;
    private int pendingValues = 0;
    private long uncompressedLength = 0;
    private int numChunks = 0;
    private boolean finished = false;

    public ChunkedBytesWriter(ChunkCodec codec, ChunkBounds bounds, IndexOutput data, IndexOutput navigation) {
        this.codec = codec;
        this.compressor = codec.newCompressor();
        this.bounds = bounds;
        this.data = data;
        this.dataOffset = data.getFilePointer();
        this.pending = new byte[Math.min(bounds.targetBytes(), 64 * 1024)];
        this.starts = new MonotonicWriter(navigation);
        this.fileOffsets = new MonotonicWriter(navigation);
    }

    /** The number of bytes appended so far; the offset the next appended value will start at. */
    public long uncompressedLength() {
        return uncompressedLength;
    }

    /**
     * Closes the pending chunk if the {@code values} the caller is about to append would take it past the
     * value bound, and counts them towards the chunk they land in. The byte bound needs no such warning: it
     * is enforced as the bytes arrive.
     */
    public void boundary(int values) throws IOException {
        // A chunk with no bytes in it is nothing to decompress and nothing to cut, so only a chunk that holds
        // something closes: a run of zero-length values reaches the value bound while holding no bytes at all.
        if (pendingLength > 0 && pendingValues + values > bounds.maxValues()) {
            flushChunk();
        }
        pendingValues += values;
    }

    /**
     * Appends bytes to the pending chunk, closing it every time it fills. A run of bytes longer than a chunk
     * is spread over as many as it takes, so the bound holds whatever the caller appends in one go.
     */
    public void append(byte[] bytes, int offset, int length) throws IOException {
        int at = offset;
        int remaining = length;
        while (remaining > 0) {
            final int take = Math.min(bounds.targetBytes() - pendingLength, remaining);
            pending = ArrayUtil.grow(pending, pendingLength + take);
            System.arraycopy(bytes, at, pending, pendingLength, take);
            pendingLength += take;
            uncompressedLength += take;
            at += take;
            remaining -= take;
            if (pendingLength == bounds.targetBytes()) {
                flushChunk();
            }
        }
    }

    /** Emits any pending chunk and returns where the chunks and their index are. */
    public Chunks finish() throws IOException {
        assert finished == false : "already finished";
        finished = true;
        if (pendingLength > 0) {
            flushChunk();
        }
        if (numChunks == 0) {
            // Nothing was written, so there is no chunk for a table to locate.
            return new Chunks(codec.id(), 0, 0, dataOffset, MonotonicWriter.Table.NONE, MonotonicWriter.Table.NONE);
        }
        // Past-the-end markers, so a chunk's extent is the gap to the next entry.
        record(uncompressedLength, data.getFilePointer() - dataOffset);
        return new Chunks(codec.id(), numChunks, uncompressedLength, dataOffset, starts.finish(), fileOffsets.finish());
    }

    private void flushChunk() throws IOException {
        assert pendingLength <= bounds.targetBytes() : "chunk of " + pendingLength + " over a bound of " + bounds.targetBytes();
        record(uncompressedLength - pendingLength, data.getFilePointer() - dataOffset);
        compressor.write(pending, pendingLength, data);
        pendingLength = 0;
        pendingValues = 0;
        numChunks++;
    }

    private void record(long start, long fileOffset) throws IOException {
        starts.add(start);
        fileOffsets.add(fileOffset);
    }
}
