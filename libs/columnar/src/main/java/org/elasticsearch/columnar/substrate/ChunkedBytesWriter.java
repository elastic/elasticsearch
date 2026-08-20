/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils;

import java.io.Closeable;
import java.io.IOException;

/**
 * Writes a column's byte stream as chunks: values are appended in order, and a chunk is emitted once it
 * reaches {@link #targetChunkBytes}. Chunks end on a value boundary, so a value never spans two of them and
 * reading one never needs more than one chunk.
 *
 * <p>Two tables locate a value. Callers record each value's offset in the <em>uncompressed</em> stream
 * themselves, which is what {@link #uncompressedLength()} returns after each append; this class records
 * where each chunk starts in that stream and where it lands in the file.
 *
 * <p>Nothing on the heap grows with the column: one chunk is buffered, and the two chunk tables are staged
 * in a temporary file because {@link MonotonicWriter} needs its entry count up front and the number of
 * chunks is only known once the last one is written.
 */
public final class ChunkedBytesWriter implements Closeable {

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
    private final int targetChunkBytes;
    private final IndexOutput data;
    private final long dataOffset;
    private final Directory directory;
    private final IOContext context;
    private final String prefix;

    /** Staged {@code (start, fileOffset)} pairs, one per chunk plus a past-the-end marker. */
    private final IndexOutput chunkTemp;
    private final String chunkTempName;

    private byte[] pending;
    private int pendingLength = 0;
    private long uncompressedLength = 0;
    private int numChunks = 0;
    private boolean finished = false;
    private boolean tempClosed = false;

    public ChunkedBytesWriter(
        ChunkCodec codec,
        int targetChunkBytes,
        Directory directory,
        IOContext context,
        String prefix,
        IndexOutput data
    ) throws IOException {
        if (targetChunkBytes <= 0) {
            throw new IllegalArgumentException("targetChunkBytes must be positive, got " + targetChunkBytes);
        }
        this.codec = codec;
        this.targetChunkBytes = targetChunkBytes;
        this.directory = directory;
        this.context = context;
        this.prefix = prefix;
        this.data = data;
        this.dataOffset = data.getFilePointer();
        this.pending = new byte[Math.min(targetChunkBytes, 64 * 1024)];
        this.chunkTemp = directory.createTempOutput(prefix, "columnar-chunk-index", context);
        this.chunkTempName = chunkTemp.getName();
    }

    /** The number of bytes appended so far; the offset the next appended value will start at. */
    public long uncompressedLength() {
        return uncompressedLength;
    }

    /**
     * Closes the pending chunk if it has reached its target size. Callers invoke this only where a chunk may
     * end, so that whatever they address — a value, a run of values — never straddles two chunks and a read
     * of it never spans more than one.
     */
    public void boundary() throws IOException {
        if (pendingLength >= targetChunkBytes) {
            flushChunk();
        }
    }

    /** Appends bytes to the pending chunk. */
    public void append(byte[] bytes, int offset, int length) {
        pending = ArrayUtil.grow(pending, pendingLength + length);
        System.arraycopy(bytes, offset, pending, pendingLength, length);
        pendingLength += length;
        uncompressedLength += length;
    }

    /** Emits any pending chunk, writes the index tables into {@code data}, and returns where everything is. */
    public Chunks finish() throws IOException {
        assert finished == false : "already finished";
        finished = true;
        if (pendingLength > 0) {
            flushChunk();
        }
        // Past-the-end markers, so a chunk's extent is the gap to the next entry.
        record(uncompressedLength, data.getFilePointer() - dataOffset);
        chunkTemp.close();
        tempClosed = true;

        final MonotonicWriter.Table startsTable;
        final MonotonicWriter.Table offsetsTable;
        try (
            IndexInput staged = directory.openInput(chunkTempName, context);
            MonotonicWriter startsOut = new MonotonicWriter(directory, context, prefix, numChunks + 1L);
            MonotonicWriter offsetsOut = new MonotonicWriter(directory, context, prefix, numChunks + 1L)
        ) {
            // Both tables are built in one replay, so the staged pairs are read exactly once.
            for (int i = 0; i <= numChunks; i++) {
                startsOut.add(staged.readVLong());
                offsetsOut.add(staged.readVLong());
            }
            startsTable = startsOut.finish(data);
            offsetsTable = offsetsOut.finish(data);
        }
        return new Chunks(codec.id(), numChunks, uncompressedLength, dataOffset, startsTable, offsetsTable);
    }

    private void flushChunk() throws IOException {
        record(uncompressedLength - pendingLength, data.getFilePointer() - dataOffset);
        codec.write(pending, pendingLength, data);
        pendingLength = 0;
        numChunks++;
    }

    private void record(long start, long fileOffset) throws IOException {
        chunkTemp.writeVLong(start);
        chunkTemp.writeVLong(fileOffset);
    }

    @Override
    public void close() throws IOException {
        try {
            if (tempClosed == false) {
                IOUtils.closeWhileHandlingException(chunkTemp);
            }
        } finally {
            IOUtils.deleteFilesIgnoringExceptions(directory, chunkTempName);
        }
    }
}
