/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;

import java.io.IOException;

/**
 * Reads a byte stream written by {@link ChunkedBytesWriter}, addressed by offset in the uncompressed stream.
 *
 * <p>A chunk is decoded whole into a reused buffer with a single-chunk cache, so a scan decodes each chunk
 * once however many values it holds, and a caller that reads values in order pays one chunk decode per
 * chunk rather than any work per value. Under the identity codec nothing is decoded at all: the bytes are
 * read straight from the mapped input, so no chunk buffer is ever allocated.
 */
public final class ChunkedBytesReader {

    private final IndexInput data;
    private final ChunkCodec codec;
    private final ChunkDecompressor decompressor;
    private final long dataOffset;
    private final LongValues starts;
    private final LongValues fileOffsets;
    private final long numChunks;

    private byte[] chunk = new byte[0];
    // Holds a span read from a verbatim chunk, where there is no decoded chunk to point into.
    private byte[] verbatim = new byte[0];
    private long cachedChunk = -1;
    private long cachedStart = 0;
    private int cachedLength = 0;

    public ChunkedBytesReader(
        IndexInput data,
        ChunkCodec codec,
        long dataOffset,
        LongValues starts,
        LongValues fileOffsets,
        long numChunks
    ) {
        // Cloned because reading a chunk seeks: the input this was opened over is shared, and a segment is
        // read by many threads at once.
        this.data = data.clone();
        this.codec = codec;
        this.decompressor = codec.newDecompressor();
        this.dataOffset = dataOffset;
        this.starts = starts;
        this.fileOffsets = fileOffsets;
        this.numChunks = numChunks;
    }

    /**
     * Reads {@code length} bytes at {@code offset} in the uncompressed stream into {@code dst}, growing it if
     * needed, and returns the buffer the bytes landed in. The returned array is only valid until the next
     * call, and the bytes always start at position 0.
     */
    public byte[] read(long offset, int length, byte[] dst) throws IOException {
        if (length == 0) {
            // Every value is empty, so the column holds no bytes and no chunk was ever written.
            return dst;
        }
        dst = ArrayUtil.growNoCopy(dst, length);
        if (codec.isIdentity()) {
            // An optimization rather than a separate layout: the bytes could be read through the chunk
            // index like any other, but under this codec the uncompressed offset is already a file offset,
            // so a chunk need never be decoded and no buffer is needed to hold one.
            data.seek(dataOffset + offset);
            data.readBytes(dst, 0, length);
            return dst;
        }
        final long index = chunkContaining(offset);
        ensureChunk(index);
        assert offset - cachedStart + length <= cachedLength
            : "read of "
                + length
                + " at "
                + offset
                + " spans past chunk "
                + index
                + "; chunks must close on the boundaries the caller addresses";
        System.arraycopy(chunk, (int) (offset - cachedStart), dst, 0, length);
        return dst;
    }

    /**
     * Points {@code dst} at {@code length} bytes at {@code offset} rather than copying them. The bytes are
     * the decoded chunk's own, or the file's under the identity codec, and stay valid only until the next
     * call on this reader. A caller that decodes a block and then hands out the values inside it saves a
     * copy of the whole block this way; one that keeps the bytes must copy them.
     */
    public void span(long offset, int length, BytesRef dst) throws IOException {
        if (length == 0) {
            dst.bytes = chunk;
            dst.offset = 0;
            dst.length = 0;
            return;
        }
        if (codec.isIdentity()) {
            // Stored verbatim in the file, so there is no decoded chunk to point into.
            verbatim = ArrayUtil.growNoCopy(verbatim, length);
            data.seek(dataOffset + offset);
            data.readBytes(verbatim, 0, length);
            dst.bytes = verbatim;
            dst.offset = 0;
            dst.length = length;
            return;
        }
        final long index = chunkContaining(offset);
        ensureChunk(index);
        assert offset - cachedStart + length <= cachedLength : "span of " + length + " at " + offset + " leaves chunk " + index;
        dst.bytes = chunk;
        dst.offset = (int) (offset - cachedStart);
        dst.length = length;
    }

    /** The chunk holding {@code offset}, by binary search over the chunk starts. */
    private long chunkContaining(long offset) {
        if (cachedChunk >= 0 && offset >= cachedStart && offset < cachedStart + cachedLength) {
            return cachedChunk;
        }
        long low = 0;
        long high = numChunks - 1;
        while (low < high) {
            final long mid = (low + high + 1) >>> 1;
            if (starts.get(mid) <= offset) {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        return low;
    }

    private void ensureChunk(long index) throws IOException {
        if (index == cachedChunk) {
            return;
        }
        final long start = starts.get(index);
        final int uncompressed = (int) (starts.get(index + 1) - start);
        final long fileStart = fileOffsets.get(index);
        final int stored = (int) (fileOffsets.get(index + 1) - fileStart);
        chunk = ArrayUtil.growNoCopy(chunk, uncompressed);
        data.seek(dataOffset + fileStart);
        decompressor.read(data, stored, chunk, uncompressed);
        cachedChunk = index;
        cachedStart = start;
        cachedLength = uncompressed;
    }
}
