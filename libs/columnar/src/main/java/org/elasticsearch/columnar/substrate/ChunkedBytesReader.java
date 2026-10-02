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
        final int within = (int) (offset - cachedStart);
        if (within + length <= cachedLength) {
            System.arraycopy(chunk, within, dst, 0, length);
        } else {
            gather(offset, length, dst);
        }
        return dst;
    }

    /**
     * Points {@code dst} at {@code length} bytes at {@code offset} without copying them where they lie in one
     * chunk, which is what a caller that decodes a block and then hands out the values inside it saves by.
     * Bytes spread over several chunks are put back together first. Either way they are valid only until the
     * next call on this reader, so a caller that keeps them must copy them.
     */
    public void span(long offset, int length, BytesRef dst) throws IOException {
        if (length == 0) {
            dst.bytes = chunk;
            dst.offset = 0;
            dst.length = 0;
            return;
        }
        if (codec.isIdentity()) {
            if (numChunks == 1) {
                // Everything the stream holds is in that one chunk, so keeping it costs a single read and
                // every value after the first is a slice of it. A stream of several chunks does not get the
                // same treatment: nothing says the next value read is in the chunk the last one was, and a
                // read that misses would copy a whole chunk to hand back a few bytes.
                ensureChunk(0);
                dst.bytes = chunk;
                dst.offset = (int) (offset - cachedStart);
                dst.length = length;
                return;
            }
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
        final int within = (int) (offset - cachedStart);
        if (within + length <= cachedLength) {
            dst.bytes = chunk;
            dst.offset = within;
            dst.length = length;
            return;
        }
        // A chunk is cut wherever its bound falls, so a range the caller addresses as one may be spread over
        // two of them or more. Those are put back together here, which is the only read that copies.
        verbatim = ArrayUtil.growNoCopy(verbatim, length);
        gather(offset, length, verbatim);
        dst.bytes = verbatim;
        dst.offset = 0;
        dst.length = length;
    }

    /** Copies {@code length} bytes at {@code offset} out of however many chunks hold them. */
    private void gather(long offset, int length, byte[] dst) throws IOException {
        int written = 0;
        long at = offset;
        while (written < length) {
            ensureChunk(chunkContaining(at));
            final int within = (int) (at - cachedStart);
            final int take = Math.min(cachedLength - within, length - written);
            System.arraycopy(chunk, within, dst, written, take);
            written += take;
            at += take;
        }
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
