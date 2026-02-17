/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.simdvec.internal;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.nativeaccess.ByteBufferAccessInput;
import org.elasticsearch.nativeaccess.CloseableByteBuffer;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

/**
 * Utility for obtaining a {@link MemorySegment} view of data in an
 * {@link IndexInput}. The segment may come from an existing whole-file
 * memory-mapped segment, a direct {@link java.nio.ByteBuffer} view
 * (e.g. blob-cache), or a heap copy as a last resort.
 */
public final class IndexInputSegments {

    private IndexInputSegments() {}

    /** A memory segment paired with an optional resource to close when the segment is no longer needed. */
    public record SegmentSlice(MemorySegment segment, CloseableByteBuffer toClose) implements AutoCloseable {
        @Override
        public void close() {
            if (toClose != null) {
                toClose.close();
            }
        }
    }

    /**
     * Returns a memory segment containing the next {@code length} bytes of the
     * index input. The position of the index input is advanced by {@code length}.
     *
     * <p>The method first tries to slice the given {@code rawSegment} (a whole-file
     * mmap). If that is {@code null}, it tries a direct {@link java.nio.ByteBuffer}
     * view via {@link ByteBufferAccessInput}. As a last resort it copies the data
     * onto the heap.
     *
     * <p>The caller must close the returned {@link SegmentSlice} when done.
     *
     * @param in         the index input positioned at the data to read
     * @param rawSegment the backing memory segment for the whole file, or {@code null}
     * @param length     the number of bytes to read
     * @return a {@link SegmentSlice} wrapping the data
     */
    public static SegmentSlice sliceOrCopy(IndexInput in, MemorySegment rawSegment, long length) throws IOException {
        if (rawSegment != null) {
            long offset = in.getFilePointer();
            MemorySegment seg = rawSegment.asSlice(offset, length);
            in.skipBytes(length);
            return new SegmentSlice(seg, null);
        }
        // try direct ByteBuffer access (e.g., from blob cache mmap'd regions)
        if (in instanceof ByteBufferAccessInput bbai) {
            long offset = in.getFilePointer();
            CloseableByteBuffer cbb = bbai.byteBufferSliceOrNull(offset, length);
            if (cbb != null) {
                in.skipBytes(length);
                return new SegmentSlice(MemorySegment.ofBuffer(cbb.buffer()), cbb);
            }
        }
        return new SegmentSlice(copyOnHeap(in, Math.toIntExact(length)), null);
    }

    /**
     * Reads the given number of bytes from the current position of the
     * given IndexInput into a heap-backed memory segment.
     */
    private static MemorySegment copyOnHeap(IndexInput in, int bytesToRead) throws IOException {
        byte[] scratch = new byte[bytesToRead];
        in.readBytes(scratch, 0, bytesToRead);
        return MemorySegment.ofArray(scratch);
    }
}
