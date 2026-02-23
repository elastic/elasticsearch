/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.simdvec.internal;

import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.DirectAccessInput;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

/**
 * Utility for obtaining a {@link MemorySegment} view of data in an
 * {@link IndexInput} and passing it to a caller-supplied action. The
 * segment may come from a {@link MemorySegmentAccessInput} (mmap),
 * a direct {@link java.nio.ByteBuffer} view (e.g. blob-cache), or a
 * heap copy as a last resort.
 *
 * <p>All resource management (ref-counting, buffer release) is handled
 * internally — callers never see a closeable resource.
 */
public final class IndexInputUtils {

    private IndexInputUtils() {}

    /**
     * Obtains a memory segment for the next {@code length} bytes of the
     * index input, passes it to {@code action}, and returns the result.
     * The position of the index input is advanced by {@code length}.
     *
     * <p> This method first tries to obtain a slice via
     * {@link MemorySegmentAccessInput#segmentSliceOrNull}. If that
     * returns {@code null}, it tries a direct {@link java.nio.ByteBuffer}
     * view via {@link DirectAccessInput}. As a last resort it copies the
     * data onto the heap.
     *
     * <p> The memory segment passed to {@code action} is valid only for
     * the duration of the call. Callers must not retain references to it.
     *
     * @param in         the index input positioned at the data to read
     * @param length     the number of bytes to read
     * @param action     the function to apply to the memory segment
     * @return the result of applying {@code action}
     */
    public static <R> R withSlice(IndexInput in, long length, CheckedFunction<MemorySegment, R, IOException> action) throws IOException {
        checkInputType(in);
        if (in instanceof MemorySegmentAccessInput msai) {
            long offset = in.getFilePointer();
            MemorySegment slice = msai.segmentSliceOrNull(offset, length);
            if (slice != null) {
                in.skipBytes(length);
                return action.apply(slice);
            }
        }
        if (in instanceof DirectAccessInput dai) {
            long offset = in.getFilePointer();
            @SuppressWarnings("unchecked")
            R[] result = (R[]) new Object[1];
            boolean available = dai.withByteBufferSlice(offset, length, bb -> {
                in.skipBytes(length);
                result[0] = action.apply(MemorySegment.ofBuffer(bb));
            });
            if (available) {
                return result[0];
            }
        }
        return action.apply(copyOnHeap(in, Math.toIntExact(length)));
    }

    /**
     * Checks that a {@link FilterIndexInput} wrapper also implements
     * {@link MemorySegmentAccessInput} or {@link DirectAccessInput},
     * so that zero-copy access is preserved through the wrapper chain.
     */
    public static void checkInputType(IndexInput in) {
        if (in instanceof FilterIndexInput && (in instanceof MemorySegmentAccessInput || in instanceof DirectAccessInput) == false) {
            throw new IllegalArgumentException(
                "IndexInput is a FilterIndexInput ("
                    + in.getClass().getName()
                    + ") that does not implement MemorySegmentAccessInput or DirectAccessInput. "
                    + "Ensure the wrapper implements DirectAccessInput or is unwrapped before constructing the scorer."
            );
        }
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
