/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.store.RandomAccessInput;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.DirectAccessInput;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.function.IntFunction;

/**
 * Utility for zero-copy {@link ByteBuffer} access to data in a {@link RandomAccessInput}.
 *
 * <p>The helper tries the following paths in order:
 * <ol>
 *   <li>{@link DirectAccessInput} — stateless / blob-cache inputs</li>
 *   <li>{@link MemorySegmentAccessInput} — mmap-backed inputs</li>
 *   <li>Heap copy fallback — {@code readBytes} into a scratch array, wrapped as a {@code ByteBuffer}</li>
 * </ol>
 *
 * <p>The caller always receives a {@link ByteBuffer}; the backing memory may be direct
 * (zero-copy) or heap-backed (copy fallback). The buffer is valid only for the duration
 * of the function call. Callers must not retain references to it.
 */
public final class RandomAccessInputUtils {

    private RandomAccessInputUtils() {}

    /**
     * Provides a {@link ByteBuffer} view of {@code length} bytes at {@code offset} in the
     * given input, passes it to {@code function}, and returns the result. Zero-copy when
     * the input supports it, otherwise copies into a scratch array obtained from
     * {@code scratchSupplier} and wraps it.
     *
     * @param input           the random access input
     * @param offset          byte offset within the input
     * @param length          number of bytes
     * @param scratchSupplier supplies a byte array of at least the requested length;
     *                        invoked only on the heap-copy fallback path
     * @param function        receives the {@link ByteBuffer} and returns a value; must not retain a reference after returning
     * @return the value returned by {@code function}
     */
    public static <R> R withByteBufferSlice(
        RandomAccessInput input,
        long offset,
        int length,
        IntFunction<byte[]> scratchSupplier,
        CheckedFunction<ByteBuffer, R, IOException> function
    ) throws IOException {
        if (input instanceof DirectAccessInput dai) {
            @SuppressWarnings("unchecked")
            R[] holder = (R[]) new Object[1];
            if (dai.withByteBufferSlice(offset, length, buf -> holder[0] = function.apply(buf))) {
                return holder[0];
            }
        }
        if (input instanceof MemorySegmentAccessInput msai) {
            MemorySegment seg = msai.segmentSliceOrNull(offset, length);
            if (seg != null) {
                return function.apply(seg.asByteBuffer().asReadOnlyBuffer());
            }
        }
        byte[] scratch = scratchSupplier.apply(length);
        input.readBytes(offset, scratch, 0, length);
        return function.apply(ByteBuffer.wrap(scratch, 0, length));
    }
}
