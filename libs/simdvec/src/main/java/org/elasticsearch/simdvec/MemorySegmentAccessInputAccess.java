/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.elasticsearch.core.CheckedConsumer;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.Objects;

public interface MemorySegmentAccessInputAccess {
    /** Returns the underlying {@link MemorySegmentAccessInput}, or {@code null} if not available. */
    MemorySegmentAccessInput get();

    /** Unwraps to the underlying {@link MemorySegmentAccessInput} if available, otherwise returns the input unchanged. */
    static IndexInput unwrap(IndexInput input) {
        MemorySegmentAccessInput memorySeg = input instanceof MemorySegmentAccessInputAccess msaia ? msaia.get() : null;
        return Objects.requireNonNullElse((IndexInput) memorySeg, input);
    }

    /**
     * Bridges a {@link MemorySegmentAccessInput} to the {@link ByteBuffer}-based
     * {@link org.elasticsearch.core.DirectAccessInput} contract. This exists so that wrapper
     * classes (e.g. {@code ReopeningIndexInput}) that cannot directly implement
     * {@code MemorySegmentAccessInput}, because {@code MemorySegment} is a preview API on
     * JDK 21 and the covariant {@code clone()} return type is incompatible with a mutable
     * delegate, can still provide zero-copy access to mmap'd data through
     * {@code DirectAccessInput.withByteBufferSlice}.
     *
     * <p>If the given {@link MemorySegmentAccessInput} can provide a contiguous memory segment
     * for the requested range, wraps it as a read-only {@link ByteBuffer} and passes it to
     * the action. Returns {@code true} if the action was invoked, {@code false} otherwise.
     *
     * <p>The byte buffer is valid only for the duration of the action. Callers must not
     * retain references to it after the action returns.
     */
    static boolean withByteBufferSlice(
        MemorySegmentAccessInput msai,
        long offset,
        long length,
        CheckedConsumer<ByteBuffer, IOException> action
    ) throws IOException {
        MemorySegment seg = msai.segmentSliceOrNull(offset, length);
        if (seg != null) {
            action.accept(seg.asByteBuffer().asReadOnlyBuffer());
            return true;
        }
        return false;
    }

    /**
     * Bulk variant of {@link #withByteBufferSlice}. See that method for the rationale behind
     * this {@code MemorySegment}-to-{@code ByteBuffer} bridge.
     *
     * <p>Resolves {@code count} ranges from the given {@link MemorySegmentAccessInput} to
     * read-only {@link ByteBuffer}s and passes them to the action. Returns {@code true} if
     * all ranges were available and the action was invoked, {@code false} otherwise (the
     * action is not invoked if any range is unavailable).
     *
     * <p>The byte buffers are valid only for the duration of the action. Callers must not
     * retain references to them after the action returns.
     */
    static boolean withByteBufferSlices(
        MemorySegmentAccessInput msai,
        long[] offsets,
        int length,
        int count,
        CheckedConsumer<ByteBuffer[], IOException> action
    ) throws IOException {
        ByteBuffer[] buffers = new ByteBuffer[count];
        for (int i = 0; i < count; i++) {
            MemorySegment seg = msai.segmentSliceOrNull(offsets[i], length);
            if (seg == null) {
                return false;
            }
            buffers[i] = seg.asByteBuffer().asReadOnlyBuffer();
        }
        action.accept(buffers);
        return true;
    }
}
