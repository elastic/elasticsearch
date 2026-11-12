/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.common;

import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.nativeaccess.SegmentBuffers;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Semaphore;

/**
 * Lets several threads fill disjoint slices of a destination {@link ByteBuffer} until the thread that owns the buffer
 * {@linkplain #complete completes} the scope. Used to populate the read buffer of a Lucene read from a cache file whose missing ranges
 * are fetched, and copied into the buffer, by other threads.
 *
 * <p>Once {@link #complete} returns, no filler is running and none will run, so the owner may reuse or free the buffer. That includes
 * native memory of a thread-confined {@link java.lang.foreign.Arena}, which other threads cannot reach through the buffer itself: for
 * those, slices are short-lived views of the memory that are not bound to the arena, created per {@link #fillSlice} call. Every other
 * buffer, including one of a shared or automatic arena, is sliced normally.
 *
 * <p>Fillers may only write to their slice, and only during the call. That cannot be checked. Slice bounds are, and with assertions
 * enabled so are overlapping regions, completing with more bytes than were claimed, and the memory of a confined arena being freed
 * while the scope is not complete; see {@code AssertingBufferFillScope}.
 */
public abstract sealed class BufferFillScope permits BufferFillScope.Plain, AssertingBufferFillScope {

    /**
     * Stands in for any thread other than the one that created the scope, to find out whether the
     * buffer is confined to that thread. It is never started.
     */
    private static final Thread ANOTHER_THREAD = new Thread(() -> {}, "buffer-fill-scope-another-thread");

    /**
     * Must be called on the thread that owns {@code buffer}, which must also call {@link #complete}; that
     * is checked when assertions are enabled. Fills the bytes between the buffer's position and its limit.
     */
    public static BufferFillScope of(ByteBuffer buffer) {
        final BufferFillScope scope = newScope(buffer);
        return Assertions.ENABLED ? new AssertingBufferFillScope(scope, buffer) : scope;
    }

    private static BufferFillScope newScope(ByteBuffer buffer) {
        // Only the memory of a confined arena needs the address: a buffer of any other kind, whether it is on the heap, direct, read-only
        // or of a shared or automatic arena, can be sliced by any thread, and slicing it keeps the JDK's thread and liveness checks.
        if (buffer.isDirect() && buffer.isReadOnly() == false) {
            // The segment is created here, on the owner thread, and only its address is kept: the address is plain data that other
            // threads can use, while the segment's scope would reject them
            final MemorySegment segment = MemorySegment.ofBuffer(buffer);
            if (segment.isAccessibleBy(ANOTHER_THREAD) == false) {
                return new Unscoped(buffer, segment.address(), buffer.remaining());
            }
        }
        return new Sliced(buffer);
    }

    /**
     * Runs {@code filler} with a slice of the buffer to write to, holding off {@link #complete} meanwhile. The filler must not retain the
     * slice. The region must not overlap one claimed by an earlier call, which is checked when assertions are enabled.
     *
     * @param position start of the slice, relative to the buffer's position when this scope was created
     * @param length slice length
     * @param filler writes to the slice, and returns a non-null result
     * @return the filler's result, or empty, without running the filler, if {@link #complete} was already called: the read has been
     *         abandoned by then, so the caller should treat it as done
     * @throws IndexOutOfBoundsException if the slice does not lie within the buffer
     */
    public abstract <R, E extends Exception> Optional<R> fillSlice(int position, int length, CheckedFunction<ByteBuffer, R, E> filler)
        throws E;

    /**
     * Ends the scope: waits, uninterruptibly, for fillers in progress, then advances the buffer's position. Later calls have no effect.
     * Must be called on the thread that created the scope, so that the owner cannot free or reuse the buffer before this returns.
     *
     * <p>The wait is deliberately neither interruptible nor timed: returning while a filler is still running would let the owner reuse or
     * free the buffer under it, which for the memory of a confined arena is silent corruption, so there is no safe way to give up early.
     * It lasts only as long as the fillers, which hold a permit just for the IO that copies into their slice and must never wait on the
     * owner.
     *
     * @param bytesFilled bytes to advance the position by, at most the total length of the claimed regions. Zero if nothing was filled or
     *                    the caller accounts for it itself.
     */
    public abstract void complete(int bytesFilled);

    /** The scope itself, without any assertions: a slice of the buffer is made by the subclass, and handed out under a permit. */
    abstract static sealed class Plain extends BufferFillScope permits Sliced, Unscoped {
        private final Semaphore permits = new Semaphore(Integer.MAX_VALUE);

        protected ByteBuffer buffer;

        Plain(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        /** Returns a slice of the buffer that the caller holding a permit may write to. */
        abstract ByteBuffer newSlice(int position, int length);

        @Override
        public final <R, E extends Exception> Optional<R> fillSlice(int position, int length, CheckedFunction<ByteBuffer, R, E> filler)
            throws E {
            if (permits.tryAcquire() == false) {
                return Optional.empty();
            }
            try {
                return Optional.of(filler.apply(newSlice(position, length)));
            } finally {
                permits.release();
            }
        }

        @Override
        public final void complete(int bytesFilled) {
            if (buffer != null) {
                // Neither interruptible nor timed, on purpose: see the Javadoc. Taking every permit also stops any later filler.
                permits.acquireUninterruptibly(Integer.MAX_VALUE);
                buffer.position(buffer.position() + bytesFilled); // mark all bytes as accounted for
                buffer = null;
            }
        }
    }

    /** A buffer that any thread can slice, which is every kind but the memory of a confined arena. */
    private static final class Sliced extends Plain {
        private final int start;

        Sliced(ByteBuffer buffer) {
            super(buffer);
            this.start = buffer.position();
        }

        @Override
        ByteBuffer newSlice(int position, int length) {
            return buffer.slice(start + position, length);
        }
    }

    /**
     * The native memory of a confined arena, which other threads cannot reach through the buffer. Slices are short-lived views of the
     * memory that are not bound to the arena, created per call.
     */
    private static final class Unscoped extends Plain {
        private final long address;
        private final int capacity;

        Unscoped(ByteBuffer buffer, long address, int capacity) {
            super(buffer);
            this.address = address;
            this.capacity = capacity;
        }

        @Override
        @SuppressForbidden(reason = "a slice is only handed to a filler holding a permit, which complete waits for")
        ByteBuffer newSlice(int position, int length) {
            Objects.checkFromIndexSize(position, length, capacity);
            return SegmentBuffers.unscopedByteBuffer(address + position, length);
        }
    }
}
