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
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;

/**
 * Lets several threads fill disjoint slices of a destination {@link ByteBuffer} until the thread that owns the buffer
 * {@linkplain #complete completes} the fill. Used to populate the read buffer of a Lucene read from a cache file whose missing ranges
 * are fetched, and copied into the buffer, by other threads.
 *
 * <p>Once {@link #complete} returns, no filler is running and none will run, so the owner may reuse or free the buffer. That includes
 * native memory of a thread-confined {@link java.lang.foreign.Arena}, which other threads cannot reach through the buffer itself: for
 * those, slices are short-lived views of the memory that are not bound to the arena, created per {@link #fillSlice} call. Every other
 * buffer, including one of a shared or automatic arena, is sliced normally.
 *
 * <p>Fillers may only write to their slice, and only during the call. That cannot be checked. Slice bounds are, and with assertions
 * enabled so are overlapping regions and completing with more bytes than were claimed.
 */
public abstract class ConcurrentBufferFill {

    /**
     * Stands in for any thread other than the one that created the fill, to find out whether the
     * buffer is confined to that thread. It is never started.
     */
    private static final Thread ANOTHER_THREAD = new Thread(() -> {}, "concurrent-buffer-fill-another-thread");

    private final Semaphore permits = new Semaphore(Integer.MAX_VALUE);

    protected ByteBuffer buffer;

    /** The thread that created this fill, which must complete it; {@code null} unless asserting. */
    private final Thread owner = Assertions.ENABLED ? Thread.currentThread() : null;

    /** Claimed regions as start to end (exclusive); {@code null} unless asserting. */
    private final TreeMap<Integer, Integer> claimedRegions = Assertions.ENABLED ? new TreeMap<>() : null;

    private ConcurrentBufferFill(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    /**
     * Must be called on the thread that owns {@code buffer}, which must also call {@link #complete}; that
     * is checked when assertions are enabled. Fills the bytes between the buffer's position and its limit.
     */
    public static ConcurrentBufferFill of(ByteBuffer buffer) {
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
     * @param position start of the slice, relative to the buffer's position when this fill was created
     * @param length slice length
     * @param valueIfComplete returned, without running the filler, if {@link #complete} was already called; the read has been abandoned
     *                        by then, so for a filler that returns a byte count this should make callers treat it as done
     * @return the filler's result, or {@code valueIfComplete}
     * @throws IndexOutOfBoundsException if the slice does not lie within the buffer
     */
    public <R, E extends Exception> R fillSlice(int position, int length, R valueIfComplete, CheckedFunction<ByteBuffer, R, E> filler)
        throws E {
        if (permits.tryAcquire() == false) {
            return valueIfComplete;
        }
        try {
            final ByteBuffer slice = newSlice(position, length);
            assert claim(position, length);
            return filler.apply(slice);
        } finally {
            permits.release();
        }
    }

    /** Returns a slice of the buffer that the caller holding a permit may write to. */
    abstract ByteBuffer newSlice(int position, int length);

    /**
     * Ends the fill: waits, uninterruptibly, for fillers in progress, then advances the buffer's position. Later calls have no effect.
     * Must be called on the thread that created the fill, so that the owner cannot free or reuse the buffer before this returns.
     *
     * @param bytesFilled bytes to advance the position by, at most the total length of the claimed regions. Zero if nothing was filled or
     *                    the caller accounts for it itself.
     */
    public void complete(int bytesFilled) {
        assert Thread.currentThread() == owner : "completed on [" + Thread.currentThread() + "] but created on [" + owner + "]";
        if (buffer != null) {
            assert bytesFilled == 0 || permits.availablePermits() == Integer.MAX_VALUE
                : "Try to complete with [" + bytesFilled + "] bytes but only had [" + permits.availablePermits() + "] permits available.";
            permits.acquireUninterruptibly(Integer.MAX_VALUE);
            assert bytesFilled <= claimedLength()
                : "completing with [" + bytesFilled + "] bytes filled but only [" + claimedLength() + "] claimed";
            buffer.position(buffer.position() + bytesFilled); // mark all bytes as accounted for
            buffer = null;
        }
    }

    private boolean claim(int position, int length) {
        if (length == 0) {
            return true;
        }
        final int end = position + length;
        synchronized (claimedRegions) {
            final Map.Entry<Integer, Integer> before = claimedRegions.floorEntry(position);
            final Map.Entry<Integer, Integer> after = claimedRegions.ceilingEntry(position);
            if (before != null && before.getValue() > position) {
                throw new AssertionError(
                    "slice [" + position + "," + end + ") overlaps [" + before.getKey() + "," + before.getValue() + ")"
                );
            }
            if (after != null && after.getKey() < end) {
                throw new AssertionError("slice [" + position + "," + end + ") overlaps [" + after.getKey() + "," + after.getValue() + ")");
            }
            claimedRegions.put(position, end);
        }
        return true;
    }

    private long claimedLength() {
        synchronized (claimedRegions) {
            return claimedRegions.entrySet().stream().mapToLong(e -> e.getValue() - e.getKey()).sum();
        }
    }

    /** A buffer that any thread can slice, which is every kind but the memory of a confined arena. */
    private static final class Sliced extends ConcurrentBufferFill {
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
    private static final class Unscoped extends ConcurrentBufferFill {
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
