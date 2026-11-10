/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.common;

import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.CheckedFunction;
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
 * those, slices are short-lived views of the memory that are not bound to the arena, created per {@link #fillSlice} call.
 *
 * <p>Fillers may only write to their slice, and only during the call. That cannot be checked. Slice bounds are, and with assertions
 * enabled so are overlapping regions and completing with more bytes than were claimed.
 */
public final class ConcurrentBufferFill {

    private final Semaphore permits = new Semaphore(Integer.MAX_VALUE);

    private ByteBuffer buffer;

    /** Address of the buffer's writable region if it is native, otherwise {@code 0}, in which case slices are slices of {@link #buffer}. */
    private final long nativeAddress;

    private final int nativeLength;

    /** Claimed regions as start to end (exclusive); {@code null} unless asserting. */
    private final TreeMap<Integer, Integer> claimedRegions = Assertions.ENABLED ? new TreeMap<>() : null;

    /**
     * Must be called on the thread that owns {@code buffer}, which must also call {@link #complete}. Fills the bytes between the
     * buffer's position and its limit.
     */
    public ConcurrentBufferFill(ByteBuffer buffer) {
        this.buffer = buffer;
        if (buffer.isDirect() && buffer.isReadOnly() == false) {
            // The segment is created here, on the owner thread, and only its address is kept: the address is plain data that other
            // threads can use, while the segment's scope would reject them
            this.nativeAddress = MemorySegment.ofBuffer(buffer).address();
            this.nativeLength = buffer.remaining();
        } else {
            this.nativeAddress = 0L;
            this.nativeLength = 0;
        }
    }

    /**
     * Runs {@code filler} with a slice of the buffer to write to, holding off {@link #complete} meanwhile. The filler must not retain the
     * slice. The region must not overlap one claimed by an earlier call, which is checked when assertions are enabled.
     *
     * @param position start of the slice, relative to the buffer's position when this fill was created
     * @param length slice length
     * @param valueIfComplete returned, without running the filler, if {@link #complete} was already called; the read has been abandoned
     *                        by then, so for a filler that returns a byte count this should be the value that makes callers treat it as done
     * @return the filler's result, or {@code valueIfComplete}
     * @throws IndexOutOfBoundsException if the slice does not lie within the buffer
     */
    public <R, E extends Exception> R fillSlice(int position, int length, R valueIfComplete, CheckedFunction<ByteBuffer, R, E> filler)
        throws E {
        if (permits.tryAcquire() == false) {
            return valueIfComplete;
        }
        try {
            return filler.apply(slice(position, length));
        } finally {
            permits.release();
        }
    }

    private ByteBuffer slice(int position, int length) {
        final ByteBuffer slice;
        if (nativeAddress == 0L) {
            slice = buffer.slice(buffer.position() + position, length);
        } else {
            Objects.checkFromIndexSize(position, length, nativeLength);
            slice = SegmentBuffers.unscopedByteBuffer(nativeAddress + position, length);
        }
        assert claim(position, length);
        return slice;
    }

    /**
     * Ends the fill: waits, uninterruptibly, for fillers in progress, then advances the buffer's position. Later calls have no effect.
     *
     * @param bytesFilled bytes to advance the position by, at most the total length of the claimed regions. Zero if nothing was filled or
     *                    the caller accounts for it itself.
     */
    public void complete(int bytesFilled) {
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
}
