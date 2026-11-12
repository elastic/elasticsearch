/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.common;

import org.elasticsearch.core.CheckedFunction;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link BufferFillScope} that checks how it is used, and otherwise defers to the scope it wraps. {@link BufferFillScope#of} only
 * creates it when assertions are enabled, so that the production scopes carry none of this state.
 *
 * <p>The checks that a scope's protocol is followed, which is what keeps fillers from writing to memory that the owner has freed or
 * reused:
 * <ul>
 * <li>the scope is created, and {@link #complete} is called, on the thread that owns the buffer, so the owner cannot free the buffer
 * before it returns;
 * <li>regions claimed by {@link #fillSlice} do not overlap, and {@link #complete} does not report more bytes than were claimed;
 * <li>no filler is running when {@link #complete} reports bytes as filled, nor holds a permit once the scope is complete;
 * <li>the memory of a confined arena is still allocated before and after a filler runs. That is not the case if the owner freed it
 * without completing the scope, which would otherwise be a silent write to freed memory.
 * </ul>
 */
final class AssertingBufferFillScope extends BufferFillScope {

    private final BufferFillScope delegate;

    /**
     * Lifetime of the memory of a direct buffer, which unlike the buffer or its segment any thread may ask about; {@code null} for a
     * buffer on the heap. It is what tells that a confined arena was closed, which nothing else on the address-backed path would.
     */
    private final MemorySegment.Scope memoryScope;

    /** The thread that created this scope, which must complete it. */
    private final Thread owner = Thread.currentThread();

    /** Claimed regions as start to end (exclusive). */
    private final TreeMap<Integer, Integer> claimedRegions = new TreeMap<>();

    /** Fillers that have started, and not yet finished, running. */
    private final AtomicInteger running = new AtomicInteger();

    /** Set by the owner once {@link #delegate} is complete, which is after every filler holding a permit has finished. */
    private volatile boolean completed;

    /**
     * Must be called on the thread that owns {@code buffer}, which is the thread that creates the scope that is wrapped.
     *
     * @param delegate the scope to check the use of
     * @param buffer the buffer that {@code delegate} fills
     */
    AssertingBufferFillScope(BufferFillScope delegate, ByteBuffer buffer) {
        this.delegate = delegate;
        if (buffer.isDirect()) {
            final MemorySegment segment = MemorySegment.ofBuffer(buffer);
            assert segment.isAccessibleBy(owner)
                : "the scope must be created on the thread that owns the buffer, but the memory is not accessible by [" + owner + "]";
            this.memoryScope = segment.scope();
        } else {
            this.memoryScope = null;
        }
    }

    @Override
    public <R, E extends Exception> Optional<R> fillSlice(int position, int length, CheckedFunction<ByteBuffer, R, E> filler) throws E {
        // The checks are made by the filler that the delegate runs, so they are made while holding a permit, which is when they hold
        return delegate.fillSlice(position, length, slice -> {
            running.incrementAndGet();
            try {
                assert completed == false : "a filler holds a permit but the scope is complete";
                assert memoryIsAllocated(true);
                assert claim(position, length);
                final R result = filler.apply(slice);
                assert memoryIsAllocated(false);
                return result;
            } finally {
                running.decrementAndGet();
            }
        });
    }

    @Override
    public void complete(int bytesFilled) {
        assert Thread.currentThread() == owner : "completed on [" + Thread.currentThread() + "] but created on [" + owner + "]";
        if (completed) {
            return;
        }
        assert bytesFilled == 0 || running.get() == 0
            : "Try to complete with [" + bytesFilled + "] bytes but [" + running.get() + "] fillers are running.";
        delegate.complete(bytesFilled);
        // From here on no filler is running, or will run, so the regions are final
        completed = true;
        assert bytesFilled <= claimedLength()
            : "completing with [" + bytesFilled + "] bytes filled but only [" + claimedLength() + "] claimed";
    }

    /**
     * Checks, for a filler holding a permit, that the memory it is about to write to, or has just written to, is still allocated. Holding
     * a permit means {@link #complete} has not returned, so the owner cannot have freed the memory unless it broke the protocol.
     *
     * @param beforeFill {@code true} when checked before the filler runs, so that it is never run against freed memory
     */
    private boolean memoryIsAllocated(boolean beforeFill) {
        assert memoryScope == null || memoryScope.isAlive()
            : (beforeFill ? "the arena was closed before a filler ran" : "the arena was closed while a filler was running")
                + ": complete must be called before the owner frees the memory";
        return true;
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
