/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Releasable;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Result of {@link StorageObject#readBytesAsync(long, long, DirectBufferFactory, java.util.concurrent.Executor,
 * org.elasticsearch.action.ActionListener)}: the bytes plus a {@link Releasable} that releases
 * whatever backed them (breaker charge for heap buffers, native memory for Arrow-backed ones).
 *
 * <p>The {@code buffer} is a {@link ByteBuffer} obtained from the caller-supplied
 * {@link DirectBufferFactory} (via {@link DirectBufferFactory#allocate(int)}). Production
 * factories ({@link DirectBufferFactory#forBreaker(CircuitBreaker)}) wrap a heap {@code byte[]};
 * {@link DirectBufferFactory#forAllocator(BufferAllocator)} wraps an {@link ArrowBuf}. The
 * buffer is not required to be direct. The caller must invoke {@link #close()} once the bytes
 * have been consumed.
 *
 * <p>Using {@link #buffer()} after {@link #close()} is undefined: a heap array may still be
 * reachable, but an Arrow-backed view reads dangling memory that may have been recycled.
 *
 * <h2>Use-after-free / double-free detection (assertions only)</h2>
 * <p>When the buffer is a detached {@link ByteBuffer} view of an {@link ArrowBuf} (the
 * {@link DirectBufferFactory#forAllocator(BufferAllocator)} path), reads through that view
 * <em>bypass Arrow's reference-count tracking entirely</em> — the Arrow debug allocator can
 * detect a double-free or a leak, but it is blind to a read that aliases this buffer after
 * {@link #close()} has freed it. That is the failure mode behind the nondeterministic zstd
 * {@code "Src size is incorrect"} / {@code "Destination buffer is too small"} corruption: a
 * slice handed out before close is read after the backing {@link ArrowBuf} was returned to the
 * allocator and recycled.
 *
 * <p>To make that class of bug deterministic and self-locating, when assertions are enabled this
 * type:
 * <ul>
 *   <li>captures the <b>allocation</b> stack trace at construction and the <b>free</b> stack trace
 *       at {@link #close()} ("who deallocated this"),</li>
 *   <li>throws on a second {@link #close()} (double-free) and on any {@link #buffer()} access after
 *       close (use-after-free), attaching both stack traces, and</li>
 *   <li><b>poisons</b> direct memory with a recognizable pattern immediately before releasing it,
 *       so any surviving alias that reads the freed region fails the same way on every run instead
 *       of occasionally seeing still-intact bytes. Heap buffers skip poisoning ({@link
 *       DirectMemoryDebug#poison(ByteBuffer)} is a no-op when {@code isDirect()} is false).</li>
 * </ul>
 * All of this compiles out (no allocation, no poisoning) when assertions are disabled, so the
 * production read path is unchanged.
 */
public final class DirectReadBuffer implements Releasable {

    private static final String STORAGE_READ_BREAKER_LABEL = "storage read buffer";

    private final ByteBuffer buffer;
    private final Releasable release;

    // Lifecycle tracking; gated by es.arrow.debug_buffers (defaults to -ea). All null/false when off.
    private final Throwable allocSite;
    private volatile boolean released;
    private volatile Throwable freeSite;

    public DirectReadBuffer(ByteBuffer buffer, Releasable release) {
        this.buffer = buffer;
        this.release = release;
        this.allocSite = DirectMemoryDebug.trackingEnabled() ? new Throwable("DirectReadBuffer allocated here") : null;
    }

    /**
     * Bridge used by {@link DirectBufferFactory#forBreaker(CircuitBreaker)}: allocates a heap
     * {@code byte[]} of {@code length} bytes, charges {@code breaker}, and wraps it as a
     * {@link DirectReadBuffer}. {@link #close()} releases the charge. Backends should call
     * {@link DirectBufferFactory#allocate(int)} instead of this method directly.
     *
     * <p>The returned buffer's contents are uninitialized; the caller is responsible for filling
     * {@link #buffer()} before delivering it downstream and for calling {@link #close()} once
     * consumption is complete (or on the failure path).
     *
     * <p>Breaker trips and {@link OutOfMemoryError} from the array allocation undo the charge
     * before propagating so callers can distinguish a circuit-breaker rejection (eligible for a
     * 429 response) from an I/O error.
     */
    public static DirectReadBuffer allocate(CircuitBreaker breaker, int length) {
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative, got: " + length);
        }
        breaker.addEstimateBytesAndMaybeBreak(length, STORAGE_READ_BREAKER_LABEL);
        final byte[] bytes;
        try {
            bytes = new byte[length];
        } catch (Throwable t) {
            breaker.addWithoutBreaking(-length);
            throw t;
        }
        AtomicBoolean chargeReleased = new AtomicBoolean();
        return new DirectReadBuffer(ByteBuffer.wrap(bytes), () -> {
            if (chargeReleased.compareAndSet(false, true)) {
                breaker.addWithoutBreaking(-length);
            }
        });
    }

    /**
     * Bridge used by {@link DirectBufferFactory#forAllocator(BufferAllocator)}: allocates an
     * {@link ArrowBuf} of {@code length} bytes from {@code allocator} and wraps it as a
     * {@link DirectReadBuffer}. Backends should call {@link DirectBufferFactory#allocate(int)}
     * instead of this method directly.
     *
     * <p>The returned buffer's contents are uninitialized; the caller is responsible for filling
     * {@link #buffer()} before delivering it downstream and for calling {@link #close()} once
     * consumption is complete (or on the failure path).
     *
     * <p>The intermediate {@link ArrowBuf} is released on every failure path. Allocator failures
     * (breaker trip, {@link OutOfMemoryError}, Arrow runtime exceptions) propagate as-is so callers
     * can distinguish a circuit-breaker rejection (eligible for a 429 response) from an I/O error.
     */
    public static DirectReadBuffer allocate(BufferAllocator allocator, int length) {
        ArrowBuf buf = null;
        boolean success = false;
        try {
            buf = allocator.buffer(length);
            DirectReadBuffer result = new DirectReadBuffer(buf.nioBuffer(0, length), buf::close);
            success = true;
            return result;
        } finally {
            if (success == false && buf != null) {
                buf.close();
            }
        }
    }

    /**
     * The bytes. Must not be accessed after {@link #close()}; doing so is undefined for Arrow-backed
     * buffers (freed, possibly recycled native memory). When {@code es.arrow.debug_buffers} is on,
     * a post-close access throws with the allocation and free stack traces attached.
     */
    public ByteBuffer buffer() {
        if (DirectMemoryDebug.trackingEnabled() && released) {
            throw useAfterFree();
        }
        return buffer;
    }

    /** The underlying {@link Releasable}. Prefer {@link #close()}, which adds lifecycle checks. */
    public Releasable release() {
        return release;
    }

    @Override
    public void close() {
        if (DirectMemoryDebug.trackingEnabled()) {
            // A second close would double-free an ArrowBuf or double-release a breaker charge.
            // Surface it here with both stacks rather than letting the backing store throw a
            // context-free exception.
            if (released) {
                throw doubleFree();
            }
            freeSite = new Throwable("DirectReadBuffer freed here");
            released = true;
        }
        // Poison before releasing (self-gated by es.arrow.* knobs; no-op for heap buffers) so any
        // surviving alias that reads this region after free fails deterministically instead of flakily.
        DirectMemoryDebug.poison(buffer);
        release.close();
    }

    private AssertionError useAfterFree() {
        AssertionError e = new AssertionError("DirectReadBuffer.buffer() accessed after close() (use-after-free)");
        if (allocSite != null) {
            e.addSuppressed(allocSite);
        }
        if (freeSite != null) {
            e.addSuppressed(freeSite);
        }
        return e;
    }

    private AssertionError doubleFree() {
        AssertionError e = new AssertionError("DirectReadBuffer.close() called twice (double-free)");
        if (allocSite != null) {
            e.addSuppressed(allocSite);
        }
        if (freeSite != null) {
            e.addSuppressed(freeSite);
        }
        return e;
    }
}
