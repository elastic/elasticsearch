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
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.UninitializedArrays;
import org.elasticsearch.core.Releasable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
 * <p>{@link #close()} drops this owner's reference to the backing buffer. Any aliases obtained
 * from {@link #buffer()} before close remain the consumer's responsibility and are not tracked by
 * this owner. Calling {@link #buffer()} after close always throws {@link IllegalStateException}.
 *
 * <h2>Use-after-free / double-free detection</h2>
 * <p>When the buffer is a detached {@link ByteBuffer} view of an {@link ArrowBuf} (the
 * {@link DirectBufferFactory#forAllocator(BufferAllocator)} path), reads through that view
 * <em>bypass Arrow's reference-count tracking entirely</em> — the Arrow debug allocator can
 * detect a double-free or a leak, but it is blind to a read that aliases this buffer after
 * {@link #close()} has freed it. That is the failure mode behind the nondeterministic zstd
 * {@code "Src size is incorrect"} / {@code "Destination buffer is too small"} corruption: a
 * slice handed out before close is read after the backing {@link ArrowBuf} was returned to the
 * allocator and recycled.
 *
 * <p>Detachment and rejection of post-close {@link #buffer()} access are production behavior.
 * When debug tracking is on (default under {@code -ea}) this type additionally:
 * <ul>
 *   <li>captures the <b>allocation</b> stack trace at construction and the <b>free</b> stack trace
 *       at {@link #close()} ("who deallocated this") and attaches both to the post-close
 *       {@link IllegalStateException},</li>
 *   <li>throws {@link AssertionError} on a second {@link #close()} (double-free), and</li>
 *   <li><b>poisons</b> direct memory with a recognizable pattern immediately before releasing it,
 *       so any surviving alias that reads the freed region fails the same way on every run instead
 *       of occasionally seeing still-intact bytes. Heap buffers skip poisoning ({@link
 *       DirectMemoryDebug#poison(ByteBuffer)} is a no-op when {@code isDirect()} is false).</li>
 * </ul>
 */
public final class DirectReadBuffer implements Releasable {

    private static final String STORAGE_READ_BREAKER_LABEL = "storage read buffer";

    private final AtomicReference<ByteBuffer> buffer;
    private final Releasable onClose;

    // Lifecycle tracking; gated by es.arrow.debug_buffers (defaults to -ea). Both are null when off.
    private final Throwable allocSite;
    private volatile Throwable freeSite;

    public DirectReadBuffer(ByteBuffer buffer, Releasable onClose) {
        this.buffer = new AtomicReference<>(buffer);
        this.onClose = onClose;
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
     *
     * <p>Charges go through {@link LocalCircuitBreaker#forAsyncIo(CircuitBreaker)}: a
     * driver-local breaker is not safe to touch from HTTP/S3 completion threads.
     */
    public static DirectReadBuffer allocate(CircuitBreaker breaker, int length) {
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative, got: " + length);
        }
        CircuitBreaker ioBreaker = LocalCircuitBreaker.forAsyncIo(breaker);
        ioBreaker.addEstimateBytesAndMaybeBreak(length, STORAGE_READ_BREAKER_LABEL);
        final byte[] bytes;
        try {
            bytes = UninitializedArrays.newByteArray(length);
        } catch (Throwable t) {
            ioBreaker.addWithoutBreaking(-length);
            throw t;
        }
        AtomicBoolean chargeReleased = new AtomicBoolean();
        return new DirectReadBuffer(ByteBuffer.wrap(bytes), () -> {
            if (chargeReleased.compareAndSet(false, true)) {
                ioBreaker.addWithoutBreaking(-length);
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
     * Constrains this owner's buffer to a writable window of {@code length} bytes starting at
     * position 0. {@link DirectBufferFactory#allocate(int)} may return a larger or oddly-limited
     * buffer; callers that fill by {@code remaining()} must invoke this before I/O so they cannot
     * read past the requested length.
     *
     * <p>Deliberately not public: this is an allocation-time normalization for
     * {@link DirectBufferFactory#allocateWritableWindow(int)}. Applying it to a buffer that has
     * already been filled would widen the window back to the requested length and expose the
     * uninitialized tail of a short read.
     *
     * @throws IOException if the buffer is read-only or {@code capacity() < length}
     */
    void requireWritableWindow(int length) throws IOException {
        ByteBuffer destination = buffer();
        if (destination.isReadOnly()) {
            throw new IOException("DirectBufferFactory returned a read-only buffer for requested length [" + length + "]");
        }
        if (destination.capacity() < length) {
            throw new IOException(
                "DirectBufferFactory returned buffer capacity [" + destination.capacity() + "] for requested length [" + length + "]"
            );
        }
        // Copies and channel reads use remaining() / relative puts, so restore the complete
        // requested window even if a custom factory returned a buffer with a smaller limit.
        destination.position(0).limit(length);
    }

    /**
     * The bytes. Must not be accessed after {@link #close()}. A post-close access throws an
     * {@link IllegalStateException}. When {@code es.arrow.debug_buffers} is on, the allocation
     * and free stack traces are attached as suppressed exceptions.
     */
    public ByteBuffer buffer() {
        ByteBuffer current = buffer.get();
        if (current == null) {
            throw useAfterFree();
        }
        return current;
    }

    @Override
    public void close() {
        ByteBuffer toRelease = buffer.getAndSet(null);
        if (toRelease == null) {
            // A second close would double-free an ArrowBuf or double-release a breaker charge.
            // Surface it here with both stacks rather than letting the backing store throw a
            // context-free exception.
            if (DirectMemoryDebug.trackingEnabled()) {
                throw doubleFree();
            }
            return;
        }
        if (DirectMemoryDebug.trackingEnabled()) {
            freeSite = new Throwable("DirectReadBuffer freed here");
        }
        try {
            // Poison before releasing (self-gated by es.arrow.* knobs; no-op for heap buffers) so any
            // surviving alias that reads this region after free fails deterministically instead of flakily.
            DirectMemoryDebug.poison(toRelease);
        } finally {
            // Unconditional: a buffer this owner cannot poison is still a charge it must refund.
            onClose.close();
        }
    }

    private IllegalStateException useAfterFree() {
        IllegalStateException e = new IllegalStateException("DirectReadBuffer buffer was closed");
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
