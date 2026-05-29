/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Releasable;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Result of {@link StorageObject#readBytesAsync(long, long, DirectBufferFactory, java.util.concurrent.Executor,
 * org.elasticsearch.action.ActionListener)}: the bytes plus a {@link Releasable} that frees the
 * native memory backing them.
 *
 * <p>The {@code buffer} is a direct {@link ByteBuffer} view of an {@link ArrowBuf} obtained from
 * the caller-supplied {@link DirectBufferFactory} (via {@link DirectBufferFactory#allocate(int)}).
 * The caller must invoke {@link #close()} once the bytes have been consumed: closing decrements
 * the {@code ArrowBuf}'s reference count, which is what actually returns the memory to the
 * underlying allocator. Closing the allocator alone is not enough — Arrow's
 * {@code BaseAllocator.close()} treats outstanding {@code ArrowBuf}s as a leak (see
 * esql-planning#851).
 *
 * <p>Using {@link #buffer()} after {@link #close()} reads dangling memory; the underlying chunk
 * may have been recycled into another allocation.
 *
 * <h2>Use-after-free / double-free detection (assertions only)</h2>
 * <p>The bytes are exposed as a detached {@link ByteBuffer} (via {@code ArrowBuf.nioBuffer(...)}),
 * so reads through that view <em>bypass Arrow's reference-count tracking entirely</em> — the Arrow
 * debug allocator can detect a double-free or a leak, but it is blind to a read that aliases this
 * buffer after {@link #close()} has freed it. That is the failure mode behind the nondeterministic
 * zstd {@code "Src size is incorrect"} / {@code "Destination buffer is too small"} corruption: a
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
 *   <li><b>poisons</b> the direct memory with a recognizable pattern immediately before releasing it,
 *       so any surviving alias that reads the freed region fails the same way on every run instead
 *       of occasionally seeing still-intact bytes.</li>
 * </ul>
 * All of this compiles out (no allocation, no poisoning) when assertions are disabled, so the
 * production read path is unchanged.
 */
public final class DirectReadBuffer implements Releasable {

    private final ByteBuffer buffer;
    private final Releasable release;

    // Assertions-only lifecycle tracking; all null/false when -ea is off.
    private final Throwable allocSite;
    private volatile boolean released;
    private volatile Throwable freeSite;

    public DirectReadBuffer(ByteBuffer buffer, Releasable release) {
        this.buffer = buffer;
        this.release = release;
        this.allocSite = Assertions.ENABLED ? new Throwable("DirectReadBuffer allocated here") : null;
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
     * <p>Translates allocator failures (breaker trip, {@link OutOfMemoryError}, any other Arrow
     * runtime exception) into a single {@link IOException} so backend implementations can use a
     * uniform error path. The intermediate {@link ArrowBuf} is released on every failure path.
     */
    public static DirectReadBuffer allocate(BufferAllocator allocator, int length) throws IOException {
        ArrowBuf buf = null;
        try {
            buf = allocator.buffer(length);
            return new DirectReadBuffer(buf.nioBuffer(0, length), buf::close);
        } catch (OutOfMemoryError | RuntimeException e) {
            if (buf != null) {
                buf.close();
            }
            throw new IOException("failed to allocate " + length + " bytes from allocator " + allocator.getName(), e);
        }
    }

    /**
     * The bytes. Must not be accessed after {@link #close()}; doing so reads freed (and possibly
     * recycled) native memory. When assertions are enabled, a post-close access throws with the
     * allocation and free stack traces attached.
     */
    public ByteBuffer buffer() {
        assert assertLive("DirectReadBuffer.buffer() accessed after close() (use-after-free)");
        return buffer;
    }

    /** The underlying {@link Releasable}. Prefer {@link #close()}, which adds lifecycle checks. */
    public Releasable release() {
        return release;
    }

    @Override
    public void close() {
        if (Assertions.ENABLED) {
            // A second close would double-free the ArrowBuf. Surface it here with both stacks
            // rather than letting Arrow throw a context-free IllegalReferenceCountException.
            assert released == false : doubleFree();
            freeSite = new Throwable("DirectReadBuffer freed here");
            released = true;
            DirectMemoryDebug.poison(buffer);
        }
        release.close();
    }

    private boolean assertLive(String message) {
        if (released) {
            AssertionError e = new AssertionError(message);
            if (allocSite != null) {
                e.addSuppressed(allocSite);
            }
            if (freeSite != null) {
                e.addSuppressed(freeSite);
            }
            throw e;
        }
        return true;
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
