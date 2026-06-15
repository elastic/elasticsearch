/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.action.ActionListener;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * Unified interface for storage object access.
 * <p>
 * Simple providers: implement sync methods - async wrapping is automatic.
 * Async-capable providers (HTTP, S3): override async methods for native non-blocking I/O.
 * <p>
 * Provides metadata access and methods to open streams for reading.
 * Uses standard Java InputStream for compatibility with existing Elasticsearch code.
 * Random access is handled via range-based reads (like BlobContainer pattern).
 * <p>
 * StorageObject instances themselves do not hold open resources and do not need closing.
 * The {@link java.io.InputStream} instances returned by {@link #newStream()} and
 * {@link #newStream(long, long)} are the resources that callers must close.
 */
public interface StorageObject {

    /**
     * Transfer buffer size used by the default {@link #readBytes(long, ByteBuffer)} when
     * reading into a direct ByteBuffer via an InputStream. Kept small to avoid large
     * stack allocations while still being efficient for typical I/O page sizes.
     */
    int TRANSFER_BUFFER_SIZE = 8192;

    // === SYNC API (required) ===

    /**
     * Sentinel {@code length} for {@link #newStream(long, long)} meaning "read from {@code position} to the end
     * of the object" — the open-ended form. It exists because the total length is not always knowable up front
     * (e.g. a compressed object has no addressable length), so the open-ended read must signal "to the end" with
     * this marker rather than by computing {@code length() - position}.
     */
    long READ_TO_END = -1L;

    /**
     * Opens an input stream for sequential reading of the whole object, from the beginning to the end.
     * <p>
     * The default is {@code newStream(0, READ_TO_END)}, so the whole-object read shares the open-ended code path
     * (and, through the decorator chain, its resilience). A provider may override this for a plain whole-object GET.
     */
    default InputStream newStream() throws IOException {
        return newStream(0, READ_TO_END);
    }

    /**
     * Opens an input stream for reading a byte range. A {@code length} of {@link #READ_TO_END} reads from
     * {@code position} to the end of the object (the open-ended form) — essential for streams whose total length
     * is not available. Providers MUST translate {@code READ_TO_END} into a native open-ended read (e.g. HTTP
     * {@code Range: bytes=position-}) and MUST NOT require {@link #length()} to serve it; an empty object (or
     * {@code position} at/after the end) yields an empty stream.
     * <p>
     * Critical for columnar formats like Parquet that read specific column chunks. For reading object footers,
     * use a bounded length: {@code newStream(length() - footerSize, footerSize)}.
     */
    InputStream newStream(long position, long length) throws IOException;

    /** Returns the object size in bytes. */
    long length() throws IOException;

    /** Returns the last modification time, or null if not available. */
    Instant lastModified() throws IOException;

    /** Checks if the object exists. */
    boolean exists() throws IOException;

    /** Returns the path of this object. */
    StoragePath path();

    /**
     * Closes a stream opened by this object, discarding any unread bytes without blocking.
     * <p>
     * The default implementation calls {@link InputStream#close()}, which is correct for local
     * files and most providers (they simply close the connection). Override this for providers
     * whose {@code close()} drains remaining bytes to reuse the connection pool — where closing
     * a partially-read stream would block for the full remaining object transfer time.
     * <p>
     * Use this instead of closing directly when the caller intentionally reads only a prefix of
     * the stream (e.g., schema detection), and connection reuse is not required.
     * <p>
     * <b>Contract:</b> {@code stream} must be the exact {@link InputStream} instance returned by
     * {@link #newStream()} or {@link #newStream(long, long)} on this object — not a wrapper
     * around it. Passing a wrapped stream (e.g. a {@link java.io.BufferedInputStream} layered on
     * top) causes provider-specific overrides (e.g. the S3 {@code Abortable} cast) to fall back
     * to a draining {@code close()}, silently defeating the abort.
     */
    default void abortStream(InputStream stream) throws IOException {
        stream.close();
    }

    // === ASYNC API (optional - default wraps sync) ===

    /**
     * Async byte read with ActionListener callback.
     * <p>
     * Default implementation wraps the sync {@link #readBytes(long, ByteBuffer)} method in an executor.
     * Override this method for native async I/O (e.g., HTTP sendAsync, S3AsyncClient).
     * <p>
     * Columnar formats (Parquet) can use this for parallel chunk reads when
     * {@link #supportsNativeAsync()} returns true.
     * <p>
     * <b>Returned buffer contract:</b> the {@link DirectReadBuffer#buffer()} delivered to the
     * listener has {@code capacity() == length} (the requested length) and {@code remaining()}
     * equal to the number of bytes actually read. On a short read these differ — consumers must
     * use {@code remaining()} (or {@code limit() - position()}) to size their work, never
     * {@code capacity()}. The buffer is direct.
     * <p>
     * On end-of-content at {@code position} the buffer is delivered with {@code remaining() == 0}.
     *
     * <p>
     * <b>Buffer ownership:</b> the storage object obtains exactly one {@link DirectReadBuffer} of
     * {@code length} bytes from {@code factory}. The caller must invoke {@link DirectReadBuffer#close()}
     * once the bytes have been consumed; closing releases the buffer back to its underlying
     * allocator. See {@link DirectReadBuffer} for the contract.
     *
     * <p>
     * <b>Implementation contract:</b> if the read fails, implementations must close the
     * {@link DirectReadBuffer} before calling {@code listener.onFailure()}. Callers that fan out
     * reads across multiple merged ranges rely on this invariant to avoid double-releasing buffers
     * from the successfully-completed sibling ranges on the failure path.
     *
     * @param position the starting byte position
     * @param length the number of bytes to read
     * @param factory produces the {@link DirectReadBuffer} the bytes are read into; the storage
     *            object calls {@link DirectBufferFactory#allocate(int)} exactly once with
     *            {@code length}
     * @param executor executor for running the async operation
     * @param listener callback for the result or failure
     */
    default void readBytesAsync(
        long position,
        long length,
        DirectBufferFactory factory,
        Executor executor,
        ActionListener<DirectReadBuffer> listener
    ) {
        if (length < 0) {
            listener.onFailure(new IllegalArgumentException("length must be non-negative, got: " + length));
            return;
        }
        if (length > Integer.MAX_VALUE) {
            listener.onFailure(new IllegalArgumentException("length must fit in an int for async reads, got: " + length));
            return;
        }
        // Allocate on the calling thread so a direct-memory OOM (or breaker trip) surfaces
        // synchronously via the listener instead of escaping the executor's Runnable as an Error
        // and leaving the listener permanently uncompleted.
        final DirectReadBuffer drb;
        boolean submitted = false;
        try {
            drb = factory.allocate((int) length);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        try {
            executor.execute(() -> {
                try {
                    int read = Math.max(0, readBytes(position, drb.buffer()));
                    drb.buffer().position(0).limit(read);
                } catch (Exception e) {
                    drb.close();
                    listener.onFailure(e);
                    return;
                }
                // Deliver outside the I/O catch so a throw from onResponse does not
                // double-close drb or invoke listener.onFailure after listener.onResponse.
                try {
                    listener.onResponse(drb);
                } catch (Exception e) {
                    try {
                        drb.close();
                    } catch (Exception closeEx) {
                        e.addSuppressed(closeEx);
                    }
                    throw e;
                }
            });
            submitted = true;
        } finally {
            if (submitted == false) {
                // Executor rejected (saturated queue, shutdown) — release the buffer eagerly so it
                // does not stay charged against the breaker for the lifetime of the JVM.
                drb.close();
            }
        }
    }

    /**
     * Async byte read into a caller-provided ByteBuffer.
     * <p>
     * Avoids per-call allocation by reading directly into the target buffer.
     * For heap-backed buffers, reads directly into the backing array.
     * For direct buffers, falls back to allocating a temporary array.
     * <p>
     * The buffer's position is advanced by the number of bytes read.
     * The listener receives the number of bytes actually read.
     *
     * @param position the starting byte position in the storage object
     * @param target the ByteBuffer to read into; bytes are written starting at {@code target.position()}
     * @param executor executor for running the async operation
     * @param listener callback with the number of bytes read, or failure
     */
    default void readBytesAsync(long position, ByteBuffer target, Executor executor, ActionListener<Integer> listener) {
        try {
            executor.execute(() -> {
                int toRead = target.remaining();
                try (InputStream stream = newStream(position, toRead)) {
                    if (target.hasArray()) {
                        int totalRead = 0;
                        int off = target.arrayOffset() + target.position();
                        while (totalRead < toRead) {
                            int n = stream.read(target.array(), off + totalRead, toRead - totalRead);
                            if (n < 0) {
                                break;
                            }
                            totalRead += n;
                        }
                        target.position(target.position() + totalRead);
                        listener.onResponse(totalRead);
                    } else {
                        byte[] bytes = stream.readAllBytes();
                        target.put(bytes);
                        listener.onResponse(bytes.length);
                    }
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            });
        } catch (RejectedExecutionException e) {
            listener.onFailure(e);
        }
    }

    // === POSITIONAL BYTE-BUFFER API (optional - enables zero-copy for columnar formats) ===

    /**
     * Reads bytes from a specific position directly into a {@link ByteBuffer}.
     * <p>
     * This is a positional, stateless read: each call specifies the position explicitly,
     * so no mutable cursor state is needed. Providers override this to enable zero-copy I/O:
     * <ul>
     *   <li>Local files: {@code FileChannel.read(target, position)} reads from OS page cache
     *       directly into both heap and direct buffers with no intermediate copies.</li>
     *   <li>GCS: {@code ReadChannel.read(target)} reads natively into the ByteBuffer.</li>
     *   <li>S3/HTTP/Azure: The default stream-based implementation is used; for heap-backed
     *       buffers it reads directly into the backing array, for direct buffers it uses
     *       a small chunked transfer buffer (8 KB) instead of allocating a full-size temporary array.</li>
     * </ul>
     * <p>
     * The buffer's position is advanced by the number of bytes read.
     *
     * @param position the starting byte position in the storage object
     * @param target the ByteBuffer to read into; bytes are written starting at {@code target.position()}
     * @return the number of bytes actually read, or -1 if the position is at or past end of content
     * @throws IOException if an I/O error occurs
     */
    default int readBytes(long position, ByteBuffer target) throws IOException {
        if (target.hasRemaining() == false) {
            return 0;
        }
        try (InputStream stream = newStream(position, target.remaining())) {
            if (target.hasArray()) {
                int totalRead = 0;
                int off = target.arrayOffset() + target.position();
                int toRead = target.remaining();
                while (totalRead < toRead) {
                    int n = stream.read(target.array(), off + totalRead, toRead - totalRead);
                    if (n < 0) {
                        break;
                    }
                    totalRead += n;
                }
                if (totalRead == 0) {
                    return -1;
                }
                target.position(target.position() + totalRead);
                return totalRead;
            } else {
                byte[] buf = new byte[Math.min(target.remaining(), TRANSFER_BUFFER_SIZE)];
                int totalRead = 0;
                while (target.hasRemaining()) {
                    int toRead = Math.min(buf.length, target.remaining());
                    int n = stream.read(buf, 0, toRead);
                    if (n < 0) {
                        break;
                    }
                    target.put(buf, 0, n);
                    totalRead += n;
                }
                return totalRead == 0 ? -1 : totalRead;
            }
        }
    }

    /**
     * Returns true if this object has native async support.
     * <p>
     * Columnar formats (Parquet) can use this to determine whether to use
     * {@link #readBytesAsync} for parallel chunk reads instead of sequential
     * stream-based reads.
     *
     * @return true if {@link #readBytesAsync} has a native implementation, false if it uses the default sync wrapper
     */
    default boolean supportsNativeAsync() {
        return false;
    }

    // === METRICS API (optional - default returns the zero-valued snapshot) ===

    /**
     * Returns cumulative I/O counters for reads against this object.
     * <p>
     * Implementations that don't track I/O return {@link StorageObjectMetrics#ZERO}.
     * Decorator wrappers must delegate to the wrapped object so counters are
     * attributed to the underlying store, not the wrapper layer.
     */
    default StorageObjectMetrics metrics() {
        return StorageObjectMetrics.ZERO;
    }
}
