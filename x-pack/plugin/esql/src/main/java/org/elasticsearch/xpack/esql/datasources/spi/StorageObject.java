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

    /** Opens an input stream for sequential reading from the beginning. */
    InputStream newStream() throws IOException;

    /**
     * Opens an input stream for reading a specific byte range.
     * Critical for columnar formats like Parquet that read specific column chunks.
     * For reading object footers (e.g., Parquet), use: {@code newStream(length() - footerSize, footerSize)}
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

    // === ASYNC API (optional - default wraps sync) ===

    /**
     * Async byte read with ActionListener callback.
     * <p>
     * Default implementation wraps the sync {@link #newStream(long, long)} method in an executor.
     * Override this method for native async I/O (e.g., HTTP sendAsync, S3AsyncClient).
     * <p>
     * Columnar formats (Parquet) can use this for parallel chunk reads when
     * {@link #supportsNativeAsync()} returns true.
     *
     * @param position the starting byte position
     * @param length the number of bytes to read
     * @param executor executor for running the async operation
     * @param listener callback for the result or failure
     */
    default void readBytesAsync(long position, long length, Executor executor, ActionListener<ByteBuffer> listener) {
        executor.execute(() -> {
            try (InputStream stream = newStream(position, length)) {
                byte[] bytes = stream.readAllBytes();
                listener.onResponse(ByteBuffer.wrap(bytes));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
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
}
