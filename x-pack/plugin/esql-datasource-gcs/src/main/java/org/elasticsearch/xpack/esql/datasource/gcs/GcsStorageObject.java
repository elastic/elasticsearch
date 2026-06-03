/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gcs;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractMeteredStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.time.Instant;
import java.util.concurrent.Executor;

/**
 * StorageObject implementation for Google Cloud Storage.
 * Supports full and range reads, metadata retrieval with caching, and efficient positional
 * byte reads via {@link ReadChannel#read(ByteBuffer)}.
 * <p>
 * In addition to the required stream-based API, this class overrides:
 * <ul>
 *   <li>{@link #readBytes(long, ByteBuffer)} — uses {@code ReadChannel.read(ByteBuffer)} for
 *       direct buffer reads without intermediate byte[] allocation.</li>
 *   <li>{@link #readBytesAsync(long, long, DirectBufferFactory, Executor, ActionListener)} — executor-wrapped
 *       ReadChannel reads for the async API.</li>
 *   <li>{@link #supportsNativeAsync()} — returns {@code true} because this class provides custom
 *       async and byte-read implementations that are more efficient than the default InputStream
 *       wrappers. Note: the async path is executor-based (blocking a worker thread), not truly
 *       non-blocking like {@code HttpClient.sendAsync()} or {@code S3AsyncClient}.</li>
 * </ul>
 */
public final class GcsStorageObject extends AbstractMeteredStorageObject {
    private final Storage storage;
    private final String bucket;
    private final String objectName;
    private final StoragePath path;

    private Long cachedLength;
    private Instant cachedLastModified;
    private Boolean cachedExists;

    // TODO: GCS retries are managed inside RetryHelper at the Storage client layer; intercepting
    // them here would require wrapping the Storage instance. Not counted in this PR.

    public GcsStorageObject(Storage storage, String bucket, String objectName, StoragePath path) {
        if (storage == null) {
            throw new IllegalArgumentException("storage cannot be null");
        }
        if (bucket == null || bucket.isEmpty()) {
            throw new IllegalArgumentException("bucket cannot be null or empty");
        }
        if (objectName == null) {
            throw new IllegalArgumentException("objectName cannot be null");
        }
        if (path == null) {
            throw new IllegalArgumentException("path cannot be null");
        }
        this.storage = storage;
        this.bucket = bucket;
        this.objectName = objectName;
        this.path = path;
    }

    public GcsStorageObject(Storage storage, String bucket, String objectName, StoragePath path, long length) {
        this(storage, bucket, objectName, path);
        this.cachedLength = length;
    }

    public GcsStorageObject(Storage storage, String bucket, String objectName, StoragePath path, long length, Instant lastModified) {
        this(storage, bucket, objectName, path, length);
        this.cachedLastModified = lastModified;
    }

    @Override
    public InputStream newStream() throws IOException {
        long startNanos = System.nanoTime();
        long bytes = 0L;
        try {
            BlobId blobId = BlobId.of(bucket, objectName);
            ReadChannel reader = storage.reader(blobId);
            // GCS ReadChannel does not expose content length on open; fall back to cached size if known.
            if (cachedLength != null) {
                bytes = cachedLength;
            }
            return new GcsTransientTypingInputStream(Channels.newInputStream(reader), path);
        } catch (StorageException e) {
            throw throwReadFailure("Failed to read object from", e);
        } finally {
            counters.addRequest(System.nanoTime() - startNanos, bytes);
        }
    }

    @Override
    public InputStream newStream(long position, long length) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException("position must be non-negative, got: " + position);
        }
        boolean toEnd = length == READ_TO_END;
        if (toEnd == false && length <= 0) {
            throw new IllegalArgumentException("length must be positive or READ_TO_END, got: " + length);
        }

        long startNanos = System.nanoTime();
        try {
            BlobId blobId = BlobId.of(bucket, objectName);
            ReadChannel reader = storage.reader(blobId);
            reader.seek(position);
            // READ_TO_END: seek to position and read to the end of the object (no limit) — no length() lookup.
            if (toEnd == false) {
                reader.limit(position + length);
            }
            return new GcsTransientTypingInputStream(Channels.newInputStream(reader), path);
        } catch (StorageException e) {
            throw throwReadFailure("Range request failed for", e);
        } finally {
            counters.addRequest(System.nanoTime() - startNanos, toEnd ? 0L : length);
        }
    }

    @Override
    public long length() throws IOException {
        if (cachedLength == null) {
            fetchMetadata();
        }
        if (cachedExists != null && cachedExists == false) {
            throw new IOException("Object not found: " + path);
        }
        return cachedLength;
    }

    @Override
    public Instant lastModified() throws IOException {
        if (cachedLastModified == null) {
            fetchMetadata();
        }
        return cachedLastModified;
    }

    @Override
    public boolean exists() throws IOException {
        if (cachedExists == null) {
            fetchMetadata();
        }
        return cachedExists;
    }

    @Override
    public StoragePath path() {
        return path;
    }

    @Override
    public int readBytes(long position, ByteBuffer target) throws IOException {
        if (target.hasRemaining() == false) {
            return 0;
        }
        long startNanos = System.nanoTime();
        int totalRead = 0;
        try {
            BlobId blobId = BlobId.of(bucket, objectName);
            try (ReadChannel reader = storage.reader(blobId)) {
                reader.seek(position);
                reader.limit(position + target.remaining());
                while (target.hasRemaining()) {
                    int n = readFromChannel(reader, target);
                    if (n < 0) {
                        break;
                    }
                    totalRead += n;
                }
                return totalRead == 0 ? -1 : totalRead;
            }
        } catch (StorageException e) {
            throw throwReadFailure("Failed to read bytes from", e);
        } finally {
            counters.addRequest(System.nanoTime() - startNanos, totalRead);
        }
    }

    @Override
    public void readBytesAsync(
        long position,
        long length,
        DirectBufferFactory factory,
        Executor executor,
        ActionListener<DirectReadBuffer> listener
    ) {
        if (position < 0) {
            listener.onFailure(new IllegalArgumentException("position must be non-negative, got: " + position));
            return;
        }
        if (length < 0) {
            listener.onFailure(new IllegalArgumentException("length must be non-negative, got: " + length));
            return;
        }
        if (length > Integer.MAX_VALUE) {
            listener.onFailure(new IllegalArgumentException("length must fit in an int for async reads, got: " + length));
            return;
        }

        // Allocate up front so the breaker decision and any OOM are surfaced synchronously via
        // the listener instead of escaping the executor's Runnable as an Error.
        int len = Math.toIntExact(length);
        final DirectReadBuffer drb;
        try {
            drb = factory.allocate(len);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        ByteBuffer buffer = drb.buffer();

        try {
            executor.execute(() -> {
                long startNanos = System.nanoTime();
                int payloadBytes = 0;
                try {
                    BlobId blobId = BlobId.of(bucket, objectName);
                    try (ReadChannel reader = storage.reader(blobId)) {
                        reader.seek(position);
                        reader.limit(position + length);
                        while (buffer.hasRemaining()) {
                            int n = readFromChannel(reader, buffer);
                            if (n < 0) {
                                break;
                            }
                        }
                        buffer.flip();
                        payloadBytes = buffer.remaining();
                    }
                } catch (StorageException e) {
                    counters.addRequest(System.nanoTime() - startNanos, 0L);
                    drb.close();
                    listener.onFailure(mapReadFailure("Failed to read bytes from", e));
                    return;
                } catch (Exception e) {
                    counters.addRequest(System.nanoTime() - startNanos, 0L);
                    drb.close();
                    listener.onFailure(e);
                    return;
                }
                // I/O succeeded; deliver outside the I/O catch blocks so a throw from
                // onResponse does not double-close drb or invoke listener.onFailure.
                counters.addRequest(System.nanoTime() - startNanos, payloadBytes);
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
        } catch (RuntimeException e) {
            // Executor rejection (saturated queue, shutdown) — release the buffer eagerly so the
            // charge does not stay against the allocator for the lifetime of the JVM.
            drb.close();
            listener.onFailure(e);
        }
    }

    @Override
    public boolean supportsNativeAsync() {
        return true;
    }

    @SuppressForbidden(reason = "GCS ReadChannel is not a FileChannel; Channels.* helpers do not apply")
    private static int readFromChannel(ReadChannel reader, ByteBuffer target) throws IOException {
        return reader.read(target);
    }

    /**
     * Maps a failure from the GCS client into the exception to surface to ES|QL. A retryable transport
     * status (5xx/429) becomes an {@link ExternalUnavailableException} (503 — the read may succeed on
     * retry, with the throttle flag set for 429/503); a missing object or any other failure becomes an
     * {@link IOException}, which the external source operator classifies as a client-class 400. Returns
     * (never throws) so both the synchronous and async read paths can route it.
     */
    private Exception mapReadFailure(String context, Throwable cause) {
        if (cause instanceof StorageException se) {
            if (ExternalUnavailableException.isRetryableStatus(se.getCode())) {
                boolean throttling = ExternalUnavailableException.isThrottlingStatus(se.getCode());
                return new ExternalUnavailableException(
                    throttling,
                    cause,
                    "GCS store unavailable reading [{}] (HTTP {})",
                    path,
                    se.getCode()
                );
            }
            if (se.getCode() == 404) {
                return new IOException("Object not found: " + path, cause);
            }
        }
        return new IOException(context + " " + path, cause);
    }

    /**
     * Synchronous-path bridge for {@link #mapReadFailure}: rethrows the mapped exception. The return
     * type lets callers write {@code throw throwReadFailure(...)} so the compiler sees an exit.
     */
    private RuntimeException throwReadFailure(String context, Throwable cause) throws IOException {
        Exception mapped = mapReadFailure(context, cause);
        if (mapped instanceof RuntimeException re) {
            throw re;
        }
        throw (IOException) mapped;
    }

    private void fetchMetadata() throws IOException {
        try {
            Blob blob = storage.get(BlobId.of(bucket, objectName));
            if (blob != null) {
                cachedExists = true;
                cachedLength = blob.getSize();
                if (blob.getUpdateTimeOffsetDateTime() != null) {
                    cachedLastModified = blob.getUpdateTimeOffsetDateTime().toInstant();
                }
            } else {
                setNotFound();
            }
        } catch (StorageException e) {
            if (e.getCode() == 404) {
                setNotFound();
            } else if (e.getCode() == 403) {
                fetchMetadataViaRangeRead();
            } else {
                throw new IOException("Failed to get metadata for " + path, e);
            }
        }
    }

    private void fetchMetadataViaRangeRead() throws IOException {
        boolean objectExists;
        try (ReadChannel reader = storage.reader(BlobId.of(bucket, objectName))) {
            reader.limit(1);
            try (InputStream is = Channels.newInputStream(reader)) {
                objectExists = is.read() >= 0;
            }
        } catch (Exception e) {
            if (e instanceof StorageException se && se.getCode() == 404) {
                setNotFound();
                return;
            }
            throw new IOException("Failed to get metadata for " + path + " (metadata denied, range read also failed)", e);
        }

        if (objectExists) {
            cachedExists = true;
            // GCS ReadChannel does not expose Content-Range; length cannot be determined
            // from a range read. The caller must know the length from listing (glob expansion).
            if (cachedLength == null) {
                throw new IOException(
                    "Failed to determine object size for "
                        + path
                        + ": GCS metadata access denied and object size cannot be determined from a range read. "
                        + "Use glob patterns (which include size from listing) instead of direct file paths."
                );
            }
        } else {
            setNotFound();
        }
    }

    private void setNotFound() {
        cachedExists = false;
        cachedLength = 0L;
        cachedLastModified = null;
    }

    String bucket() {
        return bucket;
    }

    String objectName() {
        return objectName;
    }

    @Override
    public String toString() {
        return "GcsStorageObject{bucket=" + bucket + ", objectName=" + objectName + ", path=" + path + "}";
    }
}
