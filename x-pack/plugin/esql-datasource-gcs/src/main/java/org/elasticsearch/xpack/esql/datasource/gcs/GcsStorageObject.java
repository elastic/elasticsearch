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
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
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
 *   <li>{@link #readBytesAsync(long, long, Executor, ActionListener)} — executor-wrapped
 *       ReadChannel reads for the async API.</li>
 *   <li>{@link #supportsNativeAsync()} — returns {@code true} because this class provides custom
 *       async and byte-read implementations that are more efficient than the default InputStream
 *       wrappers. Note: the async path is executor-based (blocking a worker thread), not truly
 *       non-blocking like {@code HttpClient.sendAsync()} or {@code S3AsyncClient}.</li>
 * </ul>
 */
public final class GcsStorageObject implements StorageObject {
    private final Storage storage;
    private final String bucket;
    private final String objectName;
    private final StoragePath path;

    private Long cachedLength;
    private Instant cachedLastModified;
    private Boolean cachedExists;

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
        try {
            BlobId blobId = BlobId.of(bucket, objectName);
            ReadChannel reader = storage.reader(blobId);
            return Channels.newInputStream(reader);
        } catch (StorageException e) {
            throw wrapException(e, "Failed to read object from");
        }
    }

    @Override
    public InputStream newStream(long position, long length) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException("position must be non-negative, got: " + position);
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative, got: " + length);
        }

        try {
            BlobId blobId = BlobId.of(bucket, objectName);
            ReadChannel reader = storage.reader(blobId);
            reader.seek(position);
            reader.limit(position + length);
            return Channels.newInputStream(reader);
        } catch (StorageException e) {
            throw wrapException(e, "Range request failed for");
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
        try {
            BlobId blobId = BlobId.of(bucket, objectName);
            try (ReadChannel reader = storage.reader(blobId)) {
                reader.seek(position);
                reader.limit(position + target.remaining());
                int totalRead = 0;
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
            throw wrapException(e, "Failed to read bytes from");
        }
    }

    @Override
    public void readBytesAsync(long position, long length, Executor executor, ActionListener<ByteBuffer> listener) {
        if (position < 0) {
            listener.onFailure(new IllegalArgumentException("position must be non-negative, got: " + position));
            return;
        }
        if (length < 0) {
            listener.onFailure(new IllegalArgumentException("length must be non-negative, got: " + length));
            return;
        }

        executor.execute(() -> {
            try {
                BlobId blobId = BlobId.of(bucket, objectName);
                try (ReadChannel reader = storage.reader(blobId)) {
                    reader.seek(position);
                    reader.limit(position + length);
                    ByteBuffer buffer = ByteBuffer.allocate(Math.toIntExact(length));
                    while (buffer.hasRemaining()) {
                        int n = readFromChannel(reader, buffer);
                        if (n < 0) {
                            break;
                        }
                    }
                    buffer.flip();
                    listener.onResponse(buffer);
                }
            } catch (StorageException e) {
                listener.onFailure(wrapException(e, "Failed to read bytes from"));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    @Override
    public boolean supportsNativeAsync() {
        return true;
    }

    @SuppressForbidden(reason = "GCS ReadChannel is not a FileChannel; Channels.* helpers do not apply")
    private static int readFromChannel(ReadChannel reader, ByteBuffer target) throws IOException {
        return reader.read(target);
    }

    private IOException wrapException(StorageException e, String operation) {
        String message = e.getCode() == 404 ? "Object not found: " + path : operation + " " + path;
        return new IOException(message, e);
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
