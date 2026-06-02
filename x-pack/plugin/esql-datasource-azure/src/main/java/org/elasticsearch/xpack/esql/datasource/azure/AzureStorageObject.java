/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractMeteredStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.TransientStorageException;
import org.elasticsearch.xpack.esql.datasources.utils.ContentRangeParser;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.Executor;

/**
 * StorageObject implementation for Azure Blob Storage.
 * Supports full and range reads, and metadata retrieval with caching.
 */
public final class AzureStorageObject extends AbstractMeteredStorageObject {
    private final BlobClient blobClient;
    private final BlobAsyncClient blobAsyncClient;
    private final String container;
    private final String blobName;
    private final StoragePath path;

    private volatile Long cachedLength;
    private volatile Instant cachedLastModified;
    private volatile Boolean cachedExists;

    public AzureStorageObject(BlobClient blobClient, String container, String blobName, StoragePath path) {
        this(blobClient, null, container, blobName, path);
    }

    public AzureStorageObject(BlobClient blobClient, BlobAsyncClient blobAsyncClient, String container, String blobName, StoragePath path) {
        if (blobClient == null) {
            throw new IllegalArgumentException("blobClient cannot be null");
        }
        if (container == null || container.isEmpty()) {
            throw new IllegalArgumentException("container cannot be null or empty");
        }
        if (blobName == null) {
            throw new IllegalArgumentException("blobName cannot be null");
        }
        if (path == null) {
            throw new IllegalArgumentException("path cannot be null");
        }
        this.blobClient = blobClient;
        this.blobAsyncClient = blobAsyncClient;
        this.container = container;
        this.blobName = blobName;
        this.path = path;
    }

    public AzureStorageObject(BlobClient blobClient, String container, String blobName, StoragePath path, long length) {
        this(blobClient, null, container, blobName, path, length);
    }

    public AzureStorageObject(
        BlobClient blobClient,
        BlobAsyncClient blobAsyncClient,
        String container,
        String blobName,
        StoragePath path,
        long length
    ) {
        this(blobClient, blobAsyncClient, container, blobName, path);
        this.cachedLength = length;
    }

    public AzureStorageObject(
        BlobClient blobClient,
        String container,
        String blobName,
        StoragePath path,
        long length,
        Instant lastModified
    ) {
        this(blobClient, null, container, blobName, path, length, lastModified);
    }

    public AzureStorageObject(
        BlobClient blobClient,
        BlobAsyncClient blobAsyncClient,
        String container,
        String blobName,
        StoragePath path,
        long length,
        Instant lastModified
    ) {
        this(blobClient, blobAsyncClient, container, blobName, path, length);
        this.cachedLastModified = lastModified;
    }

    @Override
    public InputStream newStream() throws IOException {
        long startNanos = System.nanoTime();
        long bytes = 0L;
        try {
            InputStream stream = new AzureTransientTypingInputStream(blobClient.openInputStream(), path);
            if (cachedLength != null) {
                bytes = cachedLength;
            }
            return stream;
        } catch (Exception e) {
            throw classify(e, "Failed to read object from");
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
            // READ_TO_END: the offset-only BlobRange reads from position to the end of the blob — no length() lookup.
            BlobRange range = toEnd ? new BlobRange(position) : new BlobRange(position, length);
            return new AzureTransientTypingInputStream(blobClient.openInputStream(range, new BlobRequestConditions()), path);
        } catch (Exception e) {
            if (toEnd && e instanceof BlobStorageException bse && bse.getStatusCode() == 416) {
                // Open-ended read at/after the end of an (empty or shorter) object: nothing to read. The SPI
                // contract for an open-ended read past the end is an empty stream.
                return InputStream.nullInputStream();
            }
            throw classify(e, "Range request failed for");
        } finally {
            counters.addRequest(System.nanoTime() - startNanos, toEnd ? 0L : length);
        }
    }

    /**
     * Types transient/throttle Azure failures so the provider-agnostic retry layer classifies them like S3/GCS;
     * 4xx and other failures surface as a plain IOException and are not retried.
     */
    private IOException classify(Exception e, String operation) {
        if (e instanceof BlobStorageException bse) {
            int status = bse.getStatusCode();
            if (status == 404) {
                return new IOException("Object not found: " + path, e);
            }
            if (status == 503 || status == 429) {
                return new TransientStorageException(operation + " " + path, e, true);
            }
            if (status >= 500) {
                return new TransientStorageException(operation + " " + path, e, false);
            }
        }
        return new IOException(operation + " " + path, e);
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

    private void fetchMetadata() throws IOException {
        try {
            var properties = blobClient.getProperties();
            cachedExists = true;
            cachedLength = properties.getBlobSize();
            cachedLastModified = properties.getLastModified() != null ? properties.getLastModified().toInstant() : null;
        } catch (Exception e) {
            if (e instanceof BlobStorageException bse && bse.getStatusCode() == 404) {
                setNotFound();
            } else if (e instanceof BlobStorageException bse && bse.getStatusCode() == 403) {
                fetchMetadataViaRangeGet();
            } else {
                throw new IOException("Failed to get metadata for " + path, e);
            }
        }
    }

    private void fetchMetadataViaRangeGet() throws IOException {
        try {
            var output = new ByteArrayOutputStream();
            var response = blobClient.downloadStreamWithResponse(output, new BlobRange(0, 1L), null, null, false, null, null);
            var headers = response.getDeserializedHeaders();
            cachedExists = true;
            Long total = ContentRangeParser.parseTotalLength(headers.getContentRange());
            if (total == null) {
                throw new IOException(
                    "Failed to determine object size for " + path + ": Content-Range header missing from range GET response"
                );
            }
            cachedLength = total;
            cachedLastModified = headers.getLastModified() != null ? headers.getLastModified().toInstant() : null;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            if (e instanceof BlobStorageException bse && bse.getStatusCode() == 404) {
                setNotFound();
            } else {
                throw new IOException("Failed to get metadata for " + path + " (properties denied, range GET also failed)", e);
            }
        }
    }

    private void setNotFound() {
        cachedExists = false;
        cachedLength = 0L;
        cachedLastModified = null;
    }

    @Override
    public void readBytesAsync(long position, long length, Executor executor, ActionListener<ByteBuffer> listener) {
        if (blobAsyncClient == null) {
            super.readBytesAsync(position, length, executor, listener);
            return;
        }

        if (position < 0) {
            listener.onFailure(new IllegalArgumentException("position must be non-negative, got: " + position));
            return;
        }
        if (length <= 0) {
            listener.onFailure(new IllegalArgumentException("length must be positive, got: " + length));
            return;
        }

        BlobRange range = new BlobRange(position, length);
        long startNanos = System.nanoTime();
        blobAsyncClient.downloadWithResponse(range, null, null, false)
            .flatMapMany(response -> response.getValue())
            .reduce(ByteBuffer.allocateDirect(Math.toIntExact(length)), (acc, buf) -> {
                if (buf.remaining() > acc.remaining()) {
                    throw new IllegalStateException("Server returned more bytes than requested (" + length + ")");
                }
                acc.put(buf);
                return acc;
            })
            .map(buffer -> {
                buffer.flip();
                return buffer;
            })
            .toFuture()
            .whenComplete((buffer, error) -> {
                if (error != null) {
                    counters.addRequest(System.nanoTime() - startNanos, 0L);
                    Throwable cause = error.getCause() != null ? error.getCause() : error;
                    listener.onFailure(cause instanceof Exception e ? e : new RuntimeException(cause));
                } else {
                    counters.addRequest(System.nanoTime() - startNanos, buffer.remaining());
                    listener.onResponse(buffer);
                }
            });
    }

    @Override
    public boolean supportsNativeAsync() {
        return blobAsyncClient != null;
    }

    // TODO: wire retry counts via an Azure SDK HttpPipelinePolicy interceptor; SDK-internal
    // retries are not yet visible to counters.addRetry().

    @Override
    public String toString() {
        return "AzureStorageObject{container=" + container + ", blobName=" + blobName + ", path=" + path + "}";
    }
}
