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
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
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
public final class AzureStorageObject implements StorageObject {
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
        try {
            return blobClient.openInputStream();
        } catch (Exception e) {
            throw new IOException("Failed to read object from " + path, e);
        }
    }

    @Override
    public InputStream newStream(long position, long length) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException("position must be non-negative, got: " + position);
        }
        if (length <= 0) {
            throw new IllegalArgumentException("length must be positive, got: " + length);
        }

        try {
            BlobRange range = new BlobRange(position, length);
            return blobClient.openInputStream(range, new BlobRequestConditions());
        } catch (Exception e) {
            throw new IOException("Range request failed for " + path, e);
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
            StorageObject.super.readBytesAsync(position, length, executor, listener);
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
        blobAsyncClient.downloadWithResponse(range, null, null, false)
            .flatMapMany(response -> response.getValue())
            .reduce(ByteBuffer.allocate(Math.toIntExact(length)), (acc, buf) -> {
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
                    Throwable cause = error.getCause() != null ? error.getCause() : error;
                    listener.onFailure(cause instanceof Exception e ? e : new RuntimeException(cause));
                } else {
                    listener.onResponse(buffer);
                }
            });
    }

    @Override
    public boolean supportsNativeAsync() {
        return blobAsyncClient != null;
    }

    @Override
    public String toString() {
        return "AzureStorageObject{container=" + container + ", blobName=" + blobName + ", path=" + path + "}";
    }
}
