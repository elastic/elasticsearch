/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;

import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;

/**
 * StorageObject implementation for Azure Blob Storage.
 * Supports full and range reads, and metadata retrieval with caching.
 */
public final class AzureStorageObject implements StorageObject {
    private final BlobClient blobClient;
    private final String container;
    private final String blobName;
    private final StoragePath path;

    private volatile Long cachedLength;
    private volatile Instant cachedLastModified;
    private volatile Boolean cachedExists;

    public AzureStorageObject(BlobClient blobClient, String container, String blobName, StoragePath path) {
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
        this.container = container;
        this.blobName = blobName;
        this.path = path;
    }

    public AzureStorageObject(BlobClient blobClient, String container, String blobName, StoragePath path, long length) {
        this(blobClient, container, blobName, path);
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
        this(blobClient, container, blobName, path, length);
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
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative, got: " + length);
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
        } catch (com.azure.storage.blob.models.BlobStorageException e) {
            if (e.getStatusCode() == 404) {
                cachedExists = false;
                cachedLength = 0L;
                cachedLastModified = null;
            } else {
                throw new IOException("Failed to get metadata for " + path, e);
            }
        } catch (Exception e) {
            throw new IOException("Failed to get metadata for " + path, e);
        }
    }

    @Override
    public String toString() {
        return "AzureStorageObject{container=" + container + ", blobName=" + blobName + ", path=" + path + "}";
    }
}
