/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

/**
 * Wraps a {@link StorageProvider} so that every {@link StorageObject} it produces
 * is automatically wrapped with retry logic for transient storage failures.
 */
class RetryableStorageProvider implements StorageProvider {

    private final StorageProvider delegate;
    private final RetryPolicy retryPolicy;

    RetryableStorageProvider(StorageProvider delegate, RetryPolicy retryPolicy) {
        if (delegate == null) {
            throw new IllegalArgumentException("delegate cannot be null");
        }
        if (retryPolicy == null) {
            throw new IllegalArgumentException("retryPolicy cannot be null");
        }
        this.delegate = delegate;
        this.retryPolicy = retryPolicy;
    }

    @Override
    public StorageObject newObject(StoragePath path) {
        return new RetryableStorageObject(delegate.newObject(path), retryPolicy);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length) {
        return new RetryableStorageObject(delegate.newObject(path, length), retryPolicy);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
        return new RetryableStorageObject(delegate.newObject(path, length, lastModified), retryPolicy);
    }

    @Override
    public StorageIterator listObjects(StoragePath prefix, boolean recursive) throws IOException {
        return retryPolicy.execute(() -> delegate.listObjects(prefix, recursive), "listObjects", prefix);
    }

    @Override
    public boolean exists(StoragePath path) throws IOException {
        return retryPolicy.execute(() -> delegate.exists(path), "exists", path);
    }

    @Override
    public List<String> supportedSchemes() {
        return delegate.supportedSchemes();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
