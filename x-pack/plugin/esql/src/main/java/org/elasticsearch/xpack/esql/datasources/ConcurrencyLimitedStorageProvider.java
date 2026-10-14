/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.StorageChildren;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.time.Instant;

/**
 * Decorates a {@link StorageProvider} with concurrency limiting. {@link #listObjects}, {@link #listChildren} and
 * {@link #exists} acquire a permit from a shared {@link ConcurrencyLimiter} before executing and release it when the
 * call completes; byte transfers acquire theirs inside the {@link StorageObject} that {@link #newObject} wraps.
 * Permits are per-API-call, not held across retries or backoff sleeps, except for a listing's, which is held until
 * its iterator is closed.
 * <p>
 * {@code objectMetadata} is deliberately not permit-governed: it is left to the base's forward, matching
 * {@code ConcurrencyLimitedStorageObject#length}, which the resolve path called before this and which forwards
 * without a permit. Parking a metadata request behind the permits that long-lived streams hold starved the threads
 * the resolve runs on.
 */
class ConcurrencyLimitedStorageProvider extends DelegatingStorageProvider {

    private final StorageProvider delegate;
    private final ConcurrencyLimiter limiter;

    ConcurrencyLimitedStorageProvider(StorageProvider delegate, ConcurrencyLimiter limiter) {
        this.delegate = delegate;
        this.limiter = limiter;
    }

    @Override
    protected StorageProvider delegate() {
        return delegate;
    }

    @Override
    public StorageObject newObject(StoragePath path) {
        return new ConcurrencyLimitedStorageObject(delegate.newObject(path), limiter);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length) {
        return new ConcurrencyLimitedStorageObject(delegate.newObject(path, length), limiter);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
        return new ConcurrencyLimitedStorageObject(delegate.newObject(path, length, lastModified), limiter);
    }

    @Override
    public StorageIterator listObjects(StoragePath prefix, boolean recursive) throws IOException {
        limiter.acquireChecked();
        try {
            StorageIterator delegateIterator = delegate.listObjects(prefix, recursive);
            return new ConcurrencyLimitedStorageIterator(delegateIterator, limiter);
        } catch (Exception e) {
            limiter.release();
            throw e;
        }
    }

    @Override
    public StorageChildren listChildren(StoragePath prefix, int limit) throws IOException {
        // One remote call, fully materialized by the delegate: the permit brackets the call itself.
        limiter.acquireChecked();
        try {
            return delegate.listChildren(prefix, limit);
        } finally {
            limiter.release();
        }
    }

    @Override
    public boolean exists(StoragePath path) throws IOException {
        limiter.acquireChecked();
        try {
            return delegate.exists(path);
        } finally {
            limiter.release();
        }
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    /**
     * Iterator wrapper that holds a permit for the duration of iteration.
     * The permit acquired during {@link #listObjects} is released when the iterator is closed.
     */
    private static class ConcurrencyLimitedStorageIterator implements StorageIterator {
        private final StorageIterator delegate;
        private final ConcurrencyLimiter limiter;
        private boolean closed;

        ConcurrencyLimitedStorageIterator(StorageIterator delegate, ConcurrencyLimiter limiter) {
            this.delegate = delegate;
            this.limiter = limiter;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public StorageEntry next() {
            return delegate.next();
        }

        @Override
        public void close() throws IOException {
            if (closed == false) {
                closed = true;
                try {
                    delegate.close();
                } finally {
                    limiter.release();
                }
            }
        }
    }
}
