/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Decorates a {@link StorageProvider} with concurrency limiting. Each cloud API call
 * acquires a permit from a shared {@link ConcurrencyLimiter} before executing and
 * releases it when the call completes. Permits are per-API-call, not held across
 * retries or backoff sleeps.
 */
class ConcurrencyLimitedStorageProvider implements StorageProvider {

    private final StorageProvider delegate;
    private final ConcurrencyLimiter limiter;

    ConcurrencyLimitedStorageProvider(StorageProvider delegate, ConcurrencyLimiter limiter) {
        this.delegate = delegate;
        this.limiter = limiter;
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
        acquirePermit();
        try {
            StorageIterator delegateIterator = delegate.listObjects(prefix, recursive);
            return new ConcurrencyLimitedStorageIterator(delegateIterator, limiter);
        } catch (Exception e) {
            limiter.release();
            throw e;
        }
    }

    @Override
    public boolean exists(StoragePath path) throws IOException {
        acquirePermit();
        try {
            return delegate.exists(path);
        } finally {
            limiter.release();
        }
    }

    @Override
    public List<String> supportedSchemes() {
        return delegate.supportedSchemes();
    }

    @Override
    public boolean supportsStableMetadata() {
        return delegate.supportsStableMetadata();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    private void acquirePermit() {
        try {
            limiter.acquire();
        } catch (TimeoutException e) {
            // Permit pool exhausted: a node-local admission back-pressure condition, not a client error. Raise it as
            // the retryable 503-class type the retry layer acts on (RetryableStorageProvider -> RetryPolicy.execute
            // catches ExternalUnavailableException and re-attempts). throttling=false: this is a local semaphore, not
            // a remote-store 429/503, so it must not feed the per-bucket adaptive backoff or the throttle budget.
            throw new ExternalUnavailableException(e, "Timed out acquiring cloud API concurrency permit: {}", e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // Interrupt is a shutdown/cancellation signal, not back-pressure: throw non-retryable so the
            // retry layer does not loop on an interrupt flag that will fire again immediately.
            throw new EsRejectedExecutionException("Interrupted while acquiring cloud API concurrency permit");
        }
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
