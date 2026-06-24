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
import java.util.function.Function;

/**
 * Wraps a {@link StorageProvider} so that every {@link StorageObject} it produces
 * is automatically wrapped with retry logic for transient storage failures.
 * <p>
 * The reactive {@link AdaptiveBackoff} is selected per <em>throttle scope</em> (the store's own hot unit —
 * per-bucket for S3/GCS, per-account/container for Azure), not per scheme: a hot bucket backs off only its own
 * traffic instead of slowing every read on the same store. The scope's backoff is bound onto the base policy when
 * an object is created (the path — hence the scope — is known there). When no scope lookup is supplied (tests),
 * the base policy is used unchanged. See elastic/esql-planning#896.
 */
class RetryableStorageProvider implements StorageProvider {

    private final StorageProvider delegate;
    private final RetryPolicy basePolicy;
    private final RetryScheduler retryScheduler;
    private final Function<StoragePath, AdaptiveBackoff> backoffForScope;

    RetryableStorageProvider(StorageProvider delegate, RetryPolicy retryPolicy) {
        this(delegate, retryPolicy, RetryScheduler.DIRECT, null);
    }

    RetryableStorageProvider(StorageProvider delegate, RetryPolicy retryPolicy, RetryScheduler retryScheduler) {
        this(delegate, retryPolicy, retryScheduler, null);
    }

    RetryableStorageProvider(
        StorageProvider delegate,
        RetryPolicy retryPolicy,
        RetryScheduler retryScheduler,
        Function<StoragePath, AdaptiveBackoff> backoffForScope
    ) {
        if (delegate == null) {
            throw new IllegalArgumentException("delegate cannot be null");
        }
        if (retryPolicy == null) {
            throw new IllegalArgumentException("retryPolicy cannot be null");
        }
        if (retryScheduler == null) {
            throw new IllegalArgumentException("retryScheduler cannot be null");
        }
        this.delegate = delegate;
        this.basePolicy = retryPolicy;
        this.retryScheduler = retryScheduler;
        this.backoffForScope = backoffForScope;
    }

    /** The base retry policy with the per-scope adaptive backoff bound on (or unchanged when no lookup is set). */
    private RetryPolicy policyFor(StoragePath path) {
        return backoffForScope == null ? basePolicy : basePolicy.withAdaptiveBackoff(backoffForScope.apply(path));
    }

    @Override
    public StorageObject newObject(StoragePath path) {
        return new RetryableStorageObject(delegate.newObject(path), policyFor(path), retryScheduler);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length) {
        return new RetryableStorageObject(delegate.newObject(path, length), policyFor(path), retryScheduler);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
        return new RetryableStorageObject(delegate.newObject(path, length, lastModified), policyFor(path), retryScheduler);
    }

    @Override
    public StorageIterator listObjects(StoragePath prefix, boolean recursive) throws IOException {
        return policyFor(prefix).execute(() -> delegate.listObjects(prefix, recursive), "listObjects", prefix);
    }

    @Override
    public boolean exists(StoragePath path) throws IOException {
        return policyFor(path).execute(() -> delegate.exists(path), "exists", path);
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
}
