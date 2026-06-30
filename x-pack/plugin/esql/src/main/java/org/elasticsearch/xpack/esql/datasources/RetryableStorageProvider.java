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
 * per-bucket for S3/GCS, per-account for Azure), not per scheme: a hot bucket backs off only its own traffic
 * instead of slowing every read on the same store. The scope's {@link RetryPolicy} (which carries that scope's
 * backoff) is looked up — cached, computed once per scope — when an object is created, since the path, hence the
 * scope, is known there. The two-arg constructor binds a single fixed policy for every path (used by tests and
 * any caller that does not need per-scope backoff).
 */
class RetryableStorageProvider implements StorageProvider {

    private final StorageProvider delegate;
    private final RetryScheduler retryScheduler;
    private final Function<StoragePath, RetryPolicy> policyForScope;

    RetryableStorageProvider(StorageProvider delegate, RetryPolicy retryPolicy) {
        this(delegate, RetryScheduler.DIRECT, fixed(retryPolicy));
    }

    RetryableStorageProvider(StorageProvider delegate, RetryScheduler retryScheduler, Function<StoragePath, RetryPolicy> policyForScope) {
        if (delegate == null) {
            throw new IllegalArgumentException("delegate cannot be null");
        }
        if (retryScheduler == null) {
            throw new IllegalArgumentException("retryScheduler cannot be null");
        }
        if (policyForScope == null) {
            throw new IllegalArgumentException("policyForScope cannot be null");
        }
        this.delegate = delegate;
        this.retryScheduler = retryScheduler;
        this.policyForScope = policyForScope;
    }

    /** A scope lookup that returns the same fixed policy for every path (no per-scope backoff). */
    private static Function<StoragePath, RetryPolicy> fixed(RetryPolicy policy) {
        if (policy == null) {
            throw new IllegalArgumentException("retryPolicy cannot be null");
        }
        return path -> policy;
    }

    /** The retry policy for this path's throttle scope (cached per scope; the scope carries its adaptive backoff). */
    private RetryPolicy policyFor(StoragePath path) {
        return policyForScope.apply(path);
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
