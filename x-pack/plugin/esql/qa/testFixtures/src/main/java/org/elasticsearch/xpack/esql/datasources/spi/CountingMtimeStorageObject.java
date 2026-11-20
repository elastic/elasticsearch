/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link StorageObject} that forwards everything to another one and counts the calls to
 * {@link #lastModified()}.
 * <p>
 * Statistics harvesting pins each read's source mtime, and the probe behind it is a round trip on stores
 * that carry no last-modified hint. Several suites assert that a read which harvests nothing never makes
 * that call, and each needs a control arm asserting that a read which does harvest still makes it --
 * otherwise the assertion also passes against a path that stopped probing altogether.
 */
public final class CountingMtimeStorageObject implements StorageObject {

    private final StorageObject delegate;
    private final AtomicInteger probes;

    /** Counts into {@code probes}, which the caller reads after the read has been drained. */
    public CountingMtimeStorageObject(StorageObject delegate, AtomicInteger probes) {
        this.delegate = delegate;
        this.probes = probes;
    }

    @Override
    public Instant lastModified() throws IOException {
        probes.incrementAndGet();
        return delegate.lastModified();
    }

    @Override
    public void attachMetrics(ExternalSourceMetrics metrics, String scheme) {
        delegate.attachMetrics(metrics, scheme);
    }

    @Override
    public StorageIdentity storageIdentity() {
        return delegate.storageIdentity();
    }

    @Override
    public InputStream newStream() throws IOException {
        return delegate.newStream();
    }

    @Override
    public InputStream newStream(long position, long length) throws IOException {
        return delegate.newStream(position, length);
    }

    @Override
    public long length() throws IOException {
        return delegate.length();
    }

    @Override
    public boolean exists() throws IOException {
        return delegate.exists();
    }

    @Override
    public StoragePath path() {
        return delegate.path();
    }
}
