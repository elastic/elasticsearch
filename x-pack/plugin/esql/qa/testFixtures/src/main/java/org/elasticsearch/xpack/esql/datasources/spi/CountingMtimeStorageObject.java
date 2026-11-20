/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Releasable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link StorageObject} that forwards everything to another one and counts the calls to
 * {@link #lastModified()}.
 * <p>
 * Statistics harvesting pins each read's source mtime, and the probe behind it is a round trip on stores
 * that carry no last-modified hint. Several suites assert that a read which harvests nothing never makes
 * that call, and each needs a control arm asserting that a read which does harvest still makes it --
 * otherwise the assertion also passes against a path that stopped probing altogether.
 * <p>
 * Every other member forwards, including the ones {@link StorageObject} gives a default: a size hint
 * decides streaming chunk sizing and a version decides cache identity, so a wrapper that let either fall
 * back to the interface default would put the test on a different read path from the object it wraps.
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
    public long knownLength() {
        return delegate.knownLength();
    }

    @Override
    public String contentGeneration() {
        return delegate.contentGeneration();
    }

    @Override
    public String version() {
        return delegate.version();
    }

    @Override
    public String probedVersion() {
        return delegate.probedVersion();
    }

    @Override
    public long lengthForFooterCacheKey() throws IOException {
        return delegate.lengthForFooterCacheKey();
    }

    @Override
    public long offsetForFooterCache(long position) {
        return delegate.offsetForFooterCache(position);
    }

    @Override
    public boolean exists() throws IOException {
        return delegate.exists();
    }

    @Override
    public StoragePath path() {
        return delegate.path();
    }

    @Override
    public void abortStream(InputStream stream) throws IOException {
        delegate.abortStream(stream);
    }

    @Override
    public int readBytes(long position, ByteBuffer target) throws IOException {
        return delegate.readBytes(position, target);
    }

    @Override
    public void readBytesAsync(
        long position,
        long length,
        DirectBufferFactory factory,
        Executor executor,
        ActionListener<DirectReadBuffer> listener
    ) {
        delegate.readBytesAsync(position, length, factory, executor, listener);
    }

    @Override
    public void readBytesAsync(long position, ByteBuffer target, Executor executor, ActionListener<Integer> listener) {
        delegate.readBytesAsync(position, target, executor, listener);
    }

    @Override
    public Releasable startReadBytesAsync(
        long position,
        long length,
        DirectBufferFactory factory,
        Executor executor,
        ActionListener<DirectReadBuffer> listener
    ) {
        return delegate.startReadBytesAsync(position, length, factory, executor, listener);
    }

    /** {@code barge} is forwarded, as {@link StorageObject#startReadBytesAsync} requires of a wrapper. */
    @Override
    public Releasable startReadBytesAsync(
        long position,
        long length,
        DirectBufferFactory factory,
        Executor executor,
        ActionListener<DirectReadBuffer> listener,
        boolean barge
    ) {
        return delegate.startReadBytesAsync(position, length, factory, executor, listener, barge);
    }

    @Override
    public boolean supportsNativeAsync() {
        return delegate.supportsNativeAsync();
    }

    @Override
    public boolean readBytesAsyncReleasesExecutor() {
        return delegate.readBytesAsyncReleasesExecutor();
    }

    @Override
    public StorageObjectMetrics metrics() {
        return delegate.metrics();
    }

    @Override
    public void bindRowGroup(RowGroupIo io) {
        delegate.bindRowGroup(io);
    }

    @Override
    public long admissionWaitTimeoutMs() {
        return delegate.admissionWaitTimeoutMs();
    }
}
