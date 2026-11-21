/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.xpack.esql.datasources.cache.StorageProviderCache;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.RowGroupIo;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.Executor;

/**
 * A file that a data source reads, together with the borrow of the storage provider that serves it, as {@link DataSourceStorageResolver}
 * returns it. The registry pools the cloud clients and counts the borrows, and shuts an idle client down after a while, so whoever
 * keeps a file for longer than one call must hold the borrow until it is done and then {@link #close()} this object.
 *
 * <p>Every method of {@link StorageObject} goes to the wrapped object, including the defaults, because a provider overrides some of
 * them with native implementations (an asynchronous ranged read, for example) that the default methods would not use.
 */
public final class BorrowedStorageObject implements StorageObject, Closeable {

    private final StorageObject delegate;
    private final StorageProvider borrow;

    BorrowedStorageObject(StorageObject delegate, StorageProvider borrow) {
        this.delegate = delegate;
        this.borrow = borrow;
    }

    /** Gives the borrow back to the pool. It does nothing for a provider that is not pooled, which must never be shut down here. */
    @Override
    public void close() {
        StorageProviderCache.closeLease(borrow);
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
    public Instant lastModified() throws IOException {
        return delegate.lastModified();
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
    public Releasable startReadBytesAsync(
        long position,
        long length,
        DirectBufferFactory factory,
        Executor executor,
        ActionListener<DirectReadBuffer> listener
    ) {
        return delegate.startReadBytesAsync(position, length, factory, executor, listener);
    }

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
    public void readBytesAsync(long position, ByteBuffer target, Executor executor, ActionListener<Integer> listener) {
        delegate.readBytesAsync(position, target, executor, listener);
    }

    @Override
    public int readBytes(long position, ByteBuffer target) throws IOException {
        return delegate.readBytes(position, target);
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
    public void attachMetrics(ExternalSourceMetrics metrics, String scheme) {
        delegate.attachMetrics(metrics, scheme);
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
