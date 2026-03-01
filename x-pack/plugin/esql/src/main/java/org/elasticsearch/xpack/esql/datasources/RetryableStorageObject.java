/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.Executor;

/**
 * Wraps a {@link StorageObject} with retry logic for transient storage failures.
 * Retries are applied to all I/O operations (stream open, metadata, async reads)
 * using exponential backoff with jitter.
 */
class RetryableStorageObject implements StorageObject {

    private static final Logger logger = LogManager.getLogger(RetryableStorageObject.class);

    private final StorageObject delegate;
    private final RetryPolicy retryPolicy;

    RetryableStorageObject(StorageObject delegate, RetryPolicy retryPolicy) {
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
    public InputStream newStream() throws IOException {
        return retryPolicy.execute(delegate::newStream, "newStream", delegate.path());
    }

    @Override
    public InputStream newStream(long position, long length) throws IOException {
        return retryPolicy.execute(() -> delegate.newStream(position, length), "newStream(range)", delegate.path());
    }

    @Override
    public long length() throws IOException {
        return retryPolicy.execute(delegate::length, "length", delegate.path());
    }

    @Override
    public Instant lastModified() throws IOException {
        return retryPolicy.execute(delegate::lastModified, "lastModified", delegate.path());
    }

    @Override
    public boolean exists() throws IOException {
        return retryPolicy.execute(delegate::exists, "exists", delegate.path());
    }

    @Override
    public StoragePath path() {
        return delegate.path();
    }

    @Override
    public void readBytesAsync(long position, long length, Executor executor, ActionListener<ByteBuffer> listener) {
        readBytesAsyncWithRetry(position, length, executor, listener, 0);
    }

    private void readBytesAsyncWithRetry(long position, long length, Executor executor, ActionListener<ByteBuffer> listener, int attempt) {
        delegate.readBytesAsync(position, length, executor, ActionListener.wrap(listener::onResponse, e -> {
            if (retryPolicy.isRetryable(e) && attempt < retryPolicy.maxRetries()) {
                long delay = retryPolicy.delayMillis(attempt);
                logger.debug(
                    "retrying async read for [{}] after transient failure (attempt [{}]/[{}], delay [{}]ms): [{}]",
                    delegate.path(),
                    attempt + 1,
                    retryPolicy.maxRetries(),
                    delay,
                    e.getMessage()
                );
                executor.execute(() -> {
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        listener.onFailure(new IOException("Retry interrupted", ie));
                        return;
                    }
                    readBytesAsyncWithRetry(position, length, executor, listener, attempt + 1);
                });
            } else {
                listener.onFailure(e);
            }
        }));
    }

    @Override
    public boolean supportsNativeAsync() {
        return delegate.supportsNativeAsync();
    }
}
