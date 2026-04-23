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
 * using exponential backoff with jitter. Throttling errors (429/503) get a higher
 * retry budget than other transient errors.
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
    public int readBytes(long position, ByteBuffer target) throws IOException {
        int savedPosition = target.position();
        return retryPolicy.execute(() -> {
            target.position(savedPosition);
            return delegate.readBytes(position, target);
        }, "readBytes", delegate.path());
    }

    @Override
    public void readBytesAsync(long position, long length, Executor executor, ActionListener<ByteBuffer> listener) {
        readBytesAsyncWithRetry(position, length, executor, listener, 0, System.nanoTime());
    }

    private void readBytesAsyncWithRetry(
        long position,
        long length,
        Executor executor,
        ActionListener<ByteBuffer> listener,
        int attempt,
        long startNanos
    ) {
        delegate.readBytesAsync(position, length, executor, ActionListener.wrap(result -> {
            retryPolicy.notifySuccess();
            listener.onResponse(result);
        }, e -> {
            boolean isThrottle = RetryPolicy.isThrottlingError(e);
            int effectiveMaxRetries = isThrottle ? retryPolicy.throttleMaxRetries() : retryPolicy.maxRetries();

            if (isThrottle) {
                retryPolicy.notifyThrottled();
            }

            if (retryPolicy.isRetryable(e) && attempt < effectiveMaxRetries) {
                long delay = retryPolicy.delayMillis(attempt, isThrottle);
                long budgetMs = retryPolicy.maxTotalDurationMs();
                if (budgetMs > 0) {
                    long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
                    if (elapsedMs + delay > budgetMs) {
                        logger.debug(
                            "aborting async retry for [{}]: elapsed [{}]ms + delay [{}]ms exceeds budget [{}]ms",
                            delegate.path(),
                            elapsedMs,
                            delay,
                            budgetMs
                        );
                        listener.onFailure(e);
                        return;
                    }
                }
                logger.debug(
                    "retrying async read for [{}] after {} failure (attempt [{}]/[{}], delay [{}]ms): [{}]",
                    delegate.path(),
                    isThrottle ? "throttle" : "transient",
                    attempt + 1,
                    effectiveMaxRetries,
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
                    readBytesAsyncWithRetry(position, length, executor, listener, attempt + 1, startNanos);
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
