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
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetricsCounters;
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
    /**
     * Local counter for retries observed at this decorator boundary, merged into {@link #metrics()}
     * via {@link StorageObjectMetrics#add}.
     * <p>
     * <b>Invariant:</b> only {@link StorageObjectMetricsCounters#addRetry()} may be called on this
     * instance. The delegate already counts requests / request-nanos / bytes-read; calling
     * {@code addRequest} here would double-count those into the merged snapshot.
     */
    private final StorageObjectMetricsCounters retryCounters = new StorageObjectMetricsCounters();

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
        return retryPolicy.execute(delegate::newStream, "newStream", delegate.path(), retryCounters::addRetry);
    }

    @Override
    public InputStream newStream(long position, long length) throws IOException {
        return retryPolicy.execute(
            () -> delegate.newStream(position, length),
            "newStream(range)",
            delegate.path(),
            retryCounters::addRetry
        );
    }

    @Override
    public long length() throws IOException {
        return retryPolicy.execute(delegate::length, "length", delegate.path(), retryCounters::addRetry);
    }

    @Override
    public Instant lastModified() throws IOException {
        return retryPolicy.execute(delegate::lastModified, "lastModified", delegate.path(), retryCounters::addRetry);
    }

    @Override
    public boolean exists() throws IOException {
        return retryPolicy.execute(delegate::exists, "exists", delegate.path(), retryCounters::addRetry);
    }

    @Override
    public StoragePath path() {
        return delegate.path();
    }

    @Override
    public void abortStream(InputStream stream) throws IOException {
        // No retry on abort: the underlying provider's abortStream is a best-effort
        // connection-discard (e.g. S3 ResponseInputStream.abort()). If we silently fall back to
        // the SPI default stream.close() here, providers like S3 drain the entire response body
        // before returning, defeating the purpose of abortStream on partial-read paths.
        delegate.abortStream(stream);
    }

    @Override
    public int readBytes(long position, ByteBuffer target) throws IOException {
        int savedPosition = target.position();
        return retryPolicy.execute(() -> {
            target.position(savedPosition);
            return delegate.readBytes(position, target);
        }, "readBytes", delegate.path(), retryCounters::addRetry);
    }

    @Override
    public void readBytesAsync(
        long position,
        long length,
        DirectBufferFactory factory,
        Executor executor,
        ActionListener<DirectReadBuffer> listener
    ) {
        readBytesAsyncWithRetry(position, length, factory, executor, listener, 0, System.nanoTime());
    }

    private void readBytesAsyncWithRetry(
        long position,
        long length,
        DirectBufferFactory factory,
        Executor executor,
        ActionListener<DirectReadBuffer> listener,
        int attempt,
        long startNanos
    ) {
        delegate.readBytesAsync(position, length, factory, executor, new ActionListener<>() {
            @Override
            public void onResponse(DirectReadBuffer result) {
                retryPolicy.notifySuccess();
                // Do NOT route a throw from listener.onResponse into onFailure — that would
                // trigger retry logic or double-complete the downstream listener. Propagate
                // the exception directly so the caller's uncaught-exception handler deals with it.
                try {
                    listener.onResponse(result);
                } catch (Exception e) {
                    // listener.onResponse threw before consuming the buffer; release it so the
                    // breaker reservation does not outlive the failed delivery.
                    try {
                        result.close();
                    } catch (Exception closeEx) {
                        e.addSuppressed(closeEx);
                    }
                    throw e;
                }
            }

            @Override
            public void onFailure(Exception e) {
                boolean isThrottle = RetryPolicy.isThrottlingError(e);
                int effectiveMaxRetries = isThrottle ? retryPolicy.throttleMaxRetries() : retryPolicy.maxRetries();

                if (isThrottle) {
                    retryPolicy.notifyThrottled();
                }

                if (retryPolicy.isRetryable(e) && attempt < effectiveMaxRetries) {
                    retryCounters.addRetry();
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
                        readBytesAsyncWithRetry(position, length, factory, executor, listener, attempt + 1, startNanos);
                    });
                } else {
                    listener.onFailure(e);
                }
            }
        });
    }

    @Override
    public boolean supportsNativeAsync() {
        return delegate.supportsNativeAsync();
    }

    @Override
    public StorageObjectMetrics metrics() {
        return delegate.metrics().add(retryCounters.snapshot());
    }
}
