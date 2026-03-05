/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;

/**
 * Retry policy with exponential backoff and jitter for transient storage failures.
 * Recognizes HTTP 429 (Too Many Requests), 503 (Service Unavailable), connection
 * resets, and socket timeouts as retryable.
 */
class RetryPolicy {

    private static final Logger logger = LogManager.getLogger(RetryPolicy.class);

    static final int DEFAULT_MAX_RETRIES = 3;
    static final long DEFAULT_INITIAL_DELAY_MS = 200;
    static final long DEFAULT_MAX_DELAY_MS = 5000;

    static final RetryPolicy NONE = new RetryPolicy(0, 0, 0);
    static final RetryPolicy DEFAULT = new RetryPolicy(DEFAULT_MAX_RETRIES, DEFAULT_INITIAL_DELAY_MS, DEFAULT_MAX_DELAY_MS);

    private final int maxRetries;
    private final long initialDelayMs;
    private final long maxDelayMs;

    RetryPolicy(int maxRetries, long initialDelayMs, long maxDelayMs) {
        this.maxRetries = maxRetries;
        this.initialDelayMs = initialDelayMs;
        this.maxDelayMs = maxDelayMs;
    }

    int maxRetries() {
        return maxRetries;
    }

    long delayMillis(int attempt) {
        if (maxRetries == 0) {
            return 0;
        }
        long baseDelay = initialDelayMs * (1L << attempt);
        long capped = Math.min(baseDelay, maxDelayMs);
        long jitter = Randomness.get().nextLong(capped / 4 + 1);
        return Math.min(maxDelayMs, capped + jitter);
    }

    boolean isRetryable(Throwable t) {
        if (maxRetries == 0) {
            return false;
        }
        return isTransientStorageError(t);
    }

    <T> T execute(IOSupplier<T> operation, String operationName, StoragePath path) throws IOException {
        if (maxRetries == 0) {
            return operation.get();
        }
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                return operation.get();
            } catch (IOException e) {
                if (isTransientStorageError(e) == false || attempt == maxRetries) {
                    throw e;
                }
                long delay = delayMillis(attempt);
                logger.debug(
                    "retrying [{}] for [{}] after transient failure (attempt [{}]/[{}], delay [{}]ms): [{}]",
                    operationName,
                    path,
                    attempt + 1,
                    maxRetries,
                    delay,
                    e.getMessage()
                );
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
        // unreachable: loop always returns or throws
        throw new AssertionError("retry loop exited unexpectedly");
    }

    private static boolean isTransientStorageError(Throwable t) {
        for (Throwable current = t; current != null; current = current.getCause()) {
            if (isTransientSingleCause(current)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isTransientSingleCause(Throwable t) {
        if (t instanceof SocketTimeoutException || t instanceof ConnectException) {
            return true;
        }
        if (t instanceof SocketException) {
            String msg = t.getMessage();
            return msg != null && msg.contains("Connection reset");
        }
        String message = t.getMessage();
        if (message == null) {
            return false;
        }
        if (message.contains("429") || message.contains("Too Many Requests")) {
            return true;
        }
        if (message.contains("503") || message.contains("Service Unavailable")) {
            return true;
        }
        if (message.contains("SlowDown") || message.contains("Reduce your request rate")) {
            return true;
        }
        return false;
    }

    @FunctionalInterface
    interface IOSupplier<T> {
        T get() throws IOException;
    }
}
