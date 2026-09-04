/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

/**
 * Shared retry-with-backoff helper for test fixtures that download data over HTTP from third-party
 * hosts (e.g. {@code raw.githubusercontent.com}, ClickHouse's ClickBench dataset host). These hosts
 * occasionally rate-limit (HTTP 429) or return transient 5xx errors under CI load; {@link #withRetries}
 * retries such failures with capped exponential backoff and jitter. Any other failure (e.g. a permanent
 * 4xx like 404, or exhausted retries) is rethrown so the caller can decide how to handle it -- typically
 * skipping the affected file or test via {@code assumeNoException} rather than failing outright.
 */
public final class HttpDownloadRetry {

    private HttpDownloadRetry() {}

    /** HTTP status codes considered transient, and therefore worth retrying. */
    public static boolean isRetryableStatus(int status) {
        return status == 429 || (status >= 500 && status < 600);
    }

    /**
     * An {@link IOException} carrying the HTTP status code of a failed download attempt, so
     * {@link #withRetries} can distinguish transient failures (429/5xx) from permanent ones (e.g. 404)
     * without wasting time retrying the latter.
     */
    public static class HttpStatusException extends IOException {
        private final int status;

        public HttpStatusException(String message, int status) {
            super(message);
            this.status = status;
        }

        public int status() {
            return status;
        }
    }

    /**
     * Runs {@code attempt}, retrying on transient failures with capped exponential backoff and jitter.
     * A failure is transient -- and therefore retried -- if it is either an {@link HttpStatusException}
     * with a {@linkplain #isRetryableStatus(int) retryable status}, or any other {@link IOException}
     * (e.g. a connection reset or timeout below the HTTP layer, which are inherently transient). A
     * non-retryable {@link HttpStatusException} (e.g. 404) is rethrown immediately without retrying.
     *
     * @param description short, human-readable description of the operation, used in log messages
     * @param maxAttempts total number of attempts, including the first (non-retry) one
     * @param initialBackoffMillis backoff before the first retry; doubles on each subsequent retry
     * @param maxBackoffMillis cap applied to the (pre-jitter) backoff
     */
    public static <T> T withRetries(
        Logger logger,
        String description,
        int maxAttempts,
        long initialBackoffMillis,
        long maxBackoffMillis,
        CheckedSupplier<T, IOException> attempt
    ) throws IOException {
        IOException lastFailure = null;
        for (int attemptNum = 1; attemptNum <= maxAttempts; attemptNum++) {
            try {
                return attempt.get();
            } catch (IOException e) {
                lastFailure = e;
                if (e instanceof HttpStatusException statusException && isRetryableStatus(statusException.status()) == false) {
                    throw e;
                }
                if (attemptNum == maxAttempts) {
                    break;
                }
                long backoff = Math.min(initialBackoffMillis << (attemptNum - 1), maxBackoffMillis);
                long jitter = ESTestCase.randomLongBetween(0, backoff / 2);
                logger.warn(
                    "Attempt {}/{} to {} failed ({}), retrying in {}ms",
                    attemptNum,
                    maxAttempts,
                    description,
                    e.getMessage(),
                    backoff + jitter
                );
                try {
                    Thread.sleep(backoff + jitter);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while retrying " + description, interrupted);
                }
            }
        }
        throw lastFailure;
    }
}
