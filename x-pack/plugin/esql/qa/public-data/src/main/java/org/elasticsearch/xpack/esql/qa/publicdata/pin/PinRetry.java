/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.pin;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Bounded retry-with-backoff for the pin probes. Mirrors the semantics of the test-side
 * {@code HttpDownloadRetry} (retry 429/5xx and transport-level {@link IOException}s, rethrow
 * permanent errors immediately) but stands alone: the probes also run from plain {@code JavaExec}
 * CLIs where test-framework randomness (which requires a randomized-runner context) is
 * unavailable.
 */
public final class PinRetry {

    /** Total attempts, including the first. */
    public static final int DEFAULT_MAX_ATTEMPTS = 4;
    private static final long INITIAL_BACKOFF_MILLIS = 500;
    private static final long MAX_BACKOFF_MILLIS = 8_000;

    private PinRetry() {}

    /** An {@link IOException} carrying the HTTP status of a failed metadata request. */
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

    /** HTTP status codes considered transient, and therefore worth retrying. */
    public static boolean isRetryableStatus(int status) {
        return status == 429 || (status >= 500 && status < 600);
    }

    @FunctionalInterface
    public interface IoAttempt<T> {
        T get() throws IOException;
    }

    /** Runs {@code attempt} with capped exponential backoff and jitter on transient failures. */
    public static <T> T withRetries(String description, int maxAttempts, IoAttempt<T> attempt) throws IOException {
        IOException lastFailure = null;
        for (int attemptNumber = 1; attemptNumber <= maxAttempts; attemptNumber++) {
            try {
                return attempt.get();
            } catch (IOException e) {
                lastFailure = e;
                if (e instanceof HttpStatusException statusException && isRetryableStatus(statusException.status()) == false) {
                    throw e;
                }
                if (attemptNumber == maxAttempts) {
                    break;
                }
                long backoff = Math.min(INITIAL_BACKOFF_MILLIS << (attemptNumber - 1), MAX_BACKOFF_MILLIS);
                long jitter = ThreadLocalRandom.current().nextLong(backoff / 2 + 1);
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
