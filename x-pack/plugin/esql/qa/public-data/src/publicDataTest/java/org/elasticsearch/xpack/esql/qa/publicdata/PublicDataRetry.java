/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata;

import org.elasticsearch.client.ResponseException;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;

/**
 * Bounded retry around a remote-backed spec execution. Third-party stores throttle and blip; a
 * flaky bucket is noise to absorb, not a finding. Only <em>transient</em> failures are retried —
 * a transport-level {@link IOException} (timeout, reset), or the cluster's own
 * retries-exhausted {@code 503}/{@code 429} — and on exhaustion the failure is rethrown as an
 * {@code INFRA_FAIL}-tagged {@link AssertionError} that <b>fails the run</b>, attributed separately
 * from correctness. Deliberately never {@code assumeTrue}: skipping on infra failures would let
 * real regressions hide behind a throttled bucket. Correctness failures ({@link AssertionError},
 * 4xx responses) are never retried.
 */
public final class PublicDataRetry {

    /** Marker prefix on retries-exhausted failures, for the verdict's INFRA_FAIL attribution. */
    public static final String INFRA_FAIL_PREFIX = "INFRA_FAIL: ";

    private static final Logger logger = LogManager.getLogger(PublicDataRetry.class);
    // S3 throttling windows persist for tens of seconds; immediate retries just re-hit the wall.
    private static final long INITIAL_BACKOFF_MILLIS = 10_000;
    private static final long MAX_BACKOFF_MILLIS = 60_000;

    private PublicDataRetry() {}

    @FunctionalInterface
    public interface SpecExecution {
        void run() throws Throwable;
    }

    /** Runs {@code execution}, retrying transient remote failures up to the configured budget. */
    public static void run(String description, SpecExecution execution) throws Throwable {
        int maxAttempts = Math.max(1, PublicDataFilters.fromSystemProperties().maxRetries());
        Throwable lastTransient = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                execution.run();
                return;
            } catch (Throwable failure) {
                if (isTransient(failure) == false) {
                    throw failure;
                }
                lastTransient = failure;
                if (attempt == maxAttempts) {
                    break;
                }
                long backoff = Math.min(INITIAL_BACKOFF_MILLIS << (attempt - 1), MAX_BACKOFF_MILLIS);
                logger.warn(
                    "attempt {}/{} of [{}] failed transiently ({}); retrying in {}ms",
                    attempt,
                    maxAttempts,
                    description,
                    failure.getMessage(),
                    backoff
                );
                Thread.sleep(backoff);
            }
        }
        throw new AssertionError(
            INFRA_FAIL_PREFIX + "exhausted " + maxAttempts + " attempts of [" + description + "] on transient remote failures",
            lastTransient
        );
    }

    /**
     * Transient: the REST client gave up waiting (a bare transport {@link IOException} with no
     * HTTP response), or the cluster exhausted its own retry budget against a throttled or
     * unavailable store (surfaced as 503, or 429 passed through). A 4xx is a real answer and an
     * {@link AssertionError} is a real mismatch — neither is retried.
     */
    static boolean isTransient(Throwable failure) {
        if (failure instanceof ResponseException responseException) {
            int status = responseException.getResponse().getStatusLine().getStatusCode();
            return status == 503 || status == 429;
        }
        return failure instanceof IOException;
    }
}
