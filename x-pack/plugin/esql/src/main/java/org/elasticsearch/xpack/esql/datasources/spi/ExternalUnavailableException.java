/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.rest.RestStatus;

/**
 * The retryable, 503-class carrier for a back-pressure / temporarily-unavailable condition on an
 * external read. Most commonly a transport failure talking to the remote store (the store returned a
 * 5xx, throttled us, or the connection timed out / was reset), but also a node-local admission
 * condition such as storage concurrency permit exhaustion. Maps to {@code 503 Service Unavailable}:
 * the read might succeed on retry, so it is neither a permanent client error nor a cluster bug.
 * <p>
 * Permanent transport outcomes (object not found, a malformed response) are not raised here; those
 * are client-class and surface as {@link ExternalClientException}.
 * <p>
 * Carries a {@link #throttling()} flag so the retry layer can tell a back-pressure response (429 /
 * 503 — slow down) from a plain transient failure (500 / 502 / 504): throttling failures get a
 * higher retry budget and feed the cross-request adaptive backoff. The provider sets it from the
 * status code (it has the concrete type); the retry policy keys on the typed exception, never on
 * message text.
 */
public final class ExternalUnavailableException extends ExternalException {

    private final boolean throttling;
    /** Server-supplied wait hint in milliseconds; 0 means absent. */
    private final long retryAfterMs;

    public ExternalUnavailableException(String message, Throwable cause) {
        super(message, cause);
        this.throttling = false;
        this.retryAfterMs = 0L;
    }

    public ExternalUnavailableException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
        this.throttling = false;
        this.retryAfterMs = 0L;
    }

    public ExternalUnavailableException(String message, Object... args) {
        super(message, args);
        this.throttling = false;
        this.retryAfterMs = 0L;
    }

    public ExternalUnavailableException(boolean throttling, Throwable cause, String message, Object... args) {
        super(cause, message, args);
        this.throttling = throttling;
        this.retryAfterMs = 0L;
    }

    public ExternalUnavailableException(boolean throttling, String message, Object... args) {
        super(message, args);
        this.throttling = throttling;
        this.retryAfterMs = 0L;
    }

    /**
     * Constructs a throttle exception carrying an explicit server-supplied retry-after hint.
     *
     * @param throttling     {@code true} for a 429/503 throttle signal
     * @param retryAfterMs   server-suggested wait in milliseconds; 0 means absent
     * @param cause          underlying cause
     * @param message        format string
     * @param args           format arguments
     */
    public ExternalUnavailableException(boolean throttling, long retryAfterMs, Throwable cause, String message, Object... args) {
        super(cause, message, args);
        this.throttling = throttling;
        this.retryAfterMs = retryAfterMs > 0 ? retryAfterMs : 0L;
    }

    /**
     * Constructs a throttle exception carrying an explicit server-supplied retry-after hint (no cause).
     *
     * @param throttling     {@code true} for a 429/503 throttle signal
     * @param retryAfterMs   server-suggested wait in milliseconds; 0 means absent
     * @param message        format string
     * @param args           format arguments
     */
    public ExternalUnavailableException(boolean throttling, long retryAfterMs, String message, Object... args) {
        super(message, args);
        this.throttling = throttling;
        this.retryAfterMs = retryAfterMs > 0 ? retryAfterMs : 0L;
    }

    @Override
    public RestStatus status() {
        return RestStatus.SERVICE_UNAVAILABLE;
    }

    /**
     * {@code true} if this represents a throttling / slow-down response (HTTP 429 or 503), which a retry
     * policy may treat differently (longer budget, adaptive backoff) from a plain transient transport fault.
     */
    public boolean throttling() {
        return throttling;
    }

    /**
     * Server-supplied retry-after hint in milliseconds, or {@code 0} if the server sent no hint.
     * When non-zero, the retry policy uses this as the backoff delay instead of its computed value,
     * provided the hint fits within the remaining time budget.
     */
    public long retryAfterMs() {
        return retryAfterMs;
    }

    /**
     * Parses a {@code Retry-After} HTTP header value (integer seconds as sent by S3, Azure, and GCS)
     * to milliseconds. Returns {@code 0} if the value is absent, blank, non-positive, or unparseable
     * (the HTTP-date form is not supported — cloud stores use integer seconds in practice).
     */
    public static long parseRetryAfterMs(String headerValue) {
        if (headerValue == null || headerValue.isBlank()) {
            return 0L;
        }
        try {
            long seconds = Long.parseLong(headerValue.strip());
            return seconds > 0 ? seconds * 1000L : 0L;
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    /**
     * Whether an HTTP status returned by a remote store should be treated as a retryable transport
     * failure (a 503 on our side) rather than a client-class error. Limited to the transient codes a
     * retry can plausibly clear: 429 (throttling), 500, 502, 503 and 504. Other 5xx codes are
     * permanent — e.g. 501 (Not Implemented) and 505 (HTTP Version Not Supported) mean the operation
     * is unsupported, not temporarily unavailable — so they, like 4xx, fall through to a client-class
     * 400.
     */
    public static boolean isRetryableStatus(int httpStatus) {
        return httpStatus == 429 || httpStatus == 500 || httpStatus == 502 || httpStatus == 503 || httpStatus == 504;
    }

    /**
     * Whether a retryable status is specifically a throttling / back-pressure signal (429 or 503) as opposed to
     * a plain transient failure (500 / 502 / 504). Used to set {@link #throttling()} at the classification site.
     */
    public static boolean isThrottlingStatus(int httpStatus) {
        return httpStatus == 429 || httpStatus == 503;
    }
}
