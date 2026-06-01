/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.rest.RestStatus;

/**
 * A retryable transport failure talking to the remote store backing an external data source — the
 * store returned a 5xx, throttled us, or the connection timed out / was reset. Maps to
 * {@code 503 Service Unavailable}: the read might succeed on retry, so it is neither a permanent
 * client error nor a cluster bug.
 * <p>
 * Permanent transport outcomes (object not found, a malformed response) are not raised here; those
 * are client-class and surface as {@link ExternalClientException}.
 */
public final class ExternalUnavailableException extends ExternalException {

    public ExternalUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }

    public ExternalUnavailableException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
    }

    public ExternalUnavailableException(String message, Object... args) {
        super(message, args);
    }

    @Override
    public RestStatus status() {
        return RestStatus.SERVICE_UNAVAILABLE;
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
}
