/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.io.IOException;

/**
 * Signals a transient transport-level failure while talking to object storage — a connection reset, a
 * premature end of the response body, a read timeout, an aborted stream, or a retryable server response
 * (e.g. HTTP 500/503/429). The operation can be safely retried, and a partially-read byte range can be
 * re-opened from the next undelivered byte and resumed.
 * <p>
 * This is the typed signal that lets the generic retry machinery ({@code RetryPolicy},
 * {@code RetryableStorageObject}) decide to retry <b>by type</b> rather than by sniffing exception
 * messages. The knowledge of which concrete provider/SDK exceptions are transient lives in the storage
 * provider (which has the concrete types and HTTP status codes); the provider classifies its own failures
 * and raises this exception, and the retry layer simply reacts to the type.
 * <p>
 * It is deliberately <em>not</em> raised for genuine, non-retryable conditions — missing objects,
 * permission denials, malformed requests, or data/parse errors — which must surface unchanged so they are
 * not retried.
 *
 * @see #throttling() whether the failure was a throttling/slow-down response, which retry policies may
 *      grant a larger retry budget
 */
public class TransientStorageException extends IOException {

    private final boolean throttling;

    public TransientStorageException(String message, Throwable cause) {
        this(message, cause, false);
    }

    public TransientStorageException(String message, Throwable cause, boolean throttling) {
        super(message, cause);
        this.throttling = throttling;
    }

    /**
     * {@code true} if this represents a throttling / slow-down response (e.g. HTTP 429/503), which a retry
     * policy may back off from more aggressively and grant a larger retry budget than a plain transient
     * transport fault.
     */
    public boolean throttling() {
        return throttling;
    }
}
