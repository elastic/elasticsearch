/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.rest.RestStatus;

/**
 * Raised when this node's own external-source admission control (the per-query concurrency budget
 * or the node-global concurrency limiter) gave up waiting for a permit: no permit was released for
 * a full timeout window, so the pool is genuinely stalled or over-subscribed beyond what queueing
 * can absorb. Maps to {@code 429 Too Many Requests}: the cluster is healthy and the request was
 * well-formed — there is simply too much concurrent demand right now, and the client should back
 * off and retry. Surfacing this as 429 (rather than 500) keeps admission-control pushback
 * distinguishable from bugs in our own reading code.
 * <p>
 * Not to be confused with the {@code throttling} flag on {@link ExternalUnavailableException},
 * which marks the <em>remote store</em> throttling us; this exception is local back-pressure.
 */
public final class ExternalThrottledException extends ExternalException {

    public ExternalThrottledException(String message, Throwable cause) {
        super(message, cause);
    }

    public ExternalThrottledException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
    }

    public ExternalThrottledException(String message, Object... args) {
        super(message, args);
    }

    @Override
    public RestStatus status() {
        return RestStatus.TOO_MANY_REQUESTS;
    }
}
