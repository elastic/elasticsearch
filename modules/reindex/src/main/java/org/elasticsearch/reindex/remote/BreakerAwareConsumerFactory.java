/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.remote;

import org.apache.http.HttpResponse;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.common.breaker.CircuitBreaker;

import java.util.Objects;

/**
 * Creates one breaker-aware response consumer per remote reindex request attempt.
 */
final class BreakerAwareConsumerFactory implements HttpAsyncResponseConsumerFactory {

    /**
     * Mirrors {@code HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory.DEFAULT_BUFFER_LIMIT}
     * for known Content-Length responses.
     */
    static final int DEFAULT_KNOWN_CONTENT_LENGTH_BUFFER_LIMIT = 100 * 1024 * 1024;

    private final CircuitBreaker breaker;
    private final int knownContentLengthBufferLimitBytes;

    BreakerAwareConsumerFactory(CircuitBreaker breaker) {
        this(breaker, DEFAULT_KNOWN_CONTENT_LENGTH_BUFFER_LIMIT);
    }

    BreakerAwareConsumerFactory(CircuitBreaker breaker, int knownContentLengthBufferLimitBytes) {
        this.breaker = Objects.requireNonNull(breaker, "breaker");
        if (knownContentLengthBufferLimitBytes <= 0) {
            throw new IllegalArgumentException("knownContentLengthBufferLimitBytes must be > 0, was " + knownContentLengthBufferLimitBytes);
        }
        this.knownContentLengthBufferLimitBytes = knownContentLengthBufferLimitBytes;
    }

    @Override
    public HttpAsyncResponseConsumer<HttpResponse> createHttpAsyncResponseConsumer() {
        return new BreakerAwareHeapBufferedAsyncResponseConsumer(breaker, knownContentLengthBufferLimitBytes);
    }
}
