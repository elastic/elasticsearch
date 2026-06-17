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
 * {@link HttpAsyncResponseConsumerFactory} for reindex-from-remote that produces a
 * {@link BreakerAwareHeapBufferedAsyncResponseConsumer} per request attempt. Apache HC calls
 * {@link #createHttpAsyncResponseConsumer()} once per attempt (including retries), so each
 * attempt independently reserves and releases its buffer bytes against the REQUEST circuit breaker.
 */
final class BreakerAwareConsumerFactory implements HttpAsyncResponseConsumerFactory {

    /** Matches the upstream default in {@code HttpAsyncResponseConsumerFactory.DEFAULT_BUFFER_LIMIT}. */
    static final int DEFAULT_BUFFER_LIMIT_BYTES = 100 * 1024 * 1024;

    private final CircuitBreaker breaker;
    private final int bufferLimitBytes;

    BreakerAwareConsumerFactory(CircuitBreaker breaker) {
        this(breaker, DEFAULT_BUFFER_LIMIT_BYTES);
    }

    /** Package-private for tests to supply a smaller buffer limit. */
    BreakerAwareConsumerFactory(CircuitBreaker breaker, int bufferLimitBytes) {
        this.breaker = Objects.requireNonNull(breaker, "breaker");
        if (bufferLimitBytes <= 0) {
            throw new IllegalArgumentException("bufferLimitBytes must be > 0, was " + bufferLimitBytes);
        }
        this.bufferLimitBytes = bufferLimitBytes;
    }

    @Override
    public HttpAsyncResponseConsumer<HttpResponse> createHttpAsyncResponseConsumer() {
        return new BreakerAwareHeapBufferedAsyncResponseConsumer(breaker, bufferLimitBytes);
    }
}
