/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

public class TransportRequestOptions {

    @Nullable
    private final TimeValue timeout;
    private final Type type;
    @Nullable
    private final CircuitBreaker circuitBreaker;

    public static TransportRequestOptions timeout(@Nullable TimeValue timeout) {
        return of(timeout, Type.REG);
    }

    public static TransportRequestOptions of(@Nullable TimeValue timeout, Type type) {
        return of(timeout, type, null);
    }

    public static TransportRequestOptions of(@Nullable TimeValue timeout, Type type, @Nullable CircuitBreaker circuitBreaker) {
        if (timeout == null && type == Type.REG && circuitBreaker == null) {
            return EMPTY;
        }
        return new TransportRequestOptions(timeout, type, circuitBreaker);
    }

    private TransportRequestOptions(@Nullable TimeValue timeout, Type type, @Nullable CircuitBreaker circuitBreaker) {
        this.timeout = timeout;
        this.type = type;
        this.circuitBreaker = circuitBreaker;
    }

    @Nullable
    public TimeValue timeout() {
        return this.timeout;
    }

    public Type type() {
        return this.type;
    }

    /**
     * Charged for the serialized request until it has been written to the network, so that a large request waiting in outbound
     * buffers fails with a {@link CircuitBreakingException}, wrapped in a {@link SendRequestTransportException}, instead of running
     * the node out of memory. {@code null} charges nothing. Connections that don't serialize, such as the local-node connection, don't
     * charge it; for a {@link BytesTransportRequest}, only the header is charged.
     */
    @Nullable
    public CircuitBreaker circuitBreaker() {
        return this.circuitBreaker;
    }

    public static final TransportRequestOptions EMPTY = new TransportRequestOptions(null, Type.REG, null);

    public enum Type {
        RECOVERY,
        BULK,
        REG,
        STATE,
        PING
    }
}
