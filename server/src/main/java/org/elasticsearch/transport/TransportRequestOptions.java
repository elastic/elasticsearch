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

    /**
     * @param circuitBreaker charged for the serialized request until it has been written to the network, see {@link #circuitBreaker()}
     */
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
     * The breaker charged for the serialized bytes of the request until they have been written to the network, or {@code null} if they
     * are not charged. The bytes of a large request sent to many nodes stay on the heap until each copy is written, which can take far
     * longer than serializing it, so callers that send large requests can use this to fail them instead of running out of memory.
     * <p>
     * If the breaker trips while the request is serialized, the request is not sent and its response handler fails with a
     * {@link SendRequestTransportException} caused by a {@link CircuitBreakingException}. Requests to the local node are not serialized
     * and are not charged. For a pre-serialized {@link BytesTransportRequest}, only the message header is charged, since the caller
     * already holds its bytes.
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
