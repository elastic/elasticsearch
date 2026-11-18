/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Nullable;

/**
 * A transport channel allows to send a response to a request on the channel.
 */
public interface TransportChannel {

    String getProfileName();

    void sendResponse(TransportResponse response);

    /**
     * Sends the response and charges its serialized bytes to {@code circuitBreaker} until they are written. If the breaker trips,
     * the caller gets the {@link org.elasticsearch.common.breaker.CircuitBreakingException} instead. Channels that don't serialize,
     * such as the local direct channel, ignore the breaker. For a pre-serialized {@link BytesTransportResponse}, only the message
     * header is charged, since the caller already holds its bytes.
     */
    default void sendResponse(TransportResponse response, @Nullable CircuitBreaker circuitBreaker) {
        sendResponse(response);
    }

    void sendResponse(Exception exception);

    /**
     * Returns the version of the data to communicate in this channel.
     */
    default TransportVersion getVersion() {
        return TransportVersion.current();
    }
}
