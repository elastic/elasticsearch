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
import org.elasticsearch.core.Releasable;

/**
 * A transport channel allows to send a response to a request on the channel.
 */
public interface TransportChannel {

    String getProfileName();

    void sendResponse(TransportResponse response);

    void sendResponse(Exception exception);

    /**
     * Sends a response with an associated releasable that will be released after the response
     * has been fully written to the network. This is useful for deferring circuit breaker
     * release until the response bytes have actually been sent, preventing memory accumulation
     * when responses are queued faster than they can be transmitted.
     *
     * @param response the response to send
     * @param onSendComplete released after the response is written to the network, not just queued.
     *                       For non-TCP channels, this may be released immediately after queuing.
     */
    default void sendResponse(TransportResponse response, Releasable onSendComplete) {
        try (onSendComplete) {
            sendResponse(response);
        }
    }

    /**
     * Returns the version of the data to communicate in this channel.
     */
    default TransportVersion getVersion() {
        return TransportVersion.current();
    }
}
