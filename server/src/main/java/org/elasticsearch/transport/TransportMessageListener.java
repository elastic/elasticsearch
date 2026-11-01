/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.transport;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.Nullable;

import java.util.List;

public interface TransportMessageListener {

    TransportMessageListener NOOP_LISTENER = new TransportMessageListener() {};

    /**
     * Called once a request is received
     * @param requestId the internal request ID
     * @param action the request action
     * @param networkMessageSize the size of the network message in bytes, including the transport header, or {@code -1} if the request
     *                           was not sent over the network, i.e. it was sent by the local node to itself
     */
    default void onRequestReceived(long requestId, String action, int networkMessageSize) {}

    /**
     * Called for every action response sent after the response has been passed to the underlying network implementation.
     * @param requestId the request ID (unique per client)
     * @param action the request action
     * @param networkMessageSize the size of the network message in bytes, including the transport header, or {@code -1} if the response
     *                           is not sent over the network, i.e. it is a response to a request that the local node sent to itself
     */
    default void onResponseSent(long requestId, String action, int networkMessageSize) {}

    /**
     * Called for every failed action response after the response has been passed to the underlying network implementation.
     * @param requestId the request ID (unique per client)
     * @param action the request action
     * @param error the error sent back to the caller
     * @param networkMessageSize the size of the network message in bytes, including the transport header, or {@code -1} if the response
     *                           is not sent over the network, i.e. it is a response to a request that the local node sent to itself
     */
    default void onResponseSent(long requestId, String action, Exception error, int networkMessageSize) {}

    /**
     * Called for every request sent to a server after the request has been passed to the underlying network implementation
     * @param node the node the request was sent to
     * @param requestId the internal request id
     * @param action the action name
     * @param request the actual request
     * @param finalOptions the request options
     * @param networkMessageSize the size of the network message in bytes, including the transport header, or {@code -1} if the request
     *                           was not sent over the network, i.e. it is sent by the local node to itself, or could not be serialized
     */
    default void onRequestSent(
        DiscoveryNode node,
        long requestId,
        String action,
        TransportRequest request,
        TransportRequestOptions finalOptions,
        int networkMessageSize
    ) {}

    /**
     * Called for every response received
     * @param requestId the request id for this response
     * @param context the response context or null if the context was already processed ie. due to a timeout.
     * @param networkMessageSize the size of the network message in bytes, including the transport header, or {@code -1} if the request
     *                           was not sent over the network, i.e. it is sent by the local node to itself, or could not be serialized
     */
    @SuppressWarnings("rawtypes")
    default void onResponseReceived(long requestId, @Nullable Transport.ResponseContext context, int networkMessageSize) {}

    /**
     * SPI for plugins to contribute {@link TransportMessageListener} instances.
     */
    interface Provider {

        /**
         * @return the {@link TransportMessageListener} instances to register on this node
         */
        List<TransportMessageListener> create();
    }
}
