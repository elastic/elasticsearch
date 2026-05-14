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

/**
 * Utility for sending requests over the transport protocol to nodes in the cluster.
 */
public interface RemoteTransportClient {

    /**
     * Send a transport request to a node.
     *
     * @param node The node to send the request to
     * @param action The transport action name to execute on the node
     * @param request The request object to send
     * @param handler A handler for the success/failure of the request
     * @param <T> The response type expected from the transport action
     */
    <T extends TransportResponse> void sendRequest(
        DiscoveryNode node,
        String action,
        TransportRequest request,
        TransportResponseHandler<T> handler
    );
}
