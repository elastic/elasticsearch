/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.Writeable.Reader;

/**
 * This interface allows plugins to intercept requests on both the sender and the receiver side.
 */
public interface TransportInterceptor {
    /**
     * This is called for each handler that is registered via
     * {@link TransportService#registerRequestHandler(String, String, boolean, boolean, Reader, TransportRequestHandler)} or
     * {@link TransportService#registerRequestHandler(String, String, Reader, TransportRequestHandler)}. The returned handler is
     * used instead of the passed in handler. By default the provided handler is returned.
     */
    default <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
        String action,
        String executor,
        boolean forceExecution,
        TransportRequestHandler<T> actualHandler
    ) {
        return actualHandler;
    }

    /**
     * This is called up-front providing the actual low level {@link AsyncSender} that performs the low level send request.
     * The returned sender is used to send all requests that come in via
     * {@link TransportService#sendRequest(DiscoveryNode, String, TransportRequest, TransportResponseHandler)} or
     * {@link TransportService#sendRequest(DiscoveryNode, String, TransportRequest, TransportRequestOptions, TransportResponseHandler)}.
     * This allows plugins to perform actions on each send request including modifying the request context etc.
     */
    default AsyncSender interceptSender(AsyncSender sender) {
        return sender;
    }

    /**
     * A simple interface to decorate
     * {@link #sendRequest(Transport.Connection, String, TransportRequest, TransportRequestOptions, TransportResponseHandler)}
     */
    interface AsyncSender {
        <T extends TransportResponse> void sendRequest(
            Transport.Connection connection,
            String action,
            TransportRequest request,
            TransportRequestOptions options,
            TransportResponseHandler<T> handler
        );
    }
}
