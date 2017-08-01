/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport;

import org.elasticsearch.cluster.node.DiscoveryNode;

import java.util.function.Supplier;

/**
 * This interface allows plugins to intercept requests on both the sender and the receiver side.
 */
public interface TransportInterceptor {
    /**
     * This is called for each handler that is registered via
     * {@link TransportService#registerRequestHandler(String, Supplier, String, boolean, boolean, TransportRequestHandler)} or
     * {@link TransportService#registerRequestHandler(String, Supplier, String, TransportRequestHandler)}. The returned handler is
     * used instead of the passed in handler. By default the provided handler is returned.
     */
    default <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(String action, String executor,
                                                                                     boolean forceExecution,
                                                                                     TransportRequestHandler<T> actualHandler) {
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
        <T extends TransportResponse> void sendRequest(Transport.Connection connection, String action,
                                                       TransportRequest request, TransportRequestOptions options,
                                                       TransportResponseHandler<T> handler);
    }
}
