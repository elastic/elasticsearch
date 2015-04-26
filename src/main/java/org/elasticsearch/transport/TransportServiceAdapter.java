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

/**
 *
 */
public interface TransportServiceAdapter {

    void received(long size);

    void sent(long size);

    /** called by the {@link Transport} implementation once a request has been sent */
    void onRequestSent(DiscoveryNode node, long requestId, String action, TransportRequest request, TransportRequestOptions options);

    /** called by the {@link Transport) implementation once a response was sent to calling node */
    void onResponseSent(long requestId, String action, TransportResponse response, TransportResponseOptions options);

    /** called by the {@link Transport) implementation after an exception was sent as a response to an incoming request */
    void onResponseSent(long requestId, String action, Throwable t);

    /**
     * called by the {@link Transport) implementation when a response or an exception has been recieved for a previously
     * sent request (before any processing or deserialization was done). Returns the appropriate response handler or null if not
     * found.
     */
    TransportResponseHandler onResponseReceived(long requestId);

    /**
     * called by the {@link Transport) implementation when an incoming request arrives but before
     * any parsing of it has happened (with the exception of the requestId and action)
     */
    void onRequestReceived(long requestId, String action);

    RequestHandlerRegistry getRequestHandler(String action);

    void raiseNodeConnected(DiscoveryNode node);

    void raiseNodeDisconnected(DiscoveryNode node);
}
