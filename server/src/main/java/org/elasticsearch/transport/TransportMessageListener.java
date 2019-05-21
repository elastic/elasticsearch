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

public interface TransportMessageListener {

    TransportMessageListener NOOP_LISTENER = new TransportMessageListener() {};

    /**
     * Called once a request is received
     * @param requestId the internal request ID
     * @param action the request action
     *
     */
    default void onRequestReceived(long requestId, String action) {}

    /**
     * Called for every action response sent after the response has been passed to the underlying network implementation.
     * @param requestId the request ID (unique per client)
     * @param action the request action
     * @param response the response send
     */
    default void onResponseSent(long requestId, String action, TransportResponse response) {}

    /***
     * Called for every failed action response after the response has been passed to the underlying network implementation.
     * @param requestId the request ID (unique per client)
     * @param action the request action
     * @param error the error sent back to the caller
     */
    default void onResponseSent(long requestId, String action, Exception error) {}

    /**
     * Called for every request sent to a server after the request has been passed to the underlying network implementation
     * @param node the node the request was sent to
     * @param requestId the internal request id
     * @param action the action name
     * @param request the actual request
     * @param finalOptions the request options
     */
    default void onRequestSent(DiscoveryNode node, long requestId, String action, TransportRequest request,
                               TransportRequestOptions finalOptions) {}

    /**
     * Called for every response received
     * @param requestId the request id for this reponse
     * @param context the response context or null if the context was already processed ie. due to a timeout.
     */
    default void onResponseReceived(long requestId, Transport.ResponseContext context) {}
}
