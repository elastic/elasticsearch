/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.CloseableConnection;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportMessageListener;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportStats;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A transport that does nothing. Normally wrapped by {@link StubbableTransport}.
 */
public class FakeTransport extends AbstractLifecycleComponent implements Transport {

    private final RequestHandlers requestHandlers = new RequestHandlers();
    private final ResponseHandlers responseHandlers = new ResponseHandlers();
    private TransportMessageListener listener;

    @Override
    public void setMessageListener(TransportMessageListener listener) {
        if (this.listener != null) {
            throw new IllegalStateException("listener already set");
        }
        this.listener = listener;
    }

    @Override
    public BoundTransportAddress boundAddress() {
        return null;
    }

    @Override
    public Map<String, BoundTransportAddress> profileBoundAddresses() {
        return null;
    }

    @Override
    public TransportAddress[] addressesFromString(String address) {
        return new TransportAddress[0];
    }

    @Override
    public List<String> getDefaultSeedAddresses() {
        return Collections.emptyList();
    }

    @Override
    public void openConnection(DiscoveryNode node, ConnectionProfile profile, ActionListener<Connection> listener) {
        listener.onResponse(new CloseableConnection() {
            @Override
            public DiscoveryNode getNode() {
                return node;
            }

            @Override
            public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options) {

            }
        });
    }

    @Override
    public TransportStats getStats() {
        return null;
    }

    @Override
    public ResponseHandlers getResponseHandlers() {
        return responseHandlers;
    }

    @Override
    public RequestHandlers getRequestHandlers() {
        return requestHandlers;
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() {

    }
}
