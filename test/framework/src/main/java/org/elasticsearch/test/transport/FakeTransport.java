/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.transport;

import org.elasticsearch.TransportVersion;
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
    public void setMessageListener(TransportMessageListener messageListener) {
        if (this.listener != null) {
            throw new IllegalStateException("listener already set");
        }
        this.listener = messageListener;
    }

    @Override
    public BoundTransportAddress boundAddress() {
        return null;
    }

    @Override
    public BoundTransportAddress boundRemoteIngressAddress() {
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
    public void openConnection(DiscoveryNode node, ConnectionProfile profile, ActionListener<Connection> actionListener) {
        actionListener.onResponse(new CloseableConnection() {
            @Override
            public DiscoveryNode getNode() {
                return node;
            }

            @Override
            public TransportVersion getTransportVersion() {
                return TransportVersion.current();
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
