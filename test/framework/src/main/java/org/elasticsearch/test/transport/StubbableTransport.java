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

package org.elasticsearch.test.transport;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.RequestHandlerRegistry;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportMessageListener;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportStats;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class StubbableTransport implements Transport {

    private final ConcurrentHashMap<TransportAddress, SendRequestBehavior> sendBehaviors = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<TransportAddress, OpenConnectionBehavior> connectBehaviors = new ConcurrentHashMap<>();
    private volatile SendRequestBehavior defaultSendRequest = null;
    private volatile OpenConnectionBehavior defaultConnectBehavior = null;
    private final Transport delegate;


    public StubbableTransport(Transport transport) {
        this.delegate = transport;
    }

    boolean setDefaultSendBehavior(SendRequestBehavior sendBehavior) {
        SendRequestBehavior prior = defaultSendRequest;
        defaultSendRequest = sendBehavior;
        return prior == null;
    }

    public boolean setDefaultConnectBehavior(OpenConnectionBehavior openConnectionBehavior) {
        OpenConnectionBehavior prior = this.defaultConnectBehavior;
        this.defaultConnectBehavior = openConnectionBehavior;
        return prior == null;
    }

    boolean addSendBehavior(TransportAddress transportAddress, SendRequestBehavior sendBehavior) {
        return sendBehaviors.put(transportAddress, sendBehavior) == null;
    }

    boolean addConnectBehavior(TransportAddress transportAddress, OpenConnectionBehavior connectBehavior) {
        return connectBehaviors.put(transportAddress, connectBehavior) == null;
    }

    void clearBehaviors() {
        this.defaultSendRequest = null;
        sendBehaviors.clear();
        this.defaultConnectBehavior = null;
        connectBehaviors.clear();
    }

    void clearBehavior(TransportAddress transportAddress) {
        SendRequestBehavior behavior = sendBehaviors.remove(transportAddress);
        if (behavior != null) {
            behavior.clearCallback();
        }
        OpenConnectionBehavior openConnectionBehavior = connectBehaviors.remove(transportAddress);
        if (openConnectionBehavior != null) {
            openConnectionBehavior.clearCallback();
        }
    }

    Transport getDelegate() {
        return delegate;
    }

    @Override
    public void setMessageListener(TransportMessageListener listener) {
        delegate.setMessageListener(listener);
    }

    @Override
    public void setLocalNode(DiscoveryNode localNode) {
        delegate.setLocalNode(localNode);
    }

    @Override
    public <Request extends TransportRequest> void registerRequestHandler(RequestHandlerRegistry<Request> reg) {
        delegate.registerRequestHandler(reg);
    }

    @Override
    public RequestHandlerRegistry getRequestHandler(String action) {
        return delegate.getRequestHandler(action);
    }

    @Override
    public BoundTransportAddress boundAddress() {
        return delegate.boundAddress();
    }

    @Override
    public TransportAddress[] addressesFromString(String address) throws UnknownHostException {
        return delegate.addressesFromString(address);
    }

    @Override
    public List<String> getDefaultSeedAddresses() {
        return delegate.getDefaultSeedAddresses();
    }

    @Override
    public void openConnection(DiscoveryNode node, ConnectionProfile profile, ActionListener<Connection> listener) {
        TransportAddress address = node.getAddress();
        OpenConnectionBehavior behavior = connectBehaviors.getOrDefault(address, defaultConnectBehavior);

        ActionListener<Connection> wrappedListener =
            ActionListener.delegateFailure(listener,
                (delegatedListener, connection) -> delegatedListener.onResponse(new WrappedConnection(connection)));

        if (behavior == null) {
            delegate.openConnection(node, profile, wrappedListener);
        } else {
            behavior.openConnection(delegate, node, profile, wrappedListener);
        }
    }

    @Override
    public TransportStats getStats() {
        return delegate.getStats();
    }

    @Override
    public Transport.ResponseHandlers getResponseHandlers() {
        return delegate.getResponseHandlers();
    }

    @Override
    public Lifecycle.State lifecycleState() {
        return delegate.lifecycleState();
    }

    @Override
    public void addLifecycleListener(LifecycleListener listener) {
        delegate.addLifecycleListener(listener);
    }

    @Override
    public void removeLifecycleListener(LifecycleListener listener) {
        delegate.removeLifecycleListener(listener);
    }

    @Override
    public void start() {
        delegate.start();
    }

    @Override
    public void stop() {
        delegate.stop();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public Map<String, BoundTransportAddress> profileBoundAddresses() {
        return delegate.profileBoundAddresses();
    }

    public class WrappedConnection implements Transport.Connection {

        private final Transport.Connection connection;

        private WrappedConnection(Transport.Connection connection) {
            this.connection = connection;
        }

        @Override
        public DiscoveryNode getNode() {
            return connection.getNode();
        }

        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
            throws IOException, TransportException {
            TransportAddress address = connection.getNode().getAddress();
            SendRequestBehavior behavior = sendBehaviors.getOrDefault(address, defaultSendRequest);
            if (behavior == null) {
                connection.sendRequest(requestId, action, request, options);
            } else {
                behavior.sendRequest(connection, requestId, action, request, options);
            }
        }

        @Override
        public void addCloseListener(ActionListener<Void> listener) {
            connection.addCloseListener(listener);
        }


        @Override
        public boolean isClosed() {
            return connection.isClosed();
        }

        @Override
        public Version getVersion() {
            return connection.getVersion();
        }

        @Override
        public Object getCacheKey() {
            return connection.getCacheKey();
        }

        @Override
        public void close() {
            connection.close();
        }

        public Transport.Connection getConnection() {
            return connection;
        }
    }

    @FunctionalInterface
    public interface OpenConnectionBehavior {

        void openConnection(Transport transport, DiscoveryNode discoveryNode, ConnectionProfile profile,
                            ActionListener<Connection> listener);

        default void clearCallback() {}
    }

    @FunctionalInterface
    public interface SendRequestBehavior {
        void sendRequest(Connection connection, long requestId, String action, TransportRequest request,
                         TransportRequestOptions options) throws IOException;

        default void clearCallback() {
        }
    }
}
