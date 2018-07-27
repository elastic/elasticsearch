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

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionManager;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportConnectionListener;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MockConnectionManager extends ConnectionManager {

    private final ConnectionManager delegate;
    private final ConcurrentMap<TransportAddress, MockTransportService.GetConnectionBehavior> getConnectionBehaviors;
    private final ConcurrentMap<TransportAddress, MockTransportService.NodeConnectedBehavior> nodeConnectedBehaviors;
    private volatile MockTransportService.GetConnectionBehavior defaultGetConnectionBehavior = null;
    private volatile MockTransportService.NodeConnectedBehavior defaultNodeConnectedBehavior = null;

    public MockConnectionManager(ConnectionManager delegate, Settings settings, Transport transport, ThreadPool threadPool) {
        super(settings, transport, threadPool);
        this.delegate = delegate;
        this.getConnectionBehaviors = new ConcurrentHashMap<>();
        this.nodeConnectedBehaviors = new ConcurrentHashMap<>();
    }

    public boolean addConnectBehavior(TransportAddress transportAddress, MockTransportService.GetConnectionBehavior connectBehavior) {
        return getConnectionBehaviors.put(transportAddress, connectBehavior) == null;
    }

    public boolean setDefaultConnectBehavior(MockTransportService.GetConnectionBehavior behavior) {
        MockTransportService.GetConnectionBehavior prior = defaultGetConnectionBehavior;
        defaultGetConnectionBehavior = behavior;
        return prior == null;
    }

    public boolean addNodeConnectedBehavior(TransportAddress transportAddress, MockTransportService.NodeConnectedBehavior behavior) {
        return nodeConnectedBehaviors.put(transportAddress, behavior) == null;
    }

    public boolean setDefaultNodeConnectedBehavior(MockTransportService.NodeConnectedBehavior behavior) {
        MockTransportService.NodeConnectedBehavior prior = defaultNodeConnectedBehavior;
        defaultNodeConnectedBehavior = behavior;
        return prior == null;
    }

    public void clearBehaviors() {
        getConnectionBehaviors.clear();
        nodeConnectedBehaviors.clear();
    }

    public void clearBehavior(TransportAddress transportAddress) {
        getConnectionBehaviors.remove(transportAddress);
        nodeConnectedBehaviors.remove(transportAddress);
    }

    @Override
    public Transport.Connection getConnection(DiscoveryNode node) {
        TransportAddress address = node.getAddress();
        MockTransportService.GetConnectionBehavior behavior = getConnectionBehaviors.getOrDefault(address, defaultGetConnectionBehavior);
        if (behavior == null) {
            return delegate.getConnection(node);
        } else {
            return behavior.getConnection(delegate, node);
        }
    }

    public boolean nodeConnected(DiscoveryNode node) {
        TransportAddress address = node.getAddress();
        MockTransportService.NodeConnectedBehavior behavior = nodeConnectedBehaviors.getOrDefault(address, defaultNodeConnectedBehavior);
        if (behavior == null) {
            return delegate.nodeConnected(node);
        } else {
            return behavior.nodeConnected(delegate, node);
        }
    }

    @Override
    public void addListener(TransportConnectionListener listener) {
        delegate.addListener(listener);
    }

    @Override
    public void removeListener(TransportConnectionListener listener) {
        delegate.removeListener(listener);
    }

    public void connectToNode(DiscoveryNode node, ConnectionProfile connectionProfile,
                              CheckedBiConsumer<Transport.Connection, ConnectionProfile, IOException> connectionValidator)
        throws ConnectTransportException {
        delegate.connectToNode(node, connectionProfile, connectionValidator);
    }

    public void disconnectFromNode(DiscoveryNode node) {
        delegate.disconnectFromNode(node);
    }

    public int connectedNodeCount() {
        return delegate.connectedNodeCount();
    }

    @Override
    public void close() {
        delegate.close();
    }
}
