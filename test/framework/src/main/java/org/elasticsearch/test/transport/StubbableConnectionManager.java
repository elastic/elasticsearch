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

public class StubbableConnectionManager extends ConnectionManager {

    private final ConnectionManager delegate;
    private final ConcurrentMap<TransportAddress, GetConnectionBehavior> getConnectionBehaviors;
    private final ConcurrentMap<TransportAddress, NodeConnectedBehavior> nodeConnectedBehaviors;
    private volatile GetConnectionBehavior defaultGetConnectionBehavior = ConnectionManager::getConnection;
    private volatile NodeConnectedBehavior defaultNodeConnectedBehavior = ConnectionManager::nodeConnected;

    public StubbableConnectionManager(ConnectionManager delegate, Settings settings, Transport transport, ThreadPool threadPool) {
        super(settings, transport, threadPool);
        this.delegate = delegate;
        this.getConnectionBehaviors = new ConcurrentHashMap<>();
        this.nodeConnectedBehaviors = new ConcurrentHashMap<>();
    }

    public boolean addConnectBehavior(TransportAddress transportAddress, GetConnectionBehavior connectBehavior) {
        return getConnectionBehaviors.put(transportAddress, connectBehavior) == null;
    }

    public boolean setDefaultGetConnectionBehavior(GetConnectionBehavior behavior) {
        GetConnectionBehavior prior = defaultGetConnectionBehavior;
        defaultGetConnectionBehavior = behavior;
        return prior == null;
    }

    public boolean addNodeConnectedBehavior(TransportAddress transportAddress, NodeConnectedBehavior behavior) {
        return nodeConnectedBehaviors.put(transportAddress, behavior) == null;
    }

    public boolean setDefaultNodeConnectedBehavior(NodeConnectedBehavior behavior) {
        NodeConnectedBehavior prior = defaultNodeConnectedBehavior;
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
    public Transport.Connection openConnection(DiscoveryNode node, ConnectionProfile connectionProfile) {
        return delegate.openConnection(node, connectionProfile);
    }

    @Override
    public Transport.Connection getConnection(DiscoveryNode node) {
        TransportAddress address = node.getAddress();
        GetConnectionBehavior behavior = getConnectionBehaviors.getOrDefault(address, defaultGetConnectionBehavior);
        return behavior.getConnection(delegate, node);
    }

    @Override
    public boolean nodeConnected(DiscoveryNode node) {
        TransportAddress address = node.getAddress();
        NodeConnectedBehavior behavior = nodeConnectedBehaviors.getOrDefault(address, defaultNodeConnectedBehavior);
        return behavior.nodeConnected(delegate, node);
    }

    @Override
    public void addListener(TransportConnectionListener listener) {
        delegate.addListener(listener);
    }

    @Override
    public void removeListener(TransportConnectionListener listener) {
        delegate.removeListener(listener);
    }

    @Override
    public void connectToNode(DiscoveryNode node, ConnectionProfile connectionProfile,
                              CheckedBiConsumer<Transport.Connection, ConnectionProfile, IOException> connectionValidator)
        throws ConnectTransportException {
        delegate.connectToNode(node, connectionProfile, connectionValidator);
    }

    @Override
    public void disconnectFromNode(DiscoveryNode node) {
        delegate.disconnectFromNode(node);
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @FunctionalInterface
    public interface GetConnectionBehavior {
        Transport.Connection getConnection(ConnectionManager connectionManager, DiscoveryNode discoveryNode);
    }

    @FunctionalInterface
    public interface NodeConnectedBehavior {
        boolean nodeConnected(ConnectionManager connectionManager, DiscoveryNode discoveryNode);
    }
}
