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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionManager;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportConnectionListener;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class StubbableConnectionManager extends ConnectionManager {

    private final ConnectionManager delegate;
    private final ConcurrentMap<TransportAddress, GetConnectionBehavior> getConnectionBehaviors;
    private volatile GetConnectionBehavior defaultGetConnectionBehavior = ConnectionManager::getConnection;
    private volatile NodeConnectedBehavior defaultNodeConnectedBehavior = ConnectionManager::connectedNodes;

    public StubbableConnectionManager(ConnectionManager delegate, Settings settings, Transport transport) {
        super(settings, transport);
        this.delegate = delegate;
        this.getConnectionBehaviors = new ConcurrentHashMap<>();
    }

    public boolean addConnectBehavior(TransportAddress transportAddress, GetConnectionBehavior connectBehavior) {
        return getConnectionBehaviors.put(transportAddress, connectBehavior) == null;
    }

    public boolean setDefaultGetConnectionBehavior(GetConnectionBehavior behavior) {
        GetConnectionBehavior prior = defaultGetConnectionBehavior;
        defaultGetConnectionBehavior = behavior;
        return prior == null;
    }

    public boolean setDefaultNodeConnectedBehavior(NodeConnectedBehavior behavior) {
        NodeConnectedBehavior prior = defaultNodeConnectedBehavior;
        defaultNodeConnectedBehavior = behavior;
        return prior == null;
    }

    public void clearBehaviors() {
        defaultGetConnectionBehavior = ConnectionManager::getConnection;
        getConnectionBehaviors.clear();
        defaultNodeConnectedBehavior = ConnectionManager::connectedNodes;
    }

    public void clearBehavior(TransportAddress transportAddress) {
        getConnectionBehaviors.remove(transportAddress);
    }

    @Override
    public void openConnection(DiscoveryNode node, ConnectionProfile connectionProfile, ActionListener<Transport.Connection> listener) {
        delegate.openConnection(node, connectionProfile, listener);
    }

    @Override
    public Transport.Connection getConnection(DiscoveryNode node) {
        TransportAddress address = node.getAddress();
        GetConnectionBehavior behavior = getConnectionBehaviors.getOrDefault(address, defaultGetConnectionBehavior);
        return behavior.getConnection(delegate, node);
    }

    @Override
    public boolean nodeConnected(DiscoveryNode node) {
        return defaultNodeConnectedBehavior.connectedNodes(delegate).contains(node);
    }

    @Override
    public Set<DiscoveryNode> connectedNodes() {
        return defaultNodeConnectedBehavior.connectedNodes(delegate);
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
                              ConnectionValidator connectionValidator, ActionListener<Void> listener)
        throws ConnectTransportException {
        delegate.connectToNode(node, connectionProfile, connectionValidator, listener);
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
        Set<DiscoveryNode> connectedNodes(ConnectionManager connectionManager);
    }
}
