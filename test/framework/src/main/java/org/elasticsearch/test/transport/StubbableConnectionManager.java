/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.ConnectionManager;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportConnectionListener;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class StubbableConnectionManager implements ConnectionManager {

    private final ConnectionManager delegate;
    private final ConcurrentMap<TransportAddress, GetConnectionBehavior> getConnectionBehaviors;
    private volatile GetConnectionBehavior defaultGetConnectionBehavior = ConnectionManager::getConnection;
    private volatile NodeConnectedBehavior defaultNodeConnectedBehavior = ConnectionManager::nodeConnected;

    public StubbableConnectionManager(ConnectionManager delegate) {
        this.delegate = delegate;
        this.getConnectionBehaviors = new ConcurrentHashMap<>();
    }

    public boolean addGetConnectionBehavior(TransportAddress transportAddress, GetConnectionBehavior connectBehavior) {
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
        return defaultNodeConnectedBehavior.connectedNodes(delegate, node);
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
    public Set<DiscoveryNode> getAllConnectedNodes() {
        return delegate.getAllConnectedNodes();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void closeNoBlock() {
        delegate.closeNoBlock();
    }

    @Override
    public ConnectionProfile getConnectionProfile() {
        return delegate.getConnectionProfile();
    }

    @Override
    public void setIsDeleting(DiscoveryNode discoveryNode) {
        this.delegate.setIsDeleting(discoveryNode);
    }

    @FunctionalInterface
    public interface GetConnectionBehavior {
        Transport.Connection getConnection(ConnectionManager connectionManager, DiscoveryNode discoveryNode);
    }

    @FunctionalInterface
    public interface NodeConnectedBehavior {
        boolean connectedNodes(ConnectionManager connectionManager, DiscoveryNode node);
    }
}
