/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;

import java.io.Closeable;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

public interface ConnectionManager extends Closeable {

    void addListener(TransportConnectionListener listener);

    void removeListener(TransportConnectionListener listener);

    void openConnection(DiscoveryNode node, ConnectionProfile connectionProfile, ActionListener<Transport.Connection> listener);

    void connectToNode(
        DiscoveryNode node,
        @Nullable ConnectionProfile connectionProfile,
        ConnectionValidator connectionValidator,
        ActionListener<Releasable> listener
    ) throws ConnectTransportException;

    Transport.Connection getConnection(DiscoveryNode node);

    boolean nodeConnected(DiscoveryNode node);

    void disconnectFromNode(DiscoveryNode node);

    Set<DiscoveryNode> getAllConnectedNodes();

    int size();

    @Override
    void close();

    void closeNoBlock();

    ConnectionProfile getConnectionProfile();

    @FunctionalInterface
    interface ConnectionValidator {
        void validate(Transport.Connection connection, ConnectionProfile profile, ActionListener<Void> listener);
    }

    final class DelegatingNodeConnectionListener implements TransportConnectionListener {

        private final CopyOnWriteArrayList<TransportConnectionListener> listeners = new CopyOnWriteArrayList<>();

        @Override
        public void onNodeDisconnected(DiscoveryNode key, Transport.Connection connection) {
            for (TransportConnectionListener listener : listeners) {
                listener.onNodeDisconnected(key, connection);
            }
        }

        @Override
        public void onNodeConnected(DiscoveryNode node, Transport.Connection connection) {
            for (TransportConnectionListener listener : listeners) {
                listener.onNodeConnected(node, connection);
            }
        }

        @Override
        public void onConnectionOpened(Transport.Connection connection) {
            for (TransportConnectionListener listener : listeners) {
                listener.onConnectionOpened(connection);
            }
        }

        @Override
        public void onConnectionClosed(Transport.Connection connection) {
            for (TransportConnectionListener listener : listeners) {
                listener.onConnectionClosed(connection);
            }
        }

        public void addListener(TransportConnectionListener listener) {
            listeners.addIfAbsent(listener);
        }

        public void removeListener(TransportConnectionListener listener) {
            listeners.remove(listener);
        }
    }
}
