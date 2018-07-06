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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.KeyedLock;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

public class ConnectionManager {

    private final ConcurrentMap<DiscoveryNode, Transport.Connection> connectedNodes = newConcurrentMap();
    private final KeyedLock<String> connectionLock = new KeyedLock<>();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final Logger logger;
    private final DelegatingNodeConnectionListener connectionListener = new DelegatingNodeConnectionListener();

    public ConnectionManager(Logger logger) {
        this.logger = logger;
    }

    public void registerListener(TransportConnectionListener.NodeConnectionListener listener) {
        this.connectionListener.listeners.add(listener);
    }

    public void connectToNode(Transport transport, DiscoveryNode node, ConnectionProfile connectionProfile,
                              CheckedBiConsumer<Transport.Connection, ConnectionProfile, IOException> connectionValidator)
        throws ConnectTransportException {
        if (node == null) {
            throw new ConnectTransportException(null, "can't connect to a null node");
        }
        ensureOpen();
        try (Releasable ignored = connectionLock.acquire(node.getId())) {
            Transport.Connection connection = connectedNodes.get(node);
            if (connection != null) {
                return;
            }
            boolean success = false;
            try {
                connection = transport.openConnection(node, connectionProfile);
                connectionValidator.accept(connection, connectionProfile);
                // we acquire a connection lock, so no way there is an existing connection
                connectedNodes.put(node, connection);
                if (logger.isDebugEnabled()) {
                    logger.debug("connected to node [{}]", node);
                }
                try {
                    connectionListener.onNodeConnected(node);
                } finally {
                    if (connection.isClosed()) {
                        // we got closed concurrently due to a disconnect or some other event on the channel.
                        // the close callback will close the NodeChannel instance first and then try to remove
                        // the connection from the connected nodes. It will NOT acquire the connectionLock for
                        // the node to prevent any blocking calls on network threads. Yet, we still establish a happens
                        // before relationship to the connectedNodes.put since we check if we can remove the
                        // (DiscoveryNode, NodeChannels) tuple from the map after we closed. Here we check if it's closed an if so we
                        // try to remove it first either way one of the two wins even if the callback has run before we even added the
                        // tuple to the map since in that case we remove it here again
                        if (connectedNodes.remove(node, connection)) {
                            connectionListener.onNodeDisconnected(node);
                        }
                        throw new NodeNotConnectedException(node, "connection concurrently closed");
                    }
                }
                success = true;
            } catch (ConnectTransportException e) {
                throw e;
            } catch (Exception e) {
                throw new ConnectTransportException(node, "general node connection failure", e);
            } finally {
                if (success == false) { // close the connection if there is a failure
                    logger.trace(() -> new ParameterizedMessage("failed to connect to [{}], cleaning dangling connections", node));
                    IOUtils.closeWhileHandlingException(connection);
                }
            }
        }
    }

    public Transport.Connection getConnection(DiscoveryNode node) {
        Transport.Connection connection = connectedNodes.get(node);
        if (connection == null) {
            throw new NodeNotConnectedException(node, "Node not connected");
        }
        return connection;
    }

    public boolean nodeConnected(DiscoveryNode node) {
        return connectedNodes.containsKey(node);
    }

    public void disconnectFromNode(DiscoveryNode node) {
        // TODO: Do we need to lock here?
        Transport.Connection nodeChannels = connectedNodes.remove(node);
        if (nodeChannels != null) { // if we found it and removed it we close and notify
            IOUtils.closeWhileHandlingException(nodeChannels, () -> connectionListener.onNodeDisconnected(node));
        }
    }

    private void ensureOpen() {
        if (isClosed.get()) {
            throw new IllegalStateException("connection manager is closed");
        }
    }

    private static final class DelegatingNodeConnectionListener implements TransportConnectionListener.NodeConnectionListener {

        private final List<TransportConnectionListener.NodeConnectionListener> listeners = new CopyOnWriteArrayList<>();

        @Override
        public void onNodeDisconnected(DiscoveryNode key) {
            for (TransportConnectionListener.NodeConnectionListener listener : listeners) {
                listener.onNodeDisconnected(key);
            }
        }

        @Override
        public void onNodeConnected(DiscoveryNode node) {
            for (TransportConnectionListener.NodeConnectionListener listener : listeners) {
                listener.onNodeConnected(node);
            }
        }
    }
}
