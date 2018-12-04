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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.KeyedLock;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class manages node connections. The connection is opened by the underlying transport. Once the
 * connection is opened, this class manages the connection. This includes keep-alive pings and closing
 * the connection when the connection manager is closed.
 */
public class ConnectionManager implements Closeable {

    private static final Logger logger = LogManager.getLogger(ConnectionManager.class);

    private final ConcurrentMap<DiscoveryNode, Transport.Connection> connectedNodes = ConcurrentCollections.newConcurrentMap();
    private final KeyedLock<String> connectionLock = new KeyedLock<>();
    private final Transport transport;
    private final ThreadPool threadPool;
    private final ConnectionProfile defaultProfile;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final ReadWriteLock closeLock = new ReentrantReadWriteLock();
    private final DelegatingNodeConnectionListener connectionListener = new DelegatingNodeConnectionListener();

    public ConnectionManager(Settings settings, Transport transport, ThreadPool threadPool) {
        this(ConnectionProfile.buildDefaultConnectionProfile(settings), transport, threadPool);
    }

    public ConnectionManager(ConnectionProfile connectionProfile, Transport transport, ThreadPool threadPool) {
        this.transport = transport;
        this.threadPool = threadPool;
        this.defaultProfile = connectionProfile;
    }

    public void addListener(TransportConnectionListener listener) {
        this.connectionListener.listeners.addIfAbsent(listener);
    }

    public void removeListener(TransportConnectionListener listener) {
        this.connectionListener.listeners.remove(listener);
    }

    public Transport.Connection openConnection(DiscoveryNode node, ConnectionProfile connectionProfile) {
        ConnectionProfile resolvedProfile = ConnectionProfile.resolveConnectionProfile(connectionProfile, defaultProfile);
        return internalOpenConnection(node, resolvedProfile);
    }

    /**
     * Connects to a node with the given connection profile. If the node is already connected this method has no effect.
     * Once a successful is established, it can be validated before being exposed.
     */
    public void connectToNode(DiscoveryNode node, ConnectionProfile connectionProfile,
                              CheckedBiConsumer<Transport.Connection, ConnectionProfile, IOException> connectionValidator)
        throws ConnectTransportException {
        ConnectionProfile resolvedProfile = ConnectionProfile.resolveConnectionProfile(connectionProfile, defaultProfile);
        if (node == null) {
            throw new ConnectTransportException(null, "can't connect to a null node");
        }
        closeLock.readLock().lock(); // ensure we don't open connections while we are closing
        try {
            ensureOpen();
            try (Releasable ignored = connectionLock.acquire(node.getId())) {
                Transport.Connection connection = connectedNodes.get(node);
                if (connection != null) {
                    return;
                }
                boolean success = false;
                try {
                    connection = internalOpenConnection(node, resolvedProfile);
                    connectionValidator.accept(connection, resolvedProfile);
                    // we acquire a connection lock, so no way there is an existing connection
                    connectedNodes.put(node, connection);
                    if (logger.isDebugEnabled()) {
                        logger.debug("connected to node [{}]", node);
                    }
                    try {
                        connectionListener.onNodeConnected(node);
                    } finally {
                        final Transport.Connection finalConnection = connection;
                        connection.addCloseListener(ActionListener.wrap(() -> {
                            connectedNodes.remove(node, finalConnection);
                            connectionListener.onNodeDisconnected(node);
                        }));
                    }
                    if (connection.isClosed()) {
                        throw new NodeNotConnectedException(node, "connection concurrently closed");
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
        } finally {
            closeLock.readLock().unlock();
        }
    }

    /**
     * Returns a connection for the given node if the node is connected.
     * Connections returned from this method must not be closed. The lifecycle of this connection is
     * maintained by this connection manager
     *
     * @throws NodeNotConnectedException if the node is not connected
     * @see #connectToNode(DiscoveryNode, ConnectionProfile, CheckedBiConsumer)
     */
    public Transport.Connection getConnection(DiscoveryNode node) {
        Transport.Connection connection = connectedNodes.get(node);
        if (connection == null) {
            throw new NodeNotConnectedException(node, "Node not connected");
        }
        return connection;
    }

    /**
     * Returns {@code true} if the node is connected.
     */
    public boolean nodeConnected(DiscoveryNode node) {
        return connectedNodes.containsKey(node);
    }

    /**
     * Disconnected from the given node, if not connected, will do nothing.
     */
    public void disconnectFromNode(DiscoveryNode node) {
        Transport.Connection nodeChannels = connectedNodes.remove(node);
        if (nodeChannels != null) {
            // if we found it and removed it we close
            nodeChannels.close();
        }
    }

    /**
     * Returns the number of nodes this manager is connected to.
     */
    public int size() {
        return connectedNodes.size();
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            CountDownLatch latch = new CountDownLatch(1);

            // TODO: Consider moving all read/write lock (in Transport and this class) to the TransportService
            threadPool.generic().execute(() -> {
                closeLock.writeLock().lock();
                try {
                    // we are holding a write lock so nobody adds to the connectedNodes / openConnections map - it's safe to first close
                    // all instances and then clear them maps
                    Iterator<Map.Entry<DiscoveryNode, Transport.Connection>> iterator = connectedNodes.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<DiscoveryNode, Transport.Connection> next = iterator.next();
                        try {
                            IOUtils.closeWhileHandlingException(next.getValue());
                        } finally {
                            iterator.remove();
                        }
                    }
                } finally {
                    closeLock.writeLock().unlock();
                    latch.countDown();
                }
            });

            try {
                latch.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // ignore
            }
        }
    }

    private Transport.Connection internalOpenConnection(DiscoveryNode node, ConnectionProfile connectionProfile) {
        PlainActionFuture<Transport.Connection> future = PlainActionFuture.newFuture();
        Releasable pendingConnection = transport.openConnection(node, connectionProfile, future);
        Transport.Connection connection;
        try {
            connection = future.actionGet();
        } catch (IllegalStateException e) {
            // If the future was interrupted we must cancel the pending connection to avoid channels leaking
            if (e.getCause() instanceof InterruptedException) {
                pendingConnection.close();
            }
            throw e;
        }
        try {
            connectionListener.onConnectionOpened(connection);
        } finally {
            connection.addCloseListener(ActionListener.wrap(() -> connectionListener.onConnectionClosed(connection)));
        }
        if (connection.isClosed()) {
            throw new ConnectTransportException(node, "a channel closed while connecting");
        }
        return connection;
    }

    private void ensureOpen() {
        if (isClosed.get()) {
            throw new IllegalStateException("connection manager is closed");
        }
    }

    ConnectionProfile getConnectionProfile() {
        return defaultProfile;
    }

    private static final class DelegatingNodeConnectionListener implements TransportConnectionListener {

        private final CopyOnWriteArrayList<TransportConnectionListener> listeners = new CopyOnWriteArrayList<>();

        @Override
        public void onNodeDisconnected(DiscoveryNode key) {
            for (TransportConnectionListener listener : listeners) {
                listener.onNodeDisconnected(key);
            }
        }

        @Override
        public void onNodeConnected(DiscoveryNode node) {
            for (TransportConnectionListener listener : listeners) {
                listener.onNodeConnected(node);
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
    }
}
