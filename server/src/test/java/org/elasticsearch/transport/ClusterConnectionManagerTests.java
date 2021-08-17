/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class ClusterConnectionManagerTests extends ESTestCase {

    private ClusterConnectionManager connectionManager;
    private ThreadPool threadPool;
    private Transport transport;
    private ConnectionProfile connectionProfile;

    @Before
    public void createConnectionManager() {
        Settings settings = Settings.builder()
            .put("node.name", ClusterConnectionManagerTests.class.getSimpleName())
            .build();
        threadPool = new ThreadPool(settings);
        transport = mock(Transport.class);
        connectionManager = new ClusterConnectionManager(settings, transport);
        TimeValue oneSecond = new TimeValue(1000);
        TimeValue oneMinute = TimeValue.timeValueMinutes(1);
        connectionProfile = ConnectionProfile.buildSingleChannelProfile(TransportRequestOptions.Type.REG, oneSecond, oneSecond,
            oneMinute, Compression.Enabled.FALSE, Compression.Scheme.DEFLATE);
    }

    @After
    public void stopThreadPool() {
        ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS);
    }

    public void testConnectAndDisconnect() {
        AtomicInteger nodeConnectedCount = new AtomicInteger();
        AtomicInteger nodeDisconnectedCount = new AtomicInteger();
        connectionManager.addListener(new TransportConnectionListener() {
            @Override
            public void onNodeConnected(DiscoveryNode node, Transport.Connection connection) {
                nodeConnectedCount.incrementAndGet();
            }

            @Override
            public void onNodeDisconnected(DiscoveryNode node, Transport.Connection connection) {
                nodeDisconnectedCount.incrementAndGet();
            }
        });


        DiscoveryNode node = new DiscoveryNode("", new TransportAddress(InetAddress.getLoopbackAddress(), 0), Version.CURRENT);
        Transport.Connection connection = new TestConnect(node);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Transport.Connection> listener = (ActionListener<Transport.Connection>) invocationOnMock.getArguments()[2];
            listener.onResponse(connection);
            return null;
        }).when(transport).openConnection(eq(node), eq(connectionProfile), anyActionListener());

        assertFalse(connectionManager.nodeConnected(node));

        AtomicReference<Transport.Connection> connectionRef = new AtomicReference<>();
        ConnectionManager.ConnectionValidator validator = (c, p, l) -> {
            connectionRef.set(c);
            l.onResponse(null);
        };
        PlainActionFuture.get(fut -> connectionManager.connectToNode(node, connectionProfile, validator, fut.map(x -> null)));

        assertFalse(connection.isClosed());
        assertTrue(connectionManager.nodeConnected(node));
        assertSame(connection, connectionManager.getConnection(node));
        assertEquals(1, connectionManager.size());
        assertEquals(1, nodeConnectedCount.get());
        assertEquals(0, nodeDisconnectedCount.get());

        if (randomBoolean()) {
            connectionManager.disconnectFromNode(node);
        } else {
            connection.close();
        }
        assertTrue(connection.isClosed());
        assertEquals(0, connectionManager.size());
        assertEquals(1, nodeConnectedCount.get());
        assertEquals(1, nodeDisconnectedCount.get());
    }

    public void testConcurrentConnects() throws Exception {
        Set<Transport.Connection> connections = Collections.newSetFromMap(new ConcurrentHashMap<>());

        DiscoveryNode node = new DiscoveryNode("", new TransportAddress(InetAddress.getLoopbackAddress(), 0), Version.CURRENT);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Transport.Connection> listener = (ActionListener<Transport.Connection>) invocationOnMock.getArguments()[2];

            boolean success = randomBoolean();
            if (success) {
                Transport.Connection connection = new TestConnect(node);
                connections.add(connection);
                if (randomBoolean()) {
                    listener.onResponse(connection);
                } else {
                    threadPool.generic().execute(() -> listener.onResponse(connection));
                }
            } else {
                threadPool.generic().execute(() -> listener.onFailure(new IllegalStateException("dummy exception")));
            }
            return null;
        }).when(transport).openConnection(eq(node), eq(connectionProfile), anyActionListener());

        assertFalse(connectionManager.nodeConnected(node));

        ConnectionManager.ConnectionValidator validator = (c, p, l) -> {
            boolean success = randomBoolean();
            if (success) {
                if (randomBoolean()) {
                    l.onResponse(null);
                } else {
                    threadPool.generic().execute(() -> l.onResponse(null));
                }
            } else {
                threadPool.generic().execute(() -> l.onFailure(new IllegalStateException("dummy exception")));
            }
        };

        List<Thread> threads = new ArrayList<>();
        AtomicInteger nodeConnectedCount = new AtomicInteger();
        AtomicInteger nodeFailureCount = new AtomicInteger();

        CyclicBarrier barrier = new CyclicBarrier(11);
        for (int i = 0; i < 10; i++) {
            Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }
                CountDownLatch latch = new CountDownLatch(1);
                connectionManager.connectToNode(node, connectionProfile, validator,
                    ActionListener.wrap(c -> {
                        nodeConnectedCount.incrementAndGet();
                        if (connectionManager.nodeConnected(node) == false) {
                            throw new AssertionError("Expected node to be connected");
                        }
                        assert latch.getCount() == 1;
                        latch.countDown();
                    }, e -> {
                        nodeFailureCount.incrementAndGet();
                        assert latch.getCount() == 1;
                        latch.countDown();
                    }));
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            });
            threads.add(thread);
            thread.start();
        }

        barrier.await();
        threads.forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        });

        assertEquals(10, nodeConnectedCount.get() + nodeFailureCount.get());

        int managedConnections = connectionManager.size();
        if (managedConnections != 0) {
            assertEquals(1, managedConnections);

            // Only a single connection attempt should be open.
            assertEquals(1, connections.stream().filter(c -> c.isClosed() == false).count());
        } else {
            // No connections succeeded
            assertEquals(0, connections.stream().filter(c -> c.isClosed() == false).count());
        }


        connectionManager.close();
        // The connection manager will close all open connections
        for (Transport.Connection connection : connections) {
            assertTrue(connection.isClosed());
        }
    }

    public void testConnectFailsDuringValidation() {
        AtomicInteger nodeConnectedCount = new AtomicInteger();
        AtomicInteger nodeDisconnectedCount = new AtomicInteger();
        connectionManager.addListener(new TransportConnectionListener() {
            @Override
            public void onNodeConnected(DiscoveryNode node, Transport.Connection connection) {
                nodeConnectedCount.incrementAndGet();
            }

            @Override
            public void onNodeDisconnected(DiscoveryNode node, Transport.Connection connection) {
                nodeDisconnectedCount.incrementAndGet();
            }
        });


        DiscoveryNode node = new DiscoveryNode("", new TransportAddress(InetAddress.getLoopbackAddress(), 0), Version.CURRENT);
        Transport.Connection connection = new TestConnect(node);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Transport.Connection> listener = (ActionListener<Transport.Connection>) invocationOnMock.getArguments()[2];
            listener.onResponse(connection);
            return null;
        }).when(transport).openConnection(eq(node), eq(connectionProfile), anyActionListener());

        assertFalse(connectionManager.nodeConnected(node));

        ConnectionManager.ConnectionValidator validator = (c, p, l) -> l.onFailure(new ConnectTransportException(node, ""));

        PlainActionFuture<Void> fut = new PlainActionFuture<>();
        connectionManager.connectToNode(node, connectionProfile, validator, fut);
        expectThrows(ConnectTransportException.class, () -> fut.actionGet());

        assertTrue(connection.isClosed());
        assertFalse(connectionManager.nodeConnected(node));
        expectThrows(NodeNotConnectedException.class, () -> connectionManager.getConnection(node));
        assertEquals(0, connectionManager.size());
        assertEquals(0, nodeConnectedCount.get());
        assertEquals(0, nodeDisconnectedCount.get());
    }

    public void testConnectFailsDuringConnect() {
        AtomicInteger nodeConnectedCount = new AtomicInteger();
        AtomicInteger nodeDisconnectedCount = new AtomicInteger();
        connectionManager.addListener(new TransportConnectionListener() {
            @Override
            public void onNodeConnected(DiscoveryNode node, Transport.Connection connection) {
                nodeConnectedCount.incrementAndGet();
            }

            @Override
            public void onNodeDisconnected(DiscoveryNode node, Transport.Connection connection) {
                nodeDisconnectedCount.incrementAndGet();
            }
        });


        DiscoveryNode node = new DiscoveryNode("", new TransportAddress(InetAddress.getLoopbackAddress(), 0), Version.CURRENT);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Transport.Connection> listener = (ActionListener<Transport.Connection>)invocationOnMock.getArguments()[2];
            listener.onFailure(new ConnectTransportException(node, ""));
            return null;
        }).when(transport).openConnection(eq(node), eq(connectionProfile), anyActionListener());

        assertFalse(connectionManager.nodeConnected(node));

        ConnectionManager.ConnectionValidator validator = (c, p, l) -> l.onResponse(null);

        PlainActionFuture<Void> fut = new PlainActionFuture<>();
        connectionManager.connectToNode(node, connectionProfile, validator, fut);
        expectThrows(ConnectTransportException.class, () -> fut.actionGet());

        assertFalse(connectionManager.nodeConnected(node));
        expectThrows(NodeNotConnectedException.class, () -> connectionManager.getConnection(node));
        assertEquals(0, connectionManager.size());
        assertEquals(0, nodeConnectedCount.get());
        assertEquals(0, nodeDisconnectedCount.get());
    }

    private static class TestConnect extends CloseableConnection {

        private final DiscoveryNode node;

        private TestConnect(DiscoveryNode node) {
            this.node = node;
        }

        @Override
        public DiscoveryNode getNode() {
            return node;
        }

        @Override
        public Version getVersion() {
            return node.getVersion();
        }

        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
            throws TransportException {
        }
    }
}
