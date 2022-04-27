/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class ClusterConnectionManagerTests extends ESTestCase {

    private ClusterConnectionManager connectionManager;
    private ThreadPool threadPool;
    private Transport transport;
    private ConnectionProfile connectionProfile;

    @Before
    public void createConnectionManager() {
        Settings settings = Settings.builder().put("node.name", ClusterConnectionManagerTests.class.getSimpleName()).build();
        threadPool = new ThreadPool(settings);
        transport = mock(Transport.class);
        connectionManager = new ClusterConnectionManager(settings, transport, threadPool.getThreadContext());
        TimeValue oneSecond = new TimeValue(1000);
        TimeValue oneMinute = TimeValue.timeValueMinutes(1);
        connectionProfile = ConnectionProfile.buildSingleChannelProfile(
            TransportRequestOptions.Type.REG,
            oneSecond,
            oneSecond,
            oneMinute,
            Compression.Enabled.FALSE,
            Compression.Scheme.DEFLATE
        );
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

    @TestLogging(
        reason = "testing log messages emitted on disconnect",
        value = "org.elasticsearch.transport.ClusterConnectionManager:TRACE"
    )
    public void testDisconnectLogging() throws IllegalAccessException {
        final Supplier<DiscoveryNode> nodeFactory = () -> new DiscoveryNode(
            randomAlphaOfLength(10),
            new TransportAddress(InetAddress.getLoopbackAddress(), 0),
            Collections.singletonMap("attr", "val"),
            DiscoveryNodeRole.roles(),
            Version.CURRENT
        );
        final DiscoveryNode remoteClose = nodeFactory.get();
        final DiscoveryNode localClose = nodeFactory.get();
        final DiscoveryNode shutdownClose = nodeFactory.get();

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<Transport.Connection> listener = (ActionListener<Transport.Connection>) invocationOnMock.getArguments()[2];
            final DiscoveryNode discoveryNode = (DiscoveryNode) invocationOnMock.getArguments()[0];
            listener.onResponse(new TestConnect(discoveryNode));
            return null;
        }).when(transport).openConnection(any(), eq(connectionProfile), anyActionListener());

        final ConnectionManager.ConnectionValidator validator = (c, p, l) -> l.onResponse(null);
        final AtomicReference<Releasable> toClose = new AtomicReference<>();

        PlainActionFuture.get(f -> connectionManager.connectToNode(remoteClose, connectionProfile, validator, f.map(x -> null)));
        PlainActionFuture.get(f -> connectionManager.connectToNode(shutdownClose, connectionProfile, validator, f.map(x -> null)));
        PlainActionFuture.get(f -> connectionManager.connectToNode(localClose, connectionProfile, validator, f.map(toClose::getAndSet)));

        final Releasable localConnectionRef = toClose.getAndSet(null);
        assertThat(localConnectionRef, notNullValue());

        final String loggerName = "org.elasticsearch.transport.ClusterConnectionManager";
        final Logger logger = LogManager.getLogger(loggerName);
        final MockLogAppender appender = new MockLogAppender();
        try {
            appender.start();
            Loggers.addAppender(logger, appender);
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "locally-triggered close message",
                    loggerName,
                    Level.DEBUG,
                    "closing unused transport connection to [" + localClose + "]"
                )
            );
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "remotely-triggered close message",
                    loggerName,
                    Level.INFO,
                    "transport connection to [" + remoteClose.descriptionWithoutAttributes() + "] closed by remote"
                )
            );
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "shutdown-triggered close message",
                    loggerName,
                    Level.TRACE,
                    "connection manager shut down, closing transport connection to [" + shutdownClose + "]"
                )
            );

            Releasables.close(localConnectionRef);
            connectionManager.disconnectFromNode(remoteClose);
            connectionManager.close();

            appender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(logger, appender);
            appender.stop();
        }
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
        AtomicInteger nodeClosedCount = new AtomicInteger();
        AtomicInteger nodeFailureCount = new AtomicInteger();

        int threadCount = between(1, 10);
        Releasable[] releasables = new Releasable[threadCount];

        final ThreadContext threadContext = threadPool.getThreadContext();
        final String contextHeader = "test-context-header";

        CyclicBarrier barrier = new CyclicBarrier(threadCount + 1);
        Semaphore pendingCloses = new Semaphore(threadCount);
        for (int i = 0; i < threadCount; i++) {
            final int threadIndex = i;
            Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }
                CountDownLatch latch = new CountDownLatch(1);
                try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                    final String contextValue = randomAlphaOfLength(10);
                    threadContext.putHeader(contextHeader, contextValue);
                    connectionManager.connectToNode(node, connectionProfile, validator, ActionListener.wrap(c -> {
                        assert connectionManager.nodeConnected(node);
                        assertThat(threadContext.getHeader(contextHeader), equalTo(contextValue));

                        assertTrue(pendingCloses.tryAcquire());
                        connectionManager.getConnection(node).addRemovedListener(ActionListener.wrap(pendingCloses::release));

                        if (randomBoolean()) {
                            releasables[threadIndex] = c;
                            nodeConnectedCount.incrementAndGet();
                        } else {
                            Releasables.close(c);
                            nodeClosedCount.incrementAndGet();
                        }

                        assert latch.getCount() == 1;
                        latch.countDown();
                    }, e -> {
                        assertThat(threadContext.getHeader(contextHeader), equalTo(contextValue));
                        nodeFailureCount.incrementAndGet();
                        assert latch.getCount() == 1;
                        latch.countDown();
                    }));
                }
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

        assertEquals(threadCount, nodeConnectedCount.get() + nodeClosedCount.get() + nodeFailureCount.get());

        if (nodeConnectedCount.get() == 0) {
            // Any successful connections were closed
            assertTrue(pendingCloses.tryAcquire(threadCount, 10, TimeUnit.SECONDS));
            pendingCloses.release(threadCount);
            assertTrue(connections.stream().allMatch(Transport.Connection::isClosed));
            assertEquals(0, connectionManager.size());
        } else {
            assertEquals(1, connectionManager.size());
            assertEquals(1L, connections.stream().filter(c -> c.isClosed() == false).count());
        }

        if (randomBoolean()) {
            Releasables.close(releasables);
            assertTrue(pendingCloses.tryAcquire(threadCount, 10, TimeUnit.SECONDS));
            pendingCloses.release(threadCount);
            assertEquals(0, connectionManager.size());
            assertTrue(connections.stream().allMatch(Transport.Connection::isClosed));
        }

        connectionManager.close();
        // The connection manager will close all open connections
        for (Transport.Connection connection : connections) {
            assertTrue(connection.isClosed());
        }
    }

    public void testConcurrentConnectsAndDisconnects() throws Exception {
        final DiscoveryNode node = new DiscoveryNode("", new TransportAddress(InetAddress.getLoopbackAddress(), 0), Version.CURRENT);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Transport.Connection> listener = (ActionListener<Transport.Connection>) invocationOnMock.getArguments()[2];
            listener.onResponse(new TestConnect(node));
            return null;
        }).when(transport).openConnection(eq(node), any(), anyActionListener());

        final Semaphore validatorPermits = new Semaphore(Integer.MAX_VALUE);

        final ConnectionManager.ConnectionValidator validator = (c, p, l) -> {
            assertTrue(validatorPermits.tryAcquire());
            threadPool.executor(randomFrom(ThreadPool.Names.GENERIC, ThreadPool.Names.SAME)).execute(() -> {
                try {
                    l.onResponse(null);
                } finally {
                    validatorPermits.release();
                }
            });
        };

        final Semaphore pendingConnections = new Semaphore(between(1, 1000));
        final int threadCount = between(1, 10);
        final CountDownLatch countDownLatch = new CountDownLatch(threadCount);

        final Runnable action = new Runnable() {
            @Override
            public void run() {
                if (pendingConnections.tryAcquire()) {
                    connectionManager.connectToNode(node, null, validator, new ActionListener<>() {
                        @Override
                        public void onResponse(Releasable releasable) {
                            if (connectionManager.nodeConnected(node) == false) {
                                final String description = releasable.toString();
                                fail(description);
                            }
                            Releasables.close(releasable);
                            threadPool.generic().execute(() -> run());
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (e instanceof ConnectTransportException
                                && e.getMessage().contains("concurrently connecting and disconnecting")) {
                                pendingConnections.release();
                                threadPool.generic().execute(() -> run());
                            } else {
                                throw new AssertionError("unexpected", e);
                            }
                        }
                    });
                } else {
                    countDownLatch.countDown();
                }
            }
        };

        for (int i = 0; i < threadCount; i++) {
            threadPool.generic().execute(action);
        }

        assertTrue("threads did not all complete", countDownLatch.await(10, TimeUnit.SECONDS));
        assertTrue("validatorPermits not all released", validatorPermits.tryAcquire(Integer.MAX_VALUE, 10, TimeUnit.SECONDS));
        assertFalse("node still connected", connectionManager.nodeConnected(node));
        connectionManager.close();
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

        PlainActionFuture<Releasable> fut = new PlainActionFuture<>();
        connectionManager.connectToNode(node, connectionProfile, validator, fut);
        expectThrows(ConnectTransportException.class, fut::actionGet);

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
            ActionListener<Transport.Connection> listener = (ActionListener<Transport.Connection>) invocationOnMock.getArguments()[2];
            listener.onFailure(new ConnectTransportException(node, ""));
            return null;
        }).when(transport).openConnection(eq(node), eq(connectionProfile), anyActionListener());

        assertFalse(connectionManager.nodeConnected(node));

        ConnectionManager.ConnectionValidator validator = (c, p, l) -> l.onResponse(null);

        PlainActionFuture<Releasable> fut = new PlainActionFuture<>();
        connectionManager.connectToNode(node, connectionProfile, validator, fut);
        expectThrows(ConnectTransportException.class, fut::actionGet);

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
            throws TransportException {}
    }
}
