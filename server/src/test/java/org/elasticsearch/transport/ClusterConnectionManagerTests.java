/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.apache.logging.log4j.Level;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
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
        threadPool = new ThreadPool(settings, MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders());
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

        DiscoveryNode node = DiscoveryNodeUtils.create("", new TransportAddress(InetAddress.getLoopbackAddress(), 0));
        Transport.Connection connection = new TestConnect(node);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Transport.Connection> listener = (ActionListener<Transport.Connection>) invocationOnMock.getArguments()[2];
            listener.onResponse(connection);
            return null;
        }).when(transport).openConnection(eq(node), eq(connectionProfile), anyActionListener());

        assertFalse(connectionManager.nodeConnected(node));

        final var validatedConnectionRef = new AtomicReference<Transport.Connection>();
        ConnectionManager.ConnectionValidator validator = (c, p, l) -> {
            validatedConnectionRef.set(c);
            l.onResponse(null);
        };
        safeAwait(listener -> connectionManager.connectToNode(node, connectionProfile, validator, listener.map(x -> null)));

        assertFalse(connection.isClosed());
        assertTrue(connectionManager.nodeConnected(node));
        assertSame(connection, validatedConnectionRef.get());
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
    public void testDisconnectLogging() {
        final Supplier<DiscoveryNode> nodeFactory = () -> DiscoveryNodeUtils.create(
            randomAlphaOfLength(10),
            new TransportAddress(InetAddress.getLoopbackAddress(), 0),
            Collections.singletonMap("attr", "val"),
            DiscoveryNodeRole.roles()
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

        safeAwait(l -> connectionManager.connectToNode(remoteClose, connectionProfile, validator, l.map(x -> null)));
        safeAwait(l -> connectionManager.connectToNode(shutdownClose, connectionProfile, validator, l.map(x -> null)));
        safeAwait(l -> connectionManager.connectToNode(localClose, connectionProfile, validator, l.map(toClose::getAndSet)));

        final Releasable localConnectionRef = toClose.getAndSet(null);
        assertThat(localConnectionRef, notNullValue());

        try (var mockLog = MockLog.capture(ClusterConnectionManager.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "locally-triggered close message",
                    ClusterConnectionManager.class.getCanonicalName(),
                    Level.DEBUG,
                    "closing unused transport connection to [" + localClose + "]"
                )
            );
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "remotely-triggered close message",
                    ClusterConnectionManager.class.getCanonicalName(),
                    Level.INFO,
                    "transport connection to ["
                        + remoteClose.descriptionWithoutAttributes()
                        + "] closed by remote; "
                        + "if unexpected, see [https://www.elastic.co/docs/*] for troubleshooting guidance"
                )
            );
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "shutdown-triggered close message",
                    ClusterConnectionManager.class.getCanonicalName(),
                    Level.TRACE,
                    "connection manager shut down, closing transport connection to [" + shutdownClose + "]"
                )
            );

            Releasables.close(localConnectionRef);
            connectionManager.disconnectFromNode(remoteClose);
            connectionManager.close();

            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testConcurrentConnects() throws Exception {
        Set<Transport.Connection> connections = ConcurrentCollections.newConcurrentSet();

        DiscoveryNode node = DiscoveryNodeUtils.create("", new TransportAddress(InetAddress.getLoopbackAddress(), 0));
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
                safeAwait(barrier);
                CountDownLatch latch = new CountDownLatch(1);
                try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                    final String contextValue = randomAlphaOfLength(10);
                    threadContext.putHeader(contextHeader, contextValue);
                    connectionManager.connectToNode(node, connectionProfile, validator, ActionListener.wrap(c -> {
                        assert connectionManager.nodeConnected(node);
                        assertThat(threadContext.getHeader(contextHeader), equalTo(contextValue));

                        assertTrue(pendingCloses.tryAcquire());
                        connectionManager.getConnection(node).addRemovedListener(ActionListener.running(pendingCloses::release));

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
                safeAwait(latch);
            });
            threads.add(thread);
            thread.start();
        }

        safeAwait(barrier);
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
            safeAcquire(threadCount, pendingCloses);
            pendingCloses.release(threadCount);
            assertTrue(connections.stream().allMatch(Transport.Connection::isClosed));
            assertEquals(0, connectionManager.size());
        } else {
            assertEquals(1, connectionManager.size());
            assertEquals(1L, connections.stream().filter(c -> c.isClosed() == false).count());
        }

        if (randomBoolean()) {
            Releasables.close(releasables);
            safeAcquire(threadCount, pendingCloses);
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

    public void testConcurrentConnectsDuringClose() throws Exception {

        // This test ensures that closing the connection manager doesn't block forever, even if there's a constant stream of attempts to
        // open connections. Note that closing the connection manager _does_ block while there are in-flight connection attempts, and in
        // practice each attempt will (eventually) finish, so we're just trying to test that constant open attempts do not cause starvation.
        //
        // It works by spawning connection-open attempts in several concurrent loops, putting a Runnable to complete each attempt into a
        // queue, and then consuming and completing the enqueued runnables in a separate thread. The consuming thread is throttled via a
        // Semaphore, from which the main thread steals a permit which ensures that there's always at least one pending connection while the
        // close is ongoing even though no connection attempt blocks forever.

        final var pendingConnectionPermits = new Semaphore(0);
        final var pendingConnections = ConcurrentCollections.<Runnable>newQueue();

        // transport#openConnection enqueues a Runnable to complete the connection attempt
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final var listener = (ActionListener<Transport.Connection>) invocationOnMock.getArguments()[2];
            final var targetNode = (DiscoveryNode) invocationOnMock.getArguments()[0];
            pendingConnections.add(() -> listener.onResponse(new TestConnect(targetNode)));
            pendingConnectionPermits.release();
            return null;
        }).when(transport).openConnection(any(), eq(connectionProfile), anyActionListener());

        final ConnectionManager.ConnectionValidator validator = (c, p, l) -> l.onResponse(null);

        // Once we start to see connections being rejected, we give back the stolen permit so that the last connection can complete
        final var onConnectException = new RunOnce(pendingConnectionPermits::release);

        // Create a few threads which open connections in a loop. Must be at least 2 so that there's always more connections incoming.
        final var connectionLoops = between(2, 4);
        final var connectionLoopCountDown = new CountDownLatch(connectionLoops);
        final var expectConnectionFailures = new AtomicBoolean(); // unexpected failures would make this test pass vacuously

        class ConnectionLoop extends AbstractRunnable {
            private final boolean useConnectToNode = randomBoolean();

            @Override
            public void onFailure(Exception e) {
                assert false : e;
            }

            @Override
            protected void doRun() throws Exception {
                final var discoveryNode = DiscoveryNodeUtils.create("", new TransportAddress(InetAddress.getLoopbackAddress(), 0));
                final var listener = new ActionListener<Releasable>() {
                    @Override
                    public void onResponse(Releasable releasable) {
                        releasable.close();
                        threadPool.generic().execute(ConnectionLoop.this);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        assertTrue(expectConnectionFailures.get());
                        assertThat(e, instanceOf(ConnectTransportException.class));
                        assertThat(e.getMessage(), containsString("connection manager is closed"));
                        onConnectException.run();
                        connectionLoopCountDown.countDown();
                    }
                };

                if (useConnectToNode) {
                    connectionManager.connectToNode(discoveryNode, connectionProfile, validator, listener);
                } else {
                    connectionManager.openConnection(discoveryNode, connectionProfile, listener.map(c -> c::close));
                }
            }
        }

        for (int i = 0; i < connectionLoops; i++) {
            threadPool.generic().execute(new ConnectionLoop());
        }

        // Create a separate thread to complete pending connection attempts, throttled by the pendingConnectionPermits semaphore
        final var completionThread = new Thread(() -> {
            while (true) {
                try {
                    assertTrue(pendingConnectionPermits.tryAcquire(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // There could still be items in the queue when we are interrupted, so drain the queue before exiting:
                    while (pendingConnectionPermits.tryAcquire()) {
                        // noinspection ConstantConditions
                        pendingConnections.poll().run();
                    }
                    return;
                }
                // noinspection ConstantConditions
                pendingConnections.poll().run();
            }
        });
        completionThread.start();

        // Steal a permit so that the consumer lags behind the producers ...
        assertTrue(pendingConnectionPermits.tryAcquire(10, TimeUnit.SECONDS));
        // ... and then send a connection attempt through the system to ensure that the lagging has started
        Releasables.closeExpectNoException(
            safeAwait(
                (ActionListener<Releasable> listener) -> connectionManager.connectToNode(
                    DiscoveryNodeUtils.create("", new TransportAddress(InetAddress.getLoopbackAddress(), 0)),
                    connectionProfile,
                    validator,
                    listener
                )
            )
        );

        // Now close the connection manager
        expectConnectionFailures.set(true);
        connectionManager.close();
        // Success! The close call returned

        // Clean up and check everything completed properly
        assertTrue(connectionLoopCountDown.await(10, TimeUnit.SECONDS));
        completionThread.interrupt();
        completionThread.join();
        assertTrue(pendingConnections.isEmpty());
    }

    public void testConcurrentConnectsAndDisconnects() throws Exception {
        final DiscoveryNode node = DiscoveryNodeUtils.create("", new TransportAddress(InetAddress.getLoopbackAddress(), 0));
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Transport.Connection> listener = (ActionListener<Transport.Connection>) invocationOnMock.getArguments()[2];
            listener.onResponse(new TestConnect(node));
            return null;
        }).when(transport).openConnection(eq(node), any(), anyActionListener());

        final Semaphore validatorPermits = new Semaphore(Integer.MAX_VALUE);

        final ConnectionManager.ConnectionValidator validator = (c, p, l) -> {
            assertTrue(validatorPermits.tryAcquire());
            randomExecutor(threadPool).execute(() -> {
                try {
                    l.onResponse(null);
                } finally {
                    validatorPermits.release();
                }
            });
        };

        final int connectionCount = between(1, 1000);
        final int disconnectionCount = randomFrom(connectionCount, connectionCount - 1, between(0, connectionCount - 1));
        final var connectionPermits = new Semaphore(connectionCount);
        final var disconnectionPermits = new Semaphore(disconnectionCount);
        final int threadCount = between(1, 10);
        final var countDownLatch = new CountDownLatch(threadCount);

        final Runnable action = new Runnable() {
            @Override
            public void run() {
                if (connectionPermits.tryAcquire()) {
                    connectionManager.connectToNode(node, null, validator, new ActionListener<>() {
                        @Override
                        public void onResponse(Releasable releasable) {
                            if (connectionManager.nodeConnected(node) == false) {
                                final String description = releasable.toString();
                                fail(description);
                            }
                            if (disconnectionPermits.tryAcquire()) {
                                Releasables.close(releasable);
                            }
                            runAgain();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (e instanceof ConnectTransportException
                                && e.getMessage().contains("concurrently connecting and disconnecting")) {
                                connectionPermits.release();
                                runAgain();
                            } else {
                                fail(e);
                            }
                        }

                        private void runAgain() {
                            threadPool.generic().execute(() -> run());
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
        assertEquals("node still connected", disconnectionCount < connectionCount, connectionManager.nodeConnected(node));
        connectionManager.close();
    }

    @TestLogging(reason = "ignore copious 'closed by remote' messages", value = "org.elasticsearch.transport.ClusterConnectionManager:WARN")
    public void testConcurrentConnectsAndCloses() throws Exception {
        final DiscoveryNode node = DiscoveryNodeUtils.create("", new TransportAddress(InetAddress.getLoopbackAddress(), 0));
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Transport.Connection> listener = (ActionListener<Transport.Connection>) invocationOnMock.getArguments()[2];
            listener.onResponse(new TestConnect(node));
            return null;
        }).when(transport).openConnection(eq(node), any(), anyActionListener());

        final Semaphore validatorPermits = new Semaphore(Integer.MAX_VALUE);

        final ConnectionManager.ConnectionValidator validator = (c, p, l) -> {
            assertTrue(validatorPermits.tryAcquire());
            randomExecutor(threadPool).execute(() -> {
                try {
                    l.onResponse(null);
                } finally {
                    validatorPermits.release();
                }
            });
        };

        final var closePermits = new Semaphore(between(1, 1000));
        final int connectThreadCount = between(1, 3);
        final int closeThreadCount = between(1, 3);
        final var countDownLatch = new CountDownLatch(connectThreadCount + closeThreadCount);

        final var cleanlyOpenedConnectionFuture = new PlainActionFuture<Boolean>();
        final var closingRefs = AbstractRefCounted.of(
            () -> connectionManager.connectToNode(
                node,
                null,
                validator,
                cleanlyOpenedConnectionFuture.map(r -> connectionManager.nodeConnected(node))
            )
        );

        final Runnable connectAction = new Runnable() {
            private void runAgain() {
                threadPool.generic().execute(this);
            }

            @Override
            public void run() {
                if (cleanlyOpenedConnectionFuture.isDone() == false) {
                    connectionManager.connectToNode(node, null, validator, new ActionListener<>() {
                        @Override
                        public void onResponse(Releasable releasable) {
                            runAgain();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (e instanceof ConnectTransportException
                                && e.getMessage().contains("concurrently connecting and disconnecting")) {
                                runAgain();
                            } else {
                                fail(e);
                            }
                        }

                    });
                } else {
                    countDownLatch.countDown();
                }
            }
        };

        final Runnable closeAction = new Runnable() {
            private void runAgain() {
                threadPool.generic().execute(this);
            }

            @Override
            public void run() {
                closingRefs.decRef();
                if (closePermits.tryAcquire() && closingRefs.tryIncRef()) {
                    try {
                        var connection = connectionManager.getConnection(node);
                        connection.addRemovedListener(ActionListener.running(this::runAgain));
                        connection.close();
                    } catch (NodeNotConnectedException e) {
                        closePermits.release();
                        runAgain();
                    }
                } else {
                    countDownLatch.countDown();
                }
            }
        };

        for (int i = 0; i < connectThreadCount; i++) {
            connectAction.run();
        }
        for (int i = 0; i < closeThreadCount; i++) {
            closingRefs.incRef();
            closeAction.run();
        }
        closingRefs.decRef();

        assertTrue("threads did not all complete", countDownLatch.await(10, TimeUnit.SECONDS));
        assertFalse(closingRefs.hasReferences());
        assertTrue(cleanlyOpenedConnectionFuture.result());

        assertTrue("validatorPermits not all released", validatorPermits.tryAcquire(Integer.MAX_VALUE, 10, TimeUnit.SECONDS));
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

        DiscoveryNode node = DiscoveryNodeUtils.create("", new TransportAddress(InetAddress.getLoopbackAddress(), 0));
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

        DiscoveryNode node = DiscoveryNodeUtils.create("", new TransportAddress(InetAddress.getLoopbackAddress(), 0));
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

    public void testConnectAfterClose() {
        connectionManager.close();
        final var node = DiscoveryNodeUtils.create("", new TransportAddress(InetAddress.getLoopbackAddress(), 0));

        final var openConnectionFuture = new PlainActionFuture<Transport.Connection>();
        connectionManager.openConnection(node, connectionProfile, openConnectionFuture);
        assertTrue(openConnectionFuture.isDone());
        assertThat(
            expectThrows(ExecutionException.class, ConnectTransportException.class, openConnectionFuture::get).getMessage(),
            containsString("connection manager is closed")
        );

        final var connectToNodeFuture = new PlainActionFuture<Releasable>();
        connectionManager.connectToNode(node, connectionProfile, (c, p, l) -> fail("should not be called"), connectToNodeFuture);
        assertTrue(connectToNodeFuture.isDone());
        assertThat(
            expectThrows(ExecutionException.class, ConnectTransportException.class, connectToNodeFuture::get).getMessage(),
            containsString("connection manager is closed")
        );
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
        public TransportVersion getTransportVersion() {
            return TransportVersion.current();
        }

        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
            throws TransportException {}
    }
}
