/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.service;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptySet;
import static org.elasticsearch.test.ClusterServiceUtils.createNoOpNodeConnectionsService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class ClusterApplierServiceTests extends ESTestCase {

    private ThreadPool threadPool;
    private long currentTimeMillis;
    private boolean allowClusterStateApplicationFailure = false;
    private ClusterApplierService clusterApplierService;
    private ClusterSettings clusterSettings;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(ClusterApplierServiceTests.class.getName()) {
            @Override
            public long relativeTimeInMillis() {
                assertThat(Thread.currentThread().getName(), containsString(ClusterApplierService.CLUSTER_UPDATE_THREAD_NAME));
                return currentTimeMillis;
            }
        };
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        allowClusterStateApplicationFailure = false;
        clusterApplierService = createClusterApplierService(true);
    }

    @After
    public void tearDown() throws Exception {
        clusterApplierService.close();
        if (threadPool != null) {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
        super.tearDown();
    }

    private ClusterApplierService createClusterApplierService(boolean makeMaster) {
        final DiscoveryNode localNode = DiscoveryNodeUtils.builder("node1").roles(emptySet()).build();
        final ClusterApplierService clusterApplierService = new ClusterApplierService(
            "test_node",
            Settings.builder().put("cluster.name", "ClusterApplierServiceTests").build(),
            clusterSettings,
            threadPool
        ) {
            @Override
            protected boolean applicationMayFail() {
                return allowClusterStateApplicationFailure;
            }
        };
        clusterApplierService.setNodeConnectionsService(createNoOpNodeConnectionsService());
        clusterApplierService.setInitialState(
            ClusterState.builder(new ClusterName("ClusterApplierServiceTests"))
                .nodes(
                    DiscoveryNodes.builder()
                        .add(localNode)
                        .localNodeId(localNode.getId())
                        .masterNodeId(makeMaster ? localNode.getId() : null)
                )
                .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
                .build()
        );
        clusterApplierService.start();
        return clusterApplierService;
    }

    private void advanceTime(long millis) {
        // time is only read/written on applier thread, so no synchronization is needed
        assertThat(Thread.currentThread().getName(), containsString(ClusterApplierService.CLUSTER_UPDATE_THREAD_NAME));
        currentTimeMillis += millis;
    }

    @TestLogging(value = "org.elasticsearch.cluster.service:TRACE", reason = "to ensure that we log cluster state events on TRACE level")
    public void testClusterStateUpdateLogging() throws Exception {
        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test1",
                ClusterApplierService.class.getCanonicalName(),
                Level.DEBUG,
                "*processing [test1]: took [1s] no change in cluster state"
            )
        );
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test2",
                ClusterApplierService.class.getCanonicalName(),
                Level.TRACE,
                "*failed to execute cluster state applier in [2s]*"
            )
        );
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test3",
                ClusterApplierService.class.getCanonicalName(),
                Level.DEBUG,
                "*processing [test3]: took [0s] no change in cluster state*"
            )
        );

        Logger clusterLogger = LogManager.getLogger(ClusterApplierService.class);
        Loggers.addAppender(clusterLogger, mockAppender);
        try {
            currentTimeMillis = randomLongBetween(0L, Long.MAX_VALUE / 2);
            clusterApplierService.runOnApplierThread(
                "test1",
                Priority.HIGH,
                currentState -> advanceTime(TimeValue.timeValueSeconds(1).millis()),
                new ActionListener<>() {
                    @Override
                    public void onResponse(Void ignored) {}

                    @Override
                    public void onFailure(Exception e) {
                        fail();
                    }
                }
            );
            clusterApplierService.runOnApplierThread("test2", Priority.HIGH, currentState -> {
                advanceTime(TimeValue.timeValueSeconds(2).millis());
                throw new IllegalArgumentException("Testing handling of exceptions in the cluster state task");
            }, new ActionListener<>() {
                @Override
                public void onResponse(Void ignored) {
                    fail();
                }

                @Override
                public void onFailure(Exception e) {}
            });
            // Additional update task to make sure all previous logging made it to the loggerName
            clusterApplierService.runOnApplierThread("test3", Priority.HIGH, currentState -> {}, new ActionListener<>() {
                @Override
                public void onResponse(Void ignored) {}

                @Override
                public void onFailure(Exception e) {
                    fail();
                }
            });
            assertBusy(mockAppender::assertAllExpectationsMatched);
        } finally {
            Loggers.removeAppender(clusterLogger, mockAppender);
            mockAppender.stop();
        }
    }

    @TestLogging(value = "org.elasticsearch.cluster.service:WARN", reason = "to ensure that we log cluster state events on WARN level")
    public void testLongClusterStateUpdateLogging() throws Exception {
        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(
            new MockLogAppender.UnseenEventExpectation(
                "test1 shouldn't see because setting is too low",
                ClusterApplierService.class.getCanonicalName(),
                Level.WARN,
                "*cluster state applier task [test1] took [*] which is above the warn threshold of *"
            )
        );
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test2",
                ClusterApplierService.class.getCanonicalName(),
                Level.WARN,
                "*cluster state applier task [test2] took [32s] which is above the warn threshold of [*]: "
                    + "[running task [test2]] took [*"
            )
        );
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test4",
                ClusterApplierService.class.getCanonicalName(),
                Level.WARN,
                "*cluster state applier task [test3] took [34s] which is above the warn threshold of [*]: "
                    + "[running task [test3]] took [*"
            )
        );

        Logger clusterLogger = LogManager.getLogger(ClusterApplierService.class);
        Loggers.addAppender(clusterLogger, mockAppender);
        try {
            final CountDownLatch latch = new CountDownLatch(4);
            final CountDownLatch processedFirstTask = new CountDownLatch(1);
            currentTimeMillis = randomLongBetween(0L, Long.MAX_VALUE / 2);
            clusterApplierService.runOnApplierThread(
                "test1",
                Priority.HIGH,
                currentState -> advanceTime(TimeValue.timeValueSeconds(1).millis()),
                new ActionListener<>() {
                    @Override
                    public void onResponse(Void ignored) {
                        latch.countDown();
                        processedFirstTask.countDown();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        fail();
                    }
                }
            );
            processedFirstTask.await();
            clusterApplierService.runOnApplierThread("test2", Priority.HIGH, currentState -> {
                advanceTime(TimeValue.timeValueSeconds(32).millis());
                throw new IllegalArgumentException("Testing handling of exceptions in the cluster state task");
            }, new ActionListener<>() {
                @Override
                public void onResponse(Void ignored) {
                    fail();
                }

                @Override
                public void onFailure(Exception e) {
                    latch.countDown();
                }
            });
            clusterApplierService.runOnApplierThread(
                "test3",
                Priority.HIGH,
                currentState -> advanceTime(TimeValue.timeValueSeconds(34).millis()),
                new ActionListener<>() {
                    @Override
                    public void onResponse(Void ignored) {
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        fail();
                    }
                }
            );
            // Additional update task to make sure all previous logging made it to the loggerName
            // We don't check logging for this on since there is no guarantee that it will occur before our check
            clusterApplierService.runOnApplierThread("test4", Priority.HIGH, currentState -> {}, new ActionListener<>() {
                @Override
                public void onResponse(Void ignored) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail();
                }
            });
            latch.await();
        } finally {
            Loggers.removeAppender(clusterLogger, mockAppender);
            mockAppender.stop();
        }
        mockAppender.assertAllExpectationsMatched();
    }

    public void testLocalNodeMasterListenerCallbacks() {
        ClusterApplierService clusterApplierService = createClusterApplierService(false);

        AtomicBoolean isMaster = new AtomicBoolean();
        clusterApplierService.addLocalNodeMasterListener(new LocalNodeMasterListener() {
            @Override
            public void onMaster() {
                isMaster.set(true);
            }

            @Override
            public void offMaster() {
                isMaster.set(false);
            }
        });

        ClusterState state = clusterApplierService.state();
        DiscoveryNodes nodes = state.nodes();
        state = ClusterState.builder(state)
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .nodes(nodes.withMasterNodeId(nodes.getLocalNodeId()))
            .build();
        setState(clusterApplierService, state);
        assertThat(isMaster.get(), is(true));

        nodes = state.nodes();
        state = ClusterState.builder(state)
            .blocks(ClusterBlocks.builder().addGlobalBlock(NoMasterBlockService.NO_MASTER_BLOCK_WRITES))
            .nodes(nodes.withMasterNodeId(null))
            .build();
        setState(clusterApplierService, state);
        assertThat(isMaster.get(), is(false));
        state = ClusterState.builder(state)
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .nodes(nodes.withMasterNodeId(nodes.getLocalNodeId()))
            .build();
        setState(clusterApplierService, state);
        assertThat(isMaster.get(), is(true));

        clusterApplierService.close();
    }

    public void testClusterStateApplierCantSampleClusterState() throws InterruptedException {
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicBoolean applierCalled = new AtomicBoolean();
        clusterApplierService.addStateApplier(event -> {
            try {
                applierCalled.set(true);
                clusterApplierService.state();
                error.set(new AssertionError("successfully sampled state"));
            } catch (AssertionError e) {
                if (e.getMessage().contains("should not be called by a cluster state applier") == false) {
                    error.set(e);
                }
            }
        });

        CountDownLatch latch = new CountDownLatch(1);
        clusterApplierService.onNewClusterState(
            "test",
            () -> ClusterState.builder(clusterApplierService.state()).build(),
            new ActionListener<>() {

                @Override
                public void onResponse(Void ignored) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    error.compareAndSet(null, e);
                }
            }
        );

        latch.await();
        assertNull(error.get());
        assertTrue(applierCalled.get());
    }

    public void testClusterStateApplierBubblesUpExceptionsInApplier() throws InterruptedException {
        AtomicReference<Throwable> error = new AtomicReference<>();
        clusterApplierService.addStateApplier(event -> { throw new RuntimeException("dummy exception"); });
        allowClusterStateApplicationFailure = true;

        CountDownLatch latch = new CountDownLatch(1);
        clusterApplierService.onNewClusterState(
            "test",
            () -> ClusterState.builder(clusterApplierService.state()).build(),
            new ActionListener<>() {

                @Override
                public void onResponse(Void ignored) {
                    latch.countDown();
                    fail("should not be called");
                }

                @Override
                public void onFailure(Exception e) {
                    assertTrue(error.compareAndSet(null, e));
                    latch.countDown();
                }
            }
        );

        latch.await();
        assertNotNull(error.get());
        assertThat(error.get().getMessage(), containsString("dummy exception"));
    }

    public void testClusterStateApplierBubblesUpExceptionsInSettingsApplier() throws InterruptedException {
        AtomicReference<Throwable> error = new AtomicReference<>();
        clusterSettings.addSettingsUpdateConsumer(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING, v -> {});
        allowClusterStateApplicationFailure = true;

        CountDownLatch latch = new CountDownLatch(1);
        clusterApplierService.onNewClusterState(
            "test",
            () -> ClusterState.builder(clusterApplierService.state())
                .metadata(
                    Metadata.builder(clusterApplierService.state().metadata())
                        .persistentSettings(
                            Settings.builder()
                                .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), false)
                                .build()
                        )
                        .build()
                )
                .build(),
            new ActionListener<>() {

                @Override
                public void onResponse(Void ignored) {
                    latch.countDown();
                    fail("should not be called");
                }

                @Override
                public void onFailure(Exception e) {
                    assertTrue(error.compareAndSet(null, e));
                    latch.countDown();
                }
            }
        );

        latch.await();
        assertNotNull(error.get());
        assertThat(error.get().getMessage(), containsString("illegal value can't update"));
    }

    public void testClusterStateApplierSwallowsExceptionInListener() throws InterruptedException {
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicBoolean applierCalled = new AtomicBoolean();
        clusterApplierService.addListener(event -> {
            assertTrue(applierCalled.compareAndSet(false, true));
            throw new RuntimeException("dummy exception");
        });

        CountDownLatch latch = new CountDownLatch(1);
        clusterApplierService.onNewClusterState(
            "test",
            () -> ClusterState.builder(clusterApplierService.state()).build(),
            new ActionListener<>() {

                @Override
                public void onResponse(Void ignored) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    error.compareAndSet(null, e);
                }
            }
        );

        latch.await();
        assertNull(error.get());
        assertTrue(applierCalled.get());
    }

    public void testClusterStateApplierCanCreateAnObserver() throws InterruptedException {
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicBoolean applierCalled = new AtomicBoolean();
        clusterApplierService.addStateApplier(event -> {
            try {
                applierCalled.set(true);
                ClusterStateObserver observer = new ClusterStateObserver(
                    event.state().version(),
                    clusterApplierService,
                    null,
                    logger,
                    threadPool.getThreadContext()
                );
                observer.waitForNextChange(new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {

                    }

                    @Override
                    public void onClusterServiceClose() {

                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {

                    }
                });
            } catch (AssertionError e) {
                error.set(e);
            }
        });

        CountDownLatch latch = new CountDownLatch(1);
        clusterApplierService.onNewClusterState(
            "test",
            () -> ClusterState.builder(clusterApplierService.state()).build(),
            new ActionListener<>() {
                @Override
                public void onResponse(Void ignored) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    error.compareAndSet(null, e);
                }
            }
        );

        latch.await();
        assertNull(error.get());
        assertTrue(applierCalled.get());
    }

    public void testThreadContext() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        try (ThreadContext.StoredContext ignored = threadPool.getThreadContext().stashContext()) {
            final Map<String, String> expectedHeaders = Collections.singletonMap("test", "test");
            final Map<String, List<String>> expectedResponseHeaders = Collections.singletonMap(
                "testResponse",
                Collections.singletonList("testResponse")
            );
            threadPool.getThreadContext().putHeader(expectedHeaders);

            clusterApplierService.onNewClusterState("test", () -> {
                assertTrue(threadPool.getThreadContext().isSystemContext());
                assertEquals(Collections.emptyMap(), threadPool.getThreadContext().getHeaders());
                threadPool.getThreadContext().addResponseHeader("testResponse", "testResponse");
                assertEquals(expectedResponseHeaders, threadPool.getThreadContext().getResponseHeaders());
                if (randomBoolean()) {
                    return ClusterState.builder(clusterApplierService.state()).build();
                } else {
                    throw new IllegalArgumentException("mock failure");
                }
            }, new ActionListener<>() {

                @Override
                public void onResponse(Void ignored) {
                    assertFalse(threadPool.getThreadContext().isSystemContext());
                    assertEquals(expectedHeaders, threadPool.getThreadContext().getHeaders());
                    assertEquals(expectedResponseHeaders, threadPool.getThreadContext().getResponseHeaders());
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    assertFalse(threadPool.getThreadContext().isSystemContext());
                    assertEquals(expectedHeaders, threadPool.getThreadContext().getHeaders());
                    assertEquals(expectedResponseHeaders, threadPool.getThreadContext().getResponseHeaders());
                    latch.countDown();
                }
            });
        }

        latch.await();
    }

}
