/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.CloseableConnection;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

public class JoinValidationServiceTests extends ESTestCase {

    public void testConcurrentBehaviour() throws Exception {
        final var releasables = new ArrayList<Releasable>();
        try {
            final var settingsBuilder = Settings.builder();
            settingsBuilder.put(
                JoinValidationService.JOIN_VALIDATION_CACHE_TIMEOUT_SETTING.getKey(),
                TimeValue.timeValueMillis(between(1, 1000))
            );
            if (randomBoolean()) {
                settingsBuilder.put("thread_pool.cluster_coordination.size", between(1, 5));
            }
            final var settings = settingsBuilder.build();

            final var threadPool = new TestThreadPool("test", settings);
            releasables.add(() -> ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));

            final var sendCountdown = new CountDownLatch(between(0, 10));

            final var transport = new MockTransport() {
                @Override
                public Connection createConnection(DiscoveryNode node) {
                    return new CloseableConnection() {
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
                            throws TransportException {
                            final var executor = threadPool.executor(
                                randomFrom(ThreadPool.Names.SAME, ThreadPool.Names.GENERIC, ThreadPool.Names.CLUSTER_COORDINATION)
                            );
                            executor.execute(new AbstractRunnable() {
                                @Override
                                public void onFailure(Exception e) {
                                    assert false : e;
                                }

                                @Override
                                public void onRejection(Exception e) {
                                    handleError(requestId, new TransportException(e));
                                }

                                @Override
                                public void doRun() {
                                    handleResponse(requestId, switch (action) {
                                        case JoinValidationService.JOIN_VALIDATE_ACTION_NAME -> TransportResponse.Empty.INSTANCE;
                                        case TransportService.HANDSHAKE_ACTION_NAME -> new TransportService.HandshakeResponse(
                                            Version.CURRENT,
                                            Build.current().hash(),
                                            node,
                                            ClusterName.DEFAULT
                                        );
                                        default -> throw new AssertionError("unexpected action: " + action);
                                    });
                                    sendCountdown.countDown();
                                }
                            });
                        }
                    };
                }
            };

            final var localNode = DiscoveryNodeUtils.create("local");

            final var transportService = new TransportService(
                settings,
                transport,
                threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                ignored -> localNode,
                null,
                Set.of()
            );
            releasables.add(transportService);

            final var clusterState = ClusterState.EMPTY_STATE;

            final var joinValidationService = new JoinValidationService(settings, transportService, () -> clusterState, List.of());

            transportService.start();
            releasables.add(() -> {
                if (transportService.lifecycleState() == Lifecycle.State.STARTED) {
                    transportService.stop();
                }
            });

            transportService.acceptIncomingRequests();

            final var otherNodes = new DiscoveryNode[between(1, 10)];
            for (int i = 0; i < otherNodes.length; i++) {
                otherNodes[i] = DiscoveryNodeUtils.create("other-" + i);
                final var connectionListener = new PlainActionFuture<Releasable>();
                transportService.connectToNode(otherNodes[i], connectionListener);
                releasables.add(connectionListener.get(10, TimeUnit.SECONDS));
            }

            final var threads = new Thread[between(1, 3)];
            final var startBarrier = new CyclicBarrier(threads.length + 1);
            final var permitCount = 100; // prevent too many concurrent requests or else cleanup can take ages
            final var validationPermits = new Semaphore(permitCount);
            final var expectFailures = new AtomicBoolean(false);
            final var keepGoing = new AtomicBoolean(true);
            for (int i = 0; i < threads.length; i++) {
                final var seed = randomLong();
                threads[i] = new Thread(() -> {
                    final var random = new Random(seed);
                    try {
                        startBarrier.await(10, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }

                    while (keepGoing.get()) {
                        Thread.yield();
                        if (validationPermits.tryAcquire()) {
                            joinValidationService.validateJoin(
                                randomFrom(random, otherNodes),
                                ActionListener.notifyOnce(new ActionListener<>() {
                                    @Override
                                    public void onResponse(TransportResponse.Empty empty) {
                                        validationPermits.release();
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        validationPermits.release();
                                        assert expectFailures.get() : e;
                                    }
                                })
                            );
                        }
                    }
                }, "join-validating-thread-" + i);
                threads[i].start();
            }

            startBarrier.await(10, TimeUnit.SECONDS);
            assertTrue(sendCountdown.await(10, TimeUnit.SECONDS));

            expectFailures.set(true);
            switch (between(1, 3)) {
                case 1 -> joinValidationService.stop();
                case 2 -> {
                    joinValidationService.stop();
                    transportService.close();
                    ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
                }
                case 3 -> {
                    transportService.close();
                    keepGoing.set(false); // else the test threads keep adding to the validation service queue so the processor never stops
                    ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
                    joinValidationService.stop();
                }
            }
            keepGoing.set(false);
            for (final var thread : threads) {
                thread.join();
            }
            assertTrue(validationPermits.tryAcquire(permitCount, 10, TimeUnit.SECONDS));
            assertBusy(() -> assertTrue(joinValidationService.isIdle()));
        } finally {
            Collections.reverse(releasables);
            Releasables.close(releasables);
        }
    }

    public void testJoinValidationRejectsUnreadableClusterState() {

        class BadCustom implements SimpleDiffable<ClusterState.Custom>, ClusterState.Custom {

            @Override
            public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
                return Collections.emptyIterator();
            }

            @Override
            public String getWriteableName() {
                return "deliberately-unknown";
            }

            @Override
            public TransportVersion getMinimalSupportedVersion() {
                return TransportVersion.current();
            }

            @Override
            public void writeTo(StreamOutput out) {}
        }

        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var clusterState = ClusterState.builder(ClusterName.DEFAULT).putCustom("test", new BadCustom()).build();

        final var joiningNode = DiscoveryNodeUtils.create("joining");
        final var joiningNodeTransport = new MockTransport();
        final var joiningNodeTransportService = joiningNodeTransport.createTransportService(
            Settings.EMPTY,
            deterministicTaskQueue.getThreadPool(),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> joiningNode,
            null,
            Collections.emptySet()
        );
        new JoinValidationService(Settings.EMPTY, joiningNodeTransportService, () -> clusterState, List.of()); // registers request handler
        joiningNodeTransportService.start();
        joiningNodeTransportService.acceptIncomingRequests();

        final var masterNode = DiscoveryNodeUtils.create("node0");
        final var masterTransport = new MockTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                assertSame(node, joiningNode);
                assertEquals(JoinValidationService.JOIN_VALIDATE_ACTION_NAME, action);

                final var listener = new ActionListener<TransportResponse>() {
                    @Override
                    public void onResponse(TransportResponse transportResponse) {
                        fail("should not succeed");
                    }

                    @Override
                    public void onFailure(Exception e) {
                        handleError(requestId, new RemoteTransportException(node.getName(), node.getAddress(), action, e));
                    }
                };

                try (var out = new BytesStreamOutput()) {
                    request.writeTo(out);
                    out.flush();
                    final var handler = joiningNodeTransport.getRequestHandlers().getHandler(action);
                    handler.processMessageReceived(
                        handler.newRequest(new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writeableRegistry())),
                        new TestTransportChannel(listener)
                    );
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }
        };
        final var masterTransportService = masterTransport.createTransportService(
            Settings.EMPTY,
            deterministicTaskQueue.getThreadPool(),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> masterNode,
            null,
            Collections.emptySet()
        );
        final var joinValidationService = new JoinValidationService(Settings.EMPTY, masterTransportService, () -> clusterState, List.of());
        masterTransportService.start();
        masterTransportService.acceptIncomingRequests();

        try {
            final var future = new PlainActionFuture<TransportResponse.Empty>();
            joinValidationService.validateJoin(joiningNode, future);
            assertFalse(future.isDone());
            deterministicTaskQueue.runAllTasks();
            assertTrue(future.isDone());
            assertThat(
                expectThrows(IllegalArgumentException.class, future::actionGet).getMessage(),
                allOf(containsString("Unknown NamedWriteable"), containsString("deliberately-unknown"))
            );
        } finally {
            joinValidationService.stop();
            masterTransportService.close();
            joiningNodeTransportService.close();
        }
    }

    public void testJoinValidationRejectsMismatchedClusterUUID() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var mockTransport = new MockTransport();
        final var localNode = DiscoveryNodeUtils.create("node0");

        final var localClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().generateClusterUuidIfNeeded().clusterUUIDCommitted(true))
            .build();

        final var transportService = mockTransport.createTransportService(
            Settings.EMPTY,
            deterministicTaskQueue.getThreadPool(),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            ignored -> localNode,
            null,
            Set.of()
        );

        final var dataPath = "/my/data/path";
        final var settings = Settings.builder().put(Environment.PATH_DATA_SETTING.getKey(), dataPath).build();
        new JoinValidationService(settings, transportService, () -> localClusterState, List.of()); // registers request handler
        transportService.start();
        transportService.acceptIncomingRequests();

        final var otherClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().generateClusterUuidIfNeeded())
            .build();

        final var future = new PlainActionFuture<TransportResponse.Empty>();
        transportService.sendRequest(
            localNode,
            JoinValidationService.JOIN_VALIDATE_ACTION_NAME,
            new ValidateJoinRequest(otherClusterState),
            new ActionListenerResponseHandler<>(future, in -> TransportResponse.Empty.INSTANCE)
        );
        deterministicTaskQueue.runAllTasks();

        assertThat(
            expectThrows(CoordinationStateRejectedException.class, future::actionGet).getMessage(),
            allOf(
                containsString("This node previously joined a cluster with UUID"),
                containsString("and is now trying to join a different cluster"),
                containsString(localClusterState.metadata().clusterUUID()),
                containsString(otherClusterState.metadata().clusterUUID()),
                containsString("data path [" + dataPath + "]")
            )
        );
    }

    public void testJoinValidationRunsJoinValidators() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var mockTransport = new MockTransport();
        final var localNode = DiscoveryNodeUtils.create("node0");
        final var localClusterState = ClusterState.builder(ClusterName.DEFAULT).build();

        final var transportService = mockTransport.createTransportService(
            Settings.EMPTY,
            deterministicTaskQueue.getThreadPool(),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            ignored -> localNode,
            null,
            Set.of()
        );

        final var stateForValidation = ClusterState.builder(ClusterName.DEFAULT).build();
        new JoinValidationService(Settings.EMPTY, transportService, () -> localClusterState, List.of((node, state) -> {
            assertSame(node, localNode);
            assertSame(state, stateForValidation);
            throw new IllegalStateException("simulated validation failure");
        })); // registers request handler
        transportService.start();
        transportService.acceptIncomingRequests();

        final var future = new PlainActionFuture<TransportResponse.Empty>();
        transportService.sendRequest(
            localNode,
            JoinValidationService.JOIN_VALIDATE_ACTION_NAME,
            new ValidateJoinRequest(stateForValidation),
            new ActionListenerResponseHandler<>(future, in -> TransportResponse.Empty.INSTANCE)
        );
        deterministicTaskQueue.runAllTasks();

        assertThat(
            expectThrows(IllegalStateException.class, future::actionGet).getMessage(),
            allOf(containsString("simulated validation failure"))
        );
    }
}
