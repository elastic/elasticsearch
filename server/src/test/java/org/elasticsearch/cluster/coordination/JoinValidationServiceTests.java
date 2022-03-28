/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.CloseableConnection;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
                                            Build.CURRENT.hash(),
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

            final var localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT);

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
                otherNodes[i] = new DiscoveryNode("other-" + i, buildNewFakeTransportAddress(), Version.CURRENT);
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
}
