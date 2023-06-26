/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.routing;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class BatchedRerouteServiceTests extends ESTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;

    @Before
    public void beforeTest() {
        threadPool = new TestThreadPool("test");
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
    }

    @After
    public void afterTest() {
        clusterService.stop();
        threadPool.shutdown();
    }

    public void testReroutesWhenRequested() throws InterruptedException {
        final AtomicLong rerouteCount = new AtomicLong();
        final BatchedRerouteService batchedRerouteService = new BatchedRerouteService(clusterService, (s, r, l) -> {
            rerouteCount.incrementAndGet();
            l.onResponse(null);
            return s;
        });

        long rerouteCountBeforeReroute = 0L;
        final int iterations = between(1, 100);
        final CountDownLatch countDownLatch = new CountDownLatch(iterations);
        for (int i = 0; i < iterations; i++) {
            rerouteCountBeforeReroute = Math.max(rerouteCountBeforeReroute, rerouteCount.get());
            batchedRerouteService.reroute(
                "iteration " + i,
                randomFrom(EnumSet.allOf(Priority.class)),
                ActionListener.running(countDownLatch::countDown)
            );
        }
        assertTrue(countDownLatch.await(10, TimeUnit.SECONDS));
        assertThat(rerouteCountBeforeReroute, lessThan(rerouteCount.get()));
    }

    public void testBatchesReroutesTogetherAtPriorityOfHighestSubmittedReroute() {
        final CyclicBarrier cyclicBarrier = new CyclicBarrier(2);
        // notify test that we are blocked
        // wait to be unblocked by test
        clusterService.submitUnbatchedStateUpdateTask("block master service", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                safeAwait(cyclicBarrier); // notify test that we are blocked
                safeAwait(cyclicBarrier); // wait to be unblocked by test
                return currentState;
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("block master service", e);
            }
        });

        safeAwait(cyclicBarrier); // wait for master thread to be blocked

        final AtomicBoolean rerouteExecuted = new AtomicBoolean();
        final BatchedRerouteService batchedRerouteService = new BatchedRerouteService(clusterService, (s, r, l) -> {
            assertTrue(rerouteExecuted.compareAndSet(false, true)); // only called once
            l.onResponse(null);
            return s;
        });

        final ThreadContext threadContext = threadPool.getThreadContext();
        final String contextHeader = "test-context-header";

        final int iterations = scaledRandomIntBetween(1, 100);
        final CountDownLatch tasksSubmittedCountDown = new CountDownLatch(iterations);
        final CountDownLatch tasksCompletedCountDown = new CountDownLatch(iterations);
        final List<Runnable> actions = new ArrayList<>(iterations);
        final Function<Priority, Runnable> rerouteFromPriority = priority -> () -> {
            final AtomicBoolean alreadyRun = new AtomicBoolean();
            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                final String contextValue = randomAlphaOfLength(10);
                threadContext.putHeader(contextHeader, contextValue);
                batchedRerouteService.reroute("reroute at " + priority, priority, ActionListener.running(() -> {
                    assertTrue(alreadyRun.compareAndSet(false, true));
                    assertThat(threadContext.getHeader(contextHeader), equalTo(contextValue));
                    tasksCompletedCountDown.countDown();
                }));
            }
            tasksSubmittedCountDown.countDown();
        };
        actions.add(rerouteFromPriority.apply(Priority.URGENT)); // ensure at least one URGENT priority reroute
        for (int i = 1; i < iterations; i++) {
            if (randomBoolean()) {
                actions.add(rerouteFromPriority.apply(randomFrom(Priority.LOW, Priority.NORMAL, Priority.HIGH, Priority.URGENT)));
            } else {
                final Priority priority = randomFrom(Priority.NORMAL, Priority.HIGH, Priority.URGENT, Priority.IMMEDIATE);
                final boolean submittedConcurrentlyWithReroute = randomBoolean();
                if (submittedConcurrentlyWithReroute == false) {
                    tasksSubmittedCountDown.countDown(); // this task might be submitted later
                }
                final String source = "other task " + i + " at " + priority;
                actions.add(() -> {
                    // else this task might be submitted too late to precede the reroute
                    // may run either before or after reroute
                    clusterService.submitUnbatchedStateUpdateTask(source, new ClusterStateUpdateTask(priority) {

                        @Override
                        public ClusterState execute(ClusterState currentState) {
                            switch (priority) {
                                case IMMEDIATE:
                                    if (submittedConcurrentlyWithReroute) {
                                        assertFalse("should have rerouted after " + priority + " priority task", rerouteExecuted.get());
                                    } // else this task might be submitted too late to precede the reroute
                                    break;
                                case URGENT:
                                    // may run either before or after reroute
                                    break;
                                case HIGH:
                                case NORMAL:
                                    assertTrue("should have rerouted before " + priority + " priority task", rerouteExecuted.get());
                                    break;
                                default:
                                    fail("unexpected priority: " + priority);
                                    break;
                            }
                            return currentState;
                        }

                        @Override
                        public void onFailure(Exception e) {
                            throw new AssertionError(source, e);
                        }

                        @Override
                        public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                            tasksCompletedCountDown.countDown();
                        }
                    });
                    if (submittedConcurrentlyWithReroute) {
                        tasksSubmittedCountDown.countDown();
                    }
                });
            }
        }
        Randomness.shuffle(actions);
        actions.forEach(threadPool.generic()::execute);
        safeAwait(tasksSubmittedCountDown);

        safeAwait(cyclicBarrier); // allow master thread to continue;
        safeAwait(tasksCompletedCountDown); // wait for reroute to complete
        assertTrue(rerouteExecuted.get()); // see above for assertion that it's only called once
    }

    public void testNotifiesOnFailure() throws InterruptedException {

        final BatchedRerouteService batchedRerouteService = new BatchedRerouteService(clusterService, (s, r, l) -> {
            if (rarely()) {
                throw new ElasticsearchException("simulated");
            }
            l.onResponse(null);
            return randomBoolean() ? s : ClusterState.builder(s).build();
        });

        final int iterations = between(1, 100);
        final CountDownLatch countDownLatch = new CountDownLatch(iterations);
        for (int i = 0; i < iterations; i++) {
            batchedRerouteService.reroute(
                "iteration " + i,
                randomFrom(EnumSet.allOf(Priority.class)),
                ActionListener.runAfter(ActionListener.wrap(r -> {
                    if (rarely()) {
                        throw new ElasticsearchException("failure during notification");
                    }
                }, e -> {}), countDownLatch::countDown)
            );
            if (rarely()) {
                clusterService.getMasterService()
                    .setClusterStatePublisher(
                        randomBoolean()
                            ? ClusterServiceUtils.createClusterStatePublisher(clusterService.getClusterApplierService())
                            : (event, publishListener, ackListener) -> publishListener.onFailure(
                                new FailedToCommitClusterStateException("simulated")
                            )
                    );
            }

            if (rarely()) {
                clusterService.getClusterApplierService().onNewClusterState("simulated", () -> {
                    ClusterState state = clusterService.state();
                    return ClusterState.builder(state)
                        .nodes(state.nodes().withMasterNodeId(randomBoolean() ? null : state.nodes().getLocalNodeId()))
                        .build();
                }, ActionListener.noop());
            }
        }

        assertTrue(countDownLatch.await(10, TimeUnit.SECONDS)); // i.e. it doesn't leak any listeners
    }
}
