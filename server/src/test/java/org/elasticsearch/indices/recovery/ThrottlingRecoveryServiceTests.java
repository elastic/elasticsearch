/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.indices.recovery.ThrottlingRecoveryService.INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ThrottlingRecoveryServiceTests extends ESTestCase {
    private static TestThreadPool threadPool;

    @BeforeClass
    public static void init() throws Exception {
        threadPool = new TestThreadPool(ThrottlingRecoveryServiceTests.class.getSimpleName());
    }

    @AfterClass
    public static void close() throws Exception {
        terminate(threadPool);
    }

    public void testSynchronousTaskRunsOnProvidedThreadPoolAndNotifiesUserListener() {
        // Use real threads instead of DeterministicTaskQueue to verify actual threading behavior below
        final var service = new ThrottlingRecoveryService(threadPool.generic(), newClusterService(1));
        final var callerThread = Thread.currentThread();
        final var executionThread = new AtomicReference<Thread>();
        final var consumerReturned = new CountDownLatch(1);
        final var recoveryDone = new AtomicBoolean(false);
        final var userListener = new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {
                recoveryDone.set(true);
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                fail(e);
            }

            @Override
            public void onRecoveryAborted() {
                fail("recovery aborted");
            }
        };
        service.enqueue(userListener, fakeRecoveryState(), schedulingListener -> {
            executionThread.set(Thread.currentThread());
            schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
            assertTrue("user listener should have been notified of completion", recoveryDone.get());
            consumerReturned.countDown();
        });
        safeAwait(consumerReturned);
        assertThat("recovery executed on enqueueing thread instead of generic pool", executionThread.get(), not(equalTo(callerThread)));
        assertThat(service.currentQueueSize(), equalTo(0));
    }

    /// Asynchronous task: consumer returns before the scheduling listener receives a terminal callback.
    public void testAsynchronousTaskListenerNotificationAfterConsumerReturns() {
        // Use real threads instead of DeterministicTaskQueue to be able to use safeAwait below
        final var service = new ThrottlingRecoveryService(threadPool.generic(), newClusterService(1));
        final var consumerReturned = new CountDownLatch(1);
        final var recoveryDone = new CountDownLatch(1);
        final var userListener = new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {
                assertThat("terminal callback should follow consumer return", consumerReturned.getCount(), equalTo(0L));
                recoveryDone.countDown();
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                fail(e);
            }

            @Override
            public void onRecoveryAborted() {
                fail("recovery aborted");
            }
        };
        service.enqueue(userListener, fakeRecoveryState(), schedulingListener -> {
            threadPool.generic().execute(() -> {
                safeAwait(consumerReturned);
                schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
            });
            consumerReturned.countDown();
        });
        safeAwait(recoveryDone);
        assertThat(service.currentQueueSize(), equalTo(0));
    }

    public void testMaxConcurrencyBoundWithAsynchronousTasks() {
        final var taskQueue = new DeterministicTaskQueue();
        final var executor = taskQueue.getThreadPool().generic();
        final int maxConcurrentRecoveries = between(2, 5);
        final var service = new ThrottlingRecoveryService(executor, newClusterService(maxConcurrentRecoveries));
        final var running = new AtomicInteger();
        final var completed = new AtomicInteger();
        final var peakConcurrent = new AtomicInteger();
        final int totalEnqueuedTasks = maxConcurrentRecoveries * 3;

        RecoveryListener trackingListener = new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {
                running.decrementAndGet();
                completed.incrementAndGet();
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                fail(e);
            }

            @Override
            public void onRecoveryAborted() {
                fail("recovery aborted");
            }
        };

        long initialTime = taskQueue.getCurrentTimeMillis();
        for (int i = 0; i < totalEnqueuedTasks; i++) {
            final var ordinal = i;
            service.enqueue(trackingListener, fakeRecoveryState(), schedulingListener -> {
                int current = running.incrementAndGet();
                peakConcurrent.accumulateAndGet(current, Integer::max);
                // Schedule completion for the future
                final var minDelay = ((ordinal / maxConcurrentRecoveries) + 1) * 100 + 1;
                final var maxDelay = minDelay + 99;
                taskQueue.scheduleAt(
                    initialTime + randomIntBetween(minDelay, maxDelay),
                    () -> schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY)
                );
            });
        }
        taskQueue.runAllRunnableTasks();
        assertThat(running.get(), equalTo(maxConcurrentRecoveries));
        assertThat(completed.get(), equalTo(0));

        // Complete first batch
        taskQueue.runTasksUpToTimeInOrder(initialTime + 200);
        assertThat(running.get(), equalTo(maxConcurrentRecoveries));
        assertThat(completed.get(), equalTo(maxConcurrentRecoveries));

        // Complete second batch
        taskQueue.runTasksUpToTimeInOrder(initialTime + 300);
        assertThat(running.get(), equalTo(maxConcurrentRecoveries));
        assertThat(completed.get(), equalTo(maxConcurrentRecoveries * 2));

        // Complete all
        taskQueue.runAllTasks();
        assertThat(running.get(), equalTo(0));
        assertThat(service.currentQueueSize(), equalTo(0));
        assertThat(completed.get(), equalTo(totalEnqueuedTasks));
        assertThat(peakConcurrent.get(), equalTo(maxConcurrentRecoveries));
    }

    public void testIncreasingMaxConcurrentRecoveriesStartsPendingTasks() {
        final var taskQueue = new DeterministicTaskQueue();
        final var clusterService = newClusterService(2);
        final var service = new ThrottlingRecoveryService(taskQueue.getThreadPool().generic(), clusterService);
        final var started = new AtomicInteger();

        // Delay completion until we explicitly trigger time jump
        final long completionTime = taskQueue.getCurrentTimeMillis() + 100;
        for (int i = 0; i < 10; i++) {
            service.enqueue(RecoveryListener.NOOP, fakeRecoveryState(), schedulingListener -> {
                started.incrementAndGet();
                taskQueue.scheduleAt(
                    completionTime,
                    () -> schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY)
                );
            });
        }

        taskQueue.runAllRunnableTasks();
        assertThat(started.get(), equalTo(2));

        clusterService.getClusterSettings()
            .applySettings(Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), 4).build());
        taskQueue.runAllRunnableTasks();
        assertThat(started.get(), equalTo(4));
        taskQueue.runAllTasks();
        assertThat(started.get(), equalTo(10));
        assertThat(service.currentQueueSize(), equalTo(0));
    }

    public void testDecreasingMaxConcurrentRecoveriesDefersQueueWithoutCancellingRunningTasks() {
        final var taskQueue = new DeterministicTaskQueue();
        final var clusterService = newClusterService(3);
        final var service = new ThrottlingRecoveryService(taskQueue.getThreadPool().generic(), clusterService);
        final var started = new AtomicInteger();
        final var done = new AtomicInteger();

        // Delay completion until we explicitly trigger time jump
        final long initialTime = taskQueue.getCurrentTimeMillis();
        for (int i = 0; i < 6; i++) {
            final var ordinal = i;
            service.enqueue(new RecoveryListener() {
                @Override
                public void onRecoveryDone(
                    RecoveryState state,
                    ShardLongFieldRange timestampMillisFieldRange,
                    ShardLongFieldRange eventIngestedMillisFieldRange
                ) {
                    done.incrementAndGet();
                }

                @Override
                public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                    fail("unexpected recovery failure");
                }

                @Override
                public void onRecoveryAborted() {
                    fail("unexpected recovery abortion");
                }
            }, fakeRecoveryState(), schedulingListener -> {
                started.incrementAndGet();
                taskQueue.scheduleAt(
                    initialTime + 100 + ordinal,
                    () -> schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY)
                );
            });
        }

        taskQueue.runAllRunnableTasks();
        assertThat(started.get(), equalTo(3));
        assertThat(done.get(), equalTo(0));

        // Lower limit to 1
        clusterService.getClusterSettings()
            .applySettings(Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), 1).build());

        // Complete one task, should not start more (still have 2 running)
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
        assertThat(started.get(), equalTo(3));
        assertThat(done.get(), equalTo(1));

        // Complete second, should not start more (still have 1 running)
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
        assertThat(started.get(), equalTo(3));
        assertThat(done.get(), equalTo(2));

        // Complete third, now we're at 0 running, so should start the 4th
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
        assertThat(started.get(), equalTo(4));
        assertThat(done.get(), equalTo(3));

        // Complete remaining
        taskQueue.runAllTasks();
        assertThat(started.get(), equalTo(6));
        assertThat(done.get(), equalTo(6));
        assertThat(service.currentQueueSize(), equalTo(0));
    }

    public void testFifoWhenThrottledToOneConcurrentWithSynchronousCompletion() {
        final var taskQueue = new DeterministicTaskQueue();
        final var service = new ThrottlingRecoveryService(taskQueue.getThreadPool().generic(), newClusterService(1));
        final int total = between(10, 20);
        final var completionOrder = new CopyOnWriteArrayList<Integer>();

        for (int i = 0; i < total; i++) {
            final int sequence = i;
            RecoveryListener userListener = new RecoveryListener() {
                @Override
                public void onRecoveryDone(
                    RecoveryState state,
                    ShardLongFieldRange timestampMillisFieldRange,
                    ShardLongFieldRange eventIngestedMillisFieldRange
                ) {
                    completionOrder.add(sequence);
                }

                @Override
                public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                    fail(e);
                }

                @Override
                public void onRecoveryAborted() {
                    fail("recovery aborted");
                }
            };
            service.enqueue(
                userListener,
                fakeRecoveryState(),
                schedulingListener -> schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY)
            );
        }

        taskQueue.runAllRunnableTasks();
        assertThat(completionOrder.size(), equalTo(total));
        for (int i = 0; i < total; i++) {
            assertThat(completionOrder.get(i), equalTo(i));
        }
        assertThat(service.currentQueueSize(), equalTo(0));
    }

    public void testFailureTriggersNextQueuedRecovery() {
        final var taskQueue = new DeterministicTaskQueue();
        final var service = new ThrottlingRecoveryService(taskQueue.getThreadPool().generic(), newClusterService(1));
        final var firstTaskFailed = new AtomicBoolean(false);
        final var secondTaskCompleted = new AtomicBoolean(false);

        RecoveryListener firstListener = new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {
                fail("first task should fail");
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                firstTaskFailed.set(true);
            }

            @Override
            public void onRecoveryAborted() {
                fail("recovery aborted");
            }
        };

        RecoveryListener secondListener = new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {
                secondTaskCompleted.set(true);
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                fail(e);
            }

            @Override
            public void onRecoveryAborted() {
                fail("recovery aborted");
            }
        };

        service.enqueue(
            firstListener,
            fakeRecoveryState(),
            ignored -> { throw new RuntimeException("test recovery task injected failure"); }
        );
        service.enqueue(secondListener, fakeRecoveryState(), schedulingListener -> {
            assertTrue("first task should have completed before second one started", firstTaskFailed.get());
            schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
        });

        taskQueue.runAllRunnableTasks();
        assertTrue(secondTaskCompleted.get());
        assertThat(service.currentQueueSize(), equalTo(0));
    }

    public void testRecoveryAbortedTriggersNextQueuedRecovery() {
        final var taskQueue = new DeterministicTaskQueue();
        final var service = new ThrottlingRecoveryService(taskQueue.getThreadPool().generic(), newClusterService(1));
        final var firstTaskAborted = new AtomicBoolean(false);
        final var secondTaskCompleted = new AtomicBoolean(false);

        RecoveryListener firstListener = new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {
                fail("first task should be aborted");
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                fail(e);
            }

            @Override
            public void onRecoveryAborted() {
                firstTaskAborted.set(true);
            }
        };

        RecoveryListener secondListener = new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {
                secondTaskCompleted.set(true);
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                fail(e);
            }

            @Override
            public void onRecoveryAborted() {
                fail("second task should not be aborted");
            }
        };

        service.enqueue(firstListener, fakeRecoveryState(), RecoveryListener::onRecoveryAborted);
        service.enqueue(secondListener, fakeRecoveryState(), schedulingListener -> {
            assertTrue("first task should have completed before second one started", firstTaskAborted.get());
            schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
        });

        taskQueue.runAllRunnableTasks();
        assertTrue(secondTaskCompleted.get());
        assertThat(service.currentQueueSize(), equalTo(0));
    }

    public void testCloseAbortsQueuedButNotDispatchedRecoveries() {
        final var taskQueue = new DeterministicTaskQueue();
        final var service = new ThrottlingRecoveryService(taskQueue.getThreadPool().generic(), newClusterService(1));

        final var queuedTaskAborted = new AtomicBoolean();
        final var runningTaskDispatched = new AtomicBoolean();
        final var runningTaskListener = new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {
                fail("unexpected completion");
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                fail("unexpected failure " + e.getDetailedMessage());
            }

            @Override
            public void onRecoveryAborted() {
                fail("unexpected abort");
            }
        };

        service.enqueue(runningTaskListener, fakeRecoveryState(), ignored -> runningTaskDispatched.set(true));
        service.enqueue(new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {
                fail("unexpected completion");
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                fail("unexpected failure");
            }

            @Override
            public void onRecoveryAborted() {
                queuedTaskAborted.set(true);
            }
        }, fakeRecoveryState(), ignored -> fail("queued task should not be dispatched after close"));

        taskQueue.runAllRunnableTasks();
        assertTrue("first task should have been dispatched", runningTaskDispatched.get());
        assertFalse("second task should still be queued", queuedTaskAborted.get());

        service.close();
        assertTrue("queued task should be aborted on close", queuedTaskAborted.get());
        assertThat(service.currentQueueSize(), equalTo(0));
    }

    public void testEnqueueAfterCloseImmediatelyAborts() {
        final var taskQueue = new DeterministicTaskQueue();
        final var service = new ThrottlingRecoveryService(taskQueue.getThreadPool().generic(), newClusterService(1));
        service.close();

        final var aborted = new AtomicBoolean();
        service.enqueue(new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {
                fail("should not complete normally after close");
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                fail("should not fail after close");
            }

            @Override
            public void onRecoveryAborted() {
                aborted.set(true);
            }
        }, fakeRecoveryState(), ignored -> fail("should not be dispatched after close"));

        assertTrue("task enqueued after close should be immediately aborted", aborted.get());
        assertThat(service.currentQueueSize(), equalTo(0));
    }

    /// Stress one [ThrottlingRecoveryService] by enqueueing many tasks with randomized completion times,
    /// alternating bursty submits and completion periods, and randomly changing the max concurrent limit.
    /// Verify that all tasks finish and that concurrent execution never exceeds the limit applied.
    public void testStressConcurrentEnqueueMaintainsBoundsAndCompleteness() {
        final var taskQueue = new DeterministicTaskQueue();
        taskQueue.setExecutionDelayVariabilityMillis(100);

        final var maxConcurrency = new AtomicInteger(between(1, 20));
        final var clusterService = newClusterService(maxConcurrency.get());
        final var service = new ThrottlingRecoveryService(taskQueue.getThreadPool().generic(), clusterService);

        final var recoveryState = fakeRecoveryState();
        final var running = new AtomicInteger();
        final var completed = new AtomicInteger();
        final var totalTaskCount = new AtomicInteger();

        RecoveryListener trackingListener = new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {
                completed.incrementAndGet();
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                completed.incrementAndGet();
            }

            @Override
            public void onRecoveryAborted() {
                completed.incrementAndGet();
            }
        };

        for (int iteration = 0; iteration < 20; iteration++) {
            maxConcurrency.set(randomBoolean() ? between(1, 50) : Integer.MAX_VALUE);
            clusterService.getClusterSettings()
                .applySettings(
                    Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), maxConcurrency.get()).build()
                );

            final var incomingTasks = randomIntBetween(50, 100);
            totalTaskCount.addAndGet(incomingTasks);
            for (int i = 0; i < incomingTasks; i++) {
                if (iteration > 15 && rarely()) {
                    // idempotent
                    service.close();
                }
                taskQueue.scheduleNow(() -> service.enqueue(trackingListener, recoveryState, schedulingListener -> {
                    assertThat(running.incrementAndGet(), lessThanOrEqualTo(maxConcurrency.get()));
                    final var currentTime = taskQueue.getCurrentTimeMillis();
                    taskQueue.scheduleAt(currentTime + randomIntBetween(0, 100), () -> {
                        running.decrementAndGet();
                        // Randomly choose completion type
                        if (randomBoolean()) {
                            schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
                        } else {
                            if (randomBoolean()) {
                                schedulingListener.onRecoveryAborted();
                            } else {
                                schedulingListener.onRecoveryFailure(
                                    new RecoveryFailedException(
                                        recoveryState,
                                        null,
                                        new RuntimeException("test recovery task injected failure")
                                    ),
                                    false
                                );
                            }
                        }
                    });
                }));
                taskQueue.runAllRunnableTasks();
                while (randomBoolean() && taskQueue.hasDeferredTasks()) {
                    if (service.isClosed()) {
                        assertThat(service.currentQueueSize(), equalTo(0));
                    }
                    taskQueue.advanceTime();
                    taskQueue.runAllRunnableTasks();
                }
                if (service.isClosed()) {
                    assertThat(service.currentQueueSize(), equalTo(0));
                }
            }
            // Execute all enqueued and scheduled tasks
            taskQueue.runAllTasks();
            assertThat(completed.get(), equalTo(totalTaskCount.get()));
            assertThat(running.get(), equalTo(0));
            assertThat(service.currentQueueSize(), equalTo(0));
        }
    }

    private static ClusterService newClusterService(int maxConcurrentRecoveries) {
        ClusterService clusterService = mock(ClusterService.class);
        Settings settings = Settings.builder()
            .put(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), maxConcurrentRecoveries)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, Set.of(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        return clusterService;
    }

    private static RecoveryState fakeRecoveryState() {
        ShardRouting shardRouting = TestShardRouting.newShardRouting("index", 1, "node", true, ShardRoutingState.INITIALIZING);
        return new RecoveryState(shardRouting, DiscoveryNodeUtils.create("source"), DiscoveryNodeUtils.create("target"));
    }
}
