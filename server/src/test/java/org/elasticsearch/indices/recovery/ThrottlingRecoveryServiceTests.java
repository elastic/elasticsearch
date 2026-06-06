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
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.common.util.concurrent.EsExecutors.DIRECT_EXECUTOR_SERVICE;
import static org.elasticsearch.indices.recovery.ThrottlingRecoveryService.INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ThrottlingRecoveryServiceTests extends ESTestCase {
    private static TestThreadPool threadPool;
    private static Executor executor;

    @BeforeClass
    public static void init() throws Exception {
        threadPool = new TestThreadPool(ThrottlingRecoveryServiceTests.class.getSimpleName());
        executor = threadPool.generic();
    }

    @AfterClass
    public static void close() throws Exception {
        terminate(threadPool);
    }

    /** Work starts on {@link org.elasticsearch.threadpool.ThreadPool#generic()}, not on the enqueueing thread. */
    public void testSynchronousTaskRunsOutsideEnqueueingThread() throws Exception {
        ThrottlingRecoveryService service = new ThrottlingRecoveryService(executor, newClusterService(between(2, 4)));
        Thread caller = Thread.currentThread();
        AtomicReference<Thread> executionThread = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        RecoveryListener userListener = new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {
                done.countDown();
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                fail(e);
            }

            @Override
            public void onRecoveryCancelled() {
                fail("recovery cancelled");
            }
        };
        service.enqueue(userListener, fakeRecoveryState(), schedulingListener -> {
            executionThread.set(Thread.currentThread());
            schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
        });
        safeAwait(done);
        assertThat("recovery executed on enqueueing thread instead of generic pool", executionThread.get(), not(equalTo(caller)));
    }

    /**
     * Synchronous task: {@link RecoveryListener#onRecoveryDone} on the scheduling listener runs inline in the consumer,
     * so the user listener observes completion before the consumer runnable returns.
     */
    public void testSynchronousTaskNotifiesUserListenerBeforeConsumerReturns() throws Exception {
        ThrottlingRecoveryService service = new ThrottlingRecoveryService(executor, newClusterService(1));
        AtomicBoolean consumerReturned = new AtomicBoolean(false);
        CountDownLatch done = new CountDownLatch(1);
        RecoveryListener userListener = new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {
                assertFalse("user listener should run before the consumer body finishes", consumerReturned.get());
                done.countDown();
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                fail(e);
            }

            @Override
            public void onRecoveryCancelled() {
                fail("recovery cancelled");
            }
        };
        service.enqueue(userListener, fakeRecoveryState(), schedulingListener -> {
            schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
            consumerReturned.set(true);
        });
        safeAwait(done);
    }

    /**
     * Asynchronous task: consumer returns before the scheduling listener receives a terminal callback; the nested
     * runnable must only invoke {@link RecoveryListener#onRecoveryDone} after the consumer body has finished.
     */
    public void testAsynchronousTaskCompletesOnlyAfterConsumerReturns() throws Exception {
        ThrottlingRecoveryService service = new ThrottlingRecoveryService(executor, newClusterService(1));
        AtomicBoolean consumerReturned = new AtomicBoolean(false);
        CountDownLatch proceedNested = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(1);
        RecoveryListener userListener = new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {
                assertTrue("terminal callback should follow consumer return", consumerReturned.get());
                done.countDown();
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                fail(e);
            }

            @Override
            public void onRecoveryCancelled() {
                fail("recovery cancelled");
            }
        };
        service.enqueue(userListener, fakeRecoveryState(), schedulingListener -> {
            executor.execute(() -> {
                safeAwait(proceedNested);
                assertTrue(consumerReturned.get());
                schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
            });
            consumerReturned.set(true);
            proceedNested.countDown();
        });
        safeAwait(done);
    }

    /**
     * Never more than {@code maxConcurrentRecoveries} consumer bodies may overlap without a terminal scheduling-listener
     * callback; asynchronous completion exercises {@link ThrottlingRecoveryService}'s slot accounting.
     */
    public void testMaxConcurrencyBoundWithAsynchronousTerminalCallback() throws Exception {
        final int maxConcurrentRecoveries = between(2, 5);
        ThrottlingRecoveryService service = new ThrottlingRecoveryService(executor, newClusterService(maxConcurrentRecoveries));
        AtomicInteger running = new AtomicInteger();
        AtomicInteger peakConcurrent = new AtomicInteger();
        CountDownLatch maxConcurrentReached = new CountDownLatch(maxConcurrentRecoveries);
        CountDownLatch unblock = new CountDownLatch(1);
        int totalEnqueuedTasks = maxConcurrentRecoveries * 3;
        CountDownLatch allFinished = new CountDownLatch(totalEnqueuedTasks);

        RecoveryListener decrementingListener = new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {
                running.decrementAndGet();
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                fail(e);
            }

            @Override
            public void onRecoveryCancelled() {
                fail("recovery cancelled");
            }
        };

        Consumer<RecoveryListener> recoveryBody = schedulingListener -> {
            int current = running.incrementAndGet();
            peakConcurrent.accumulateAndGet(current, Integer::max);
            maxConcurrentReached.countDown();
            safeAwait(unblock);
            executor.execute(() -> {
                try {
                    schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
                } finally {
                    allFinished.countDown();
                }
            });
        };

        int enqueued = 0;
        for (; enqueued < maxConcurrentRecoveries; enqueued++) {
            service.enqueue(decrementingListener, fakeRecoveryState(), recoveryBody);
        }

        safeAwait(maxConcurrentReached);
        assertThat(enqueued, equalTo(maxConcurrentRecoveries));
        assertThat(running.get(), equalTo(maxConcurrentRecoveries));

        for (; enqueued < totalEnqueuedTasks; enqueued++) {
            service.enqueue(decrementingListener, fakeRecoveryState(), recoveryBody);
        }

        unblock.countDown();

        safeAwait(allFinished);
        assertThat(running.get(), equalTo(0));
        assertThat(peakConcurrent.get(), lessThanOrEqualTo(maxConcurrentRecoveries));
    }

    /** Raising the limit should start queued work without waiting for running recoveries to finish. */
    public void testIncreasingMaxConcurrentRecoveriesStartsPendingTasks() {
        final ClusterService clusterService = newClusterService(2);
        final ThrottlingRecoveryService service = new ThrottlingRecoveryService(executor, clusterService);
        final CountDownLatch firstBatchStarted = new CountDownLatch(2);
        final CountDownLatch secondBatchStarted = new CountDownLatch(4);
        final CountDownLatch unblockAll = new CountDownLatch(1);
        final RecoveryListener noopUserListener = RecoveryListener.NOOP;

        for (int i = 0; i < 4; i++) {
            service.enqueue(noopUserListener, fakeRecoveryState(), schedulingListener -> {
                firstBatchStarted.countDown();
                secondBatchStarted.countDown();
                safeAwait(unblockAll);
                schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
            });
        }

        safeAwait(firstBatchStarted);
        clusterService.getClusterSettings()
            .applySettings(Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), 4).build());
        safeAwait(secondBatchStarted);
        unblockAll.countDown();
    }

    /**
     * Lowering the limit must not cancel running recoveries, but recoveries should not be started from the pending queue
     * until enough running work finishes that a slot is free under the new limit.
     */
    public void testDecreasingMaxConcurrentRecoveriesDefersQueueWithoutCancellingRunningTasks() {
        final ClusterService clusterService = newClusterService(3);
        final ThrottlingRecoveryService service = new ThrottlingRecoveryService(executor, clusterService);
        final CountDownLatch runningEntered = new CountDownLatch(3);
        final List<CountDownLatch> unblockEachFirstBatch = List.of(new CountDownLatch(1), new CountDownLatch(1), new CountDownLatch(1));
        final AtomicBoolean[] pendingStarted = new AtomicBoolean[] { new AtomicBoolean(), new AtomicBoolean(), new AtomicBoolean() };
        final RecoveryListener noopUserListener = RecoveryListener.NOOP;
        final CyclicBarrier pendingBarrier = new CyclicBarrier(2);

        for (int i = 0; i < 3; i++) {
            final int index = i;
            service.enqueue(noopUserListener, fakeRecoveryState(), schedulingListener -> executor.execute(() -> {
                runningEntered.countDown();
                safeAwait(unblockEachFirstBatch.get(index));
                schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
            }));
        }

        safeAwait(runningEntered);

        for (int p = 0; p < 3; p++) {
            final int pendingIndex = p;
            service.enqueue(noopUserListener, fakeRecoveryState(), schedulingListener -> {
                pendingStarted[pendingIndex].set(true);
                safeAwait(pendingBarrier);
                schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
            });
        }

        for (AtomicBoolean started : pendingStarted) {
            assertFalse(started.get());
        }

        clusterService.getClusterSettings()
            .applySettings(Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), 1).build());

        unblockEachFirstBatch.get(0).countDown();
        Thread.yield();
        for (AtomicBoolean started : pendingStarted) {
            assertFalse(started.get());
        }

        unblockEachFirstBatch.get(1).countDown();
        Thread.yield();
        for (AtomicBoolean started : pendingStarted) {
            assertFalse(started.get());
        }

        // Unlocking the last running task should leave space for all pending tasks to run, one by one
        unblockEachFirstBatch.get(2).countDown();
        safeAwait(pendingBarrier);
        safeAwait(pendingBarrier);
        safeAwait(pendingBarrier);
        for (AtomicBoolean started : pendingStarted) {
            assertTrue(started.get());
        }
    }

    /** With one slot, synchronous completions preserve enqueue order. */
    public void testFifoWhenThrottledToOneConcurrentWithSynchronousCompletion() throws Exception {
        ThrottlingRecoveryService service = new ThrottlingRecoveryService(executor, newClusterService(1));
        int total = between(10, 20);
        List<Integer> completionOrder = new CopyOnWriteArrayList<>();
        CountDownLatch allDone = new CountDownLatch(total);

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
                    allDone.countDown();
                }

                @Override
                public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                    fail(e);
                }

                @Override
                public void onRecoveryCancelled() {
                    fail("recovery cancelled");
                }
            };
            service.enqueue(
                userListener,
                fakeRecoveryState(),
                schedulingListener -> schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY)
            );
        }

        safeAwait(allDone);
        assertThat(completionOrder.size(), equalTo(total));
        for (int i = 0; i < total; i++) {
            assertThat(completionOrder.get(i), equalTo(i));
        }
    }

    public void testTaskFailurePropagateToListener() throws Exception {
        ThrottlingRecoveryService service = new ThrottlingRecoveryService(executor, newClusterService(1));
        AtomicReference<Exception> exceptionReceived = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        RecoveryListener userListener = new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {
                fail("unexpected success");
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                exceptionReceived.set(e);
                done.countDown();
            }

            @Override
            public void onRecoveryCancelled() {
                fail("recovery cancelled");
            }
        };
        service.enqueue(userListener, fakeRecoveryState(), schedulingListener -> { throw new RuntimeException("Leeeeroooooy"); });
        safeAwait(done);
        assertThat("recovery executed on enqueueing thread instead of generic pool", exceptionReceived.get(), notNullValue());
    }

    /**
     * Stress one {@link ThrottlingRecoveryService} from many producer threads for one second: alternating bursty
     * submits (high contention on the throttle) and idle periods (little to no contention). Verify that all tasks finish
     * and that consumer-body overlap never exceeds the highest {@code indices.recovery.max_concurrent_recoveries} applied
     * during the run (running work is not cancelled when the limit drops).
     * <p>
     * By randomizing burst sizes we exercise different backlog shapes where in some executions there are always pending
     * recoveries, and in others the pending queue sometimes drains during idle periods.
     */
    public void testStressConcurrentEnqueueMaintainsBoundsAndCompleteness() throws Exception {
        final int initialMaxConcurrentRecoveries = between(1, 20);
        final ClusterService clusterService = newClusterService(initialMaxConcurrentRecoveries);
        final AtomicInteger peakLimitCeiling = new AtomicInteger(initialMaxConcurrentRecoveries);
        final int highContentionBurstSizeMin = between(1, initialMaxConcurrentRecoveries);
        final int highContentionBurstSizeMax = between(highContentionBurstSizeMin, initialMaxConcurrentRecoveries * 2);
        final ThrottlingRecoveryService service = new ThrottlingRecoveryService(executor, clusterService);
        final AtomicInteger running = new AtomicInteger();
        final AtomicInteger peakRunning = new AtomicInteger();
        final AtomicInteger totalTasksEnqueued = new AtomicInteger();
        final AtomicInteger totalTasksFinished = new AtomicInteger();
        final CountDownLatch allFinished = new CountDownLatch(1);
        final RefCounted taskLatch = AbstractRefCounted.of(allFinished::countDown);
        final int maxNumberOfTasks = 1000;

        final int producerThreads = between(1, 6);
        runInParallel(producerThreads, index -> {
            while (totalTasksEnqueued.get() < maxNumberOfTasks) {
                boolean highContention = (totalTasksEnqueued.get() / 100) % 2 == 0;
                if (index == 0 && rarely()) {
                    int nextLimit = between(1, 20);
                    clusterService.getClusterSettings()
                        .applySettings(
                            Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), nextLimit).build()
                        );
                    peakLimitCeiling.accumulateAndGet(nextLimit, Integer::max);
                }
                if (highContention) {
                    int burst = between(highContentionBurstSizeMin, highContentionBurstSizeMax);
                    for (int b = 0; b < burst && totalTasksEnqueued.get() < maxNumberOfTasks; b++) {
                        taskLatch.incRef();
                        totalTasksEnqueued.incrementAndGet();
                        service.enqueue(
                            RecoveryListener.NOOP,
                            fakeRecoveryState(),
                            schedulingListener -> runStressInboundRecoveryTask(
                                schedulingListener,
                                running,
                                peakRunning,
                                totalTasksFinished,
                                taskLatch,
                                random().nextBoolean() ? executor : DIRECT_EXECUTOR_SERVICE
                            )
                        );
                    }
                } else {
                    Thread.yield();
                    if (totalTasksEnqueued.get() < maxNumberOfTasks) {
                        taskLatch.incRef();
                        totalTasksEnqueued.incrementAndGet();
                        service.enqueue(
                            RecoveryListener.NOOP,
                            fakeRecoveryState(),
                            schedulingListener -> runStressInboundRecoveryTask(
                                schedulingListener,
                                running,
                                peakRunning,
                                totalTasksFinished,
                                taskLatch,
                                random().nextBoolean() ? executor : DIRECT_EXECUTOR_SERVICE
                            )
                        );
                    }
                }
                Thread.yield();
            }
        });

        // taskLatch starts with 1 ref, decremented here
        taskLatch.decRef();
        safeAwait(allFinished);
        assertThat(totalTasksFinished.get(), equalTo(totalTasksEnqueued.get()));
        assertThat(peakRunning.get(), lessThanOrEqualTo(peakLimitCeiling.get()));
        assertThat(running.get(), equalTo(0));
    }

    private static void runStressInboundRecoveryTask(
        RecoveryListener schedulingListener,
        AtomicInteger running,
        AtomicInteger peakRunning,
        AtomicInteger totalTasksFinished,
        RefCounted refCounted,
        Executor executor
    ) {
        executor.execute(() -> doRunStressInboundRecoveryTask(schedulingListener, running, peakRunning, totalTasksFinished, refCounted));
    }

    private static void doRunStressInboundRecoveryTask(
        RecoveryListener schedulingListener,
        AtomicInteger running,
        AtomicInteger peakRunning,
        AtomicInteger totalTasksFinished,
        RefCounted refCounted
    ) {
        int current = running.incrementAndGet();
        peakRunning.accumulateAndGet(current, Integer::max);
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            running.decrementAndGet();
            totalTasksFinished.incrementAndGet();
            schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
            refCounted.decRef();
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

    private RecoveryState fakeRecoveryState() {
        ShardRouting shardRouting = TestShardRouting.newShardRouting("index", 1, "node", true, ShardRoutingState.INITIALIZING);
        return new RecoveryState(shardRouting, DiscoveryNodeUtils.create("source"), DiscoveryNodeUtils.create("target"));
    }
}
