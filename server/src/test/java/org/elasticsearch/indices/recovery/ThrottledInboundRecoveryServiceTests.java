/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static org.elasticsearch.indices.recovery.ThrottledInboundRecoveryService.INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class ThrottledInboundRecoveryServiceTests extends ESTestCase {

    private TestThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        terminate(threadPool);
        super.tearDown();
    }

    /** Work starts on {@link org.elasticsearch.threadpool.ThreadPool#generic()}, not on the enqueueing thread. */
    public void testSynchronousTaskRunsOutsideEnqueueingThread() throws Exception {
        ThrottledInboundRecoveryService service = new ThrottledInboundRecoveryService(threadPool, newClusterSettings(between(2, 4)));
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
        };
        service.enqueue(userListener, schedulingListener -> {
            executionThread.set(Thread.currentThread());
            schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
        });
        assertTrue("recovery task never finished", done.await(10, TimeUnit.SECONDS));
        assertThat("recovery executed on enqueueing thread instead of generic pool", executionThread.get(), not(equalTo(caller)));
    }

    /**
     * Synchronous task: {@link RecoveryListener#onRecoveryDone} on the scheduling listener runs inline in the consumer,
     * so the user listener observes completion before the consumer runnable returns.
     */
    public void testSynchronousTaskNotifiesUserListenerBeforeConsumerReturns() throws Exception {
        ThrottledInboundRecoveryService service = new ThrottledInboundRecoveryService(threadPool, newClusterSettings(1));
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
        };
        service.enqueue(userListener, schedulingListener -> {
            schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
            consumerReturned.set(true);
        });
        assertTrue(done.await(10, TimeUnit.SECONDS));
        assertTrue(consumerReturned.get());
    }

    /**
     * Asynchronous task: consumer returns before the scheduling listener receives a terminal callback; the nested
     * runnable must only invoke {@link RecoveryListener#onRecoveryDone} after the consumer body has finished.
     */
    public void testAsynchronousTaskCompletesOnlyAfterConsumerReturns() throws Exception {
        ThrottledInboundRecoveryService service = new ThrottledInboundRecoveryService(threadPool, newClusterSettings(1));
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
        };
        service.enqueue(userListener, schedulingListener -> {
            threadPool.generic().execute(() -> {
                try {
                    assertTrue(proceedNested.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                assertTrue(consumerReturned.get());
                schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
            });
            consumerReturned.set(true);
            proceedNested.countDown();
        });
        assertTrue(done.await(10, TimeUnit.SECONDS));
    }

    /**
     * Never more than {@code maxConcurrentRecoveries} consumer bodies may overlap without a terminal scheduling-listener
     * callback; asynchronous completion exercises {@link ThrottledInboundRecoveryService}'s slot accounting.
     */
    public void testMaxConcurrencyBoundWithAsynchronousTerminalCallback() throws Exception {
        final int maxConcurrentRecoveries = between(2, 5);
        ThrottledInboundRecoveryService service = new ThrottledInboundRecoveryService(
            threadPool,
            newClusterSettings(maxConcurrentRecoveries)
        );
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
        };

        Consumer<RecoveryListener> recoveryBody = schedulingListener -> {
            int current = running.incrementAndGet();
            peakConcurrent.accumulateAndGet(current, Integer::max);
            maxConcurrentReached.countDown();
            try {
                unblock.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            threadPool.generic().execute(() -> {
                try {
                    schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
                } finally {
                    allFinished.countDown();
                }
            });
        };

        int enqueued = 0;
        for (; enqueued < maxConcurrentRecoveries; enqueued++) {
            service.enqueue(decrementingListener, recoveryBody);
        }

        assertTrue(
            "timed out waiting for expected number of concurrent tasks to execute",
            maxConcurrentReached.await(10, TimeUnit.SECONDS)
        );
        assertThat(enqueued, equalTo(maxConcurrentRecoveries));
        assertThat(running.get(), equalTo(maxConcurrentRecoveries));

        for (; enqueued < totalEnqueuedTasks; enqueued++) {
            service.enqueue(decrementingListener, recoveryBody);
        }

        unblock.countDown();

        assertTrue("timed out waiting for tasks to finish", allFinished.await(30, TimeUnit.SECONDS));
        assertThat(running.get(), equalTo(0));
        assertThat(peakConcurrent.get(), lessThanOrEqualTo(maxConcurrentRecoveries));
    }

    /** Raising the limit should start queued work without waiting for running recoveries to finish. */
    public void testIncreasingMaxConcurrentRecoveriesStartsPendingTasks() throws Exception {
        final ClusterSettings clusterSettings = newClusterSettings(2);
        final ThrottledInboundRecoveryService service = new ThrottledInboundRecoveryService(threadPool, clusterSettings);
        final CountDownLatch firstBatchStarted = new CountDownLatch(2);
        final CountDownLatch secondBatchStarted = new CountDownLatch(4);
        final CountDownLatch unblockAll = new CountDownLatch(1);
        final RecoveryListener noopUserListener = noopRecoveryListener();

        for (int i = 0; i < 4; i++) {
            service.enqueue(noopUserListener, schedulingListener -> {
                firstBatchStarted.countDown();
                secondBatchStarted.countDown();
                try {
                    assertTrue(unblockAll.await(30, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } finally {
                    schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
                }
            });
        }

        assertTrue(firstBatchStarted.await(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS));
        clusterSettings.applySettings(Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), 4).build());
        assertTrue(secondBatchStarted.await(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS));
        unblockAll.countDown();
    }

    /**
     * Lowering the limit must not cancel running recoveries, but recoveries should not be started from the pending queue
     * until enough running work finishes that a slot is free under the new limit.
     */
    public void testDecreasingMaxConcurrentRecoveriesDefersQueueWithoutCancellingRunningTasks() throws Exception {
        final ClusterSettings clusterSettings = newClusterSettings(3);
        final ThrottledInboundRecoveryService service = new ThrottledInboundRecoveryService(threadPool, clusterSettings);
        final CountDownLatch runningEntered = new CountDownLatch(3);
        final List<CountDownLatch> unblockEachFirstBatch = List.of(new CountDownLatch(1), new CountDownLatch(1), new CountDownLatch(1));
        final AtomicBoolean[] pendingStarted = new AtomicBoolean[] { new AtomicBoolean(), new AtomicBoolean(), new AtomicBoolean() };
        final RecoveryListener noopUserListener = noopRecoveryListener();
        final CyclicBarrier pendingBarrier = new CyclicBarrier(2);

        for (int i = 0; i < 3; i++) {
            final int index = i;
            service.enqueue(noopUserListener, schedulingListener -> {
                threadPool.generic().execute(() -> {
                    runningEntered.countDown();
                    try {
                        assertTrue(unblockEachFirstBatch.get(index).await(30, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    } finally {
                        schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
                    }
                });
            });
        }

        assertTrue(runningEntered.await(30, TimeUnit.SECONDS));

        for (int p = 0; p < 3; p++) {
            final int pendingIndex = p;
            service.enqueue(noopUserListener, schedulingListener -> {
                pendingStarted[pendingIndex].set(true);
                safeAwait(pendingBarrier);
                schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
            });
        }

        for (AtomicBoolean started : pendingStarted) {
            assertFalse(started.get());
        }

        clusterSettings.applySettings(Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), 1).build());

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
        ThrottledInboundRecoveryService service = new ThrottledInboundRecoveryService(threadPool, newClusterSettings(1));
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
            };
            service.enqueue(
                userListener,
                schedulingListener -> schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY)
            );
        }

        assertTrue(allDone.await(30, TimeUnit.SECONDS));
        assertThat(completionOrder.size(), equalTo(total));
        for (int i = 0; i < total; i++) {
            assertThat(completionOrder.get(i), equalTo(Integer.valueOf(i)));
        }
    }

    /**
     * Stress one {@link ThrottledInboundRecoveryService} from many producer threads for one second: alternating bursty
     * submits (high contention on the throttle) and idle periods (little to no contention). Verify that all tasks finish
     * and that consumer-body overlap never exceeds the highest {@code indices.recovery.max_concurrent_recoveries} applied
     * during the run (running work is not cancelled when the limit drops).
     *
     * By randomizing burst sizes we exercise different backlog shapes where in some executions there are always pending
     * recoveries, and in others the pending queue sometimes drains during idle periods.
     */
    public void testStressConcurrentEnqueueMaintainsBoundsFifoAndCompleteness() throws Exception {
        final int initialMaxConcurrentRecoveries = between(1, 20);
        final ClusterSettings clusterSettings = newClusterSettings(initialMaxConcurrentRecoveries);
        final AtomicInteger peakLimitCeiling = new AtomicInteger(initialMaxConcurrentRecoveries);
        final int highContentionBurstSizeMin = between(1, initialMaxConcurrentRecoveries);
        final int highContentionBurstSizeMax = between(highContentionBurstSizeMin, initialMaxConcurrentRecoveries * 2);
        final ThrottledInboundRecoveryService service = new ThrottledInboundRecoveryService(threadPool, clusterSettings);
        final AtomicInteger running = new AtomicInteger();
        final AtomicInteger peakRunning = new AtomicInteger();
        final AtomicInteger totalTasksEnqueued = new AtomicInteger();
        final AtomicInteger totalTasksFinished = new AtomicInteger();
        final DynamicTaskLatch taskLatch = new DynamicTaskLatch();

        final RecoveryListener noopUserListener = new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {}

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                fail(e);
            }
        };

        final long deadlineMillis = System.currentTimeMillis() + TimeUnit.MILLISECONDS.toMillis(1000);
        final int producerThreads = between(1, 6);
        final List<Thread> threads = new ArrayList<>(producerThreads);
        for (int t = 0; t < producerThreads; t++) {
            final int index = t;
            Thread thread = new Thread(() -> {
                try {
                    long phaseClock = System.currentTimeMillis();
                    while (System.currentTimeMillis() < deadlineMillis) {
                        long elapsedPhase = System.currentTimeMillis() - phaseClock;
                        boolean highContention = (elapsedPhase / TimeUnit.MILLISECONDS.toMillis(100)) % 2 == 0;
                        if (index == 0 && rarely()) {
                            int nextLimit = between(1, 20);
                            clusterSettings.applySettings(
                                Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), nextLimit).build()
                            );
                            peakLimitCeiling.accumulateAndGet(nextLimit, Integer::max);
                        }
                        if (highContention) {
                            int burst = between(highContentionBurstSizeMin, highContentionBurstSizeMax);
                            for (int b = 0; b < burst && System.currentTimeMillis() < deadlineMillis; b++) {
                                taskLatch.taskStarted();
                                totalTasksEnqueued.incrementAndGet();
                                service.enqueue(
                                    noopUserListener,
                                    schedulingListener -> runStressInboundRecoveryTask(
                                        schedulingListener,
                                        running,
                                        peakRunning,
                                        totalTasksFinished,
                                        taskLatch,
                                        random().nextBoolean(),
                                        threadPool
                                    )
                                );
                            }
                        } else {
                            int sleepMs = between(15, 80);
                            Thread.sleep(sleepMs);
                            if (System.currentTimeMillis() < deadlineMillis) {
                                taskLatch.taskStarted();
                                totalTasksEnqueued.incrementAndGet();
                                service.enqueue(
                                    noopUserListener,
                                    schedulingListener -> runStressInboundRecoveryTask(
                                        schedulingListener,
                                        running,
                                        peakRunning,
                                        totalTasksFinished,
                                        taskLatch,
                                        random().nextBoolean(),
                                        threadPool
                                    )
                                );
                            }
                        }
                        Thread.sleep(between(1, 10));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }, "recovery-inbound-throttle-stress-" + t);
            threads.add(thread);
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertTrue("timed out waiting for all stress tasks to finish", taskLatch.awaitZero(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS));
        assertThat(totalTasksFinished.get(), equalTo(totalTasksEnqueued.get()));
        assertThat(peakRunning.get(), lessThanOrEqualTo(peakLimitCeiling.get()));
        assertThat(running.get(), equalTo(0));
    }

    private static void runStressInboundRecoveryTask(
        RecoveryListener schedulingListener,
        AtomicInteger running,
        AtomicInteger peakRunning,
        AtomicInteger totalTasksFinished,
        DynamicTaskLatch latch,
        boolean async,
        ThreadPool threadPool
    ) {
        if (async) {
            threadPool.generic()
                .execute(() -> doRunStressInboundRecoveryTask(schedulingListener, running, peakRunning, totalTasksFinished, latch));
        } else {
            doRunStressInboundRecoveryTask(schedulingListener, running, peakRunning, totalTasksFinished, latch);
        }
    }

    private static void doRunStressInboundRecoveryTask(
        RecoveryListener schedulingListener,
        AtomicInteger running,
        AtomicInteger peakRunning,
        AtomicInteger totalTasksFinished,
        DynamicTaskLatch latch
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
            latch.taskFinished();
        }
    }

    private static final class DynamicTaskLatch {
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition zero = lock.newCondition();
        private long count;

        void taskStarted() {
            lock.lock();
            try {
                if (count == Long.MAX_VALUE) {
                    throw new IllegalStateException("Too many active tasks");
                }
                count++;
            } finally {
                lock.unlock();
            }
        }

        void taskFinished() {
            lock.lock();
            try {
                if (count == 0) {
                    throw new IllegalStateException("taskFinished without taskStarted");
                }

                count--;

                if (count == 0) {
                    zero.signalAll();
                }
            } finally {
                lock.unlock();
            }
        }

        boolean awaitZero(long timeout, TimeUnit unit) throws InterruptedException {
            lock.lock();
            try {
                long nanosRemaining = unit.toNanos(timeout);
                while (count != 0) {
                    if (nanosRemaining <= 0) {
                        return false;
                    }
                    nanosRemaining = zero.awaitNanos(nanosRemaining);
                }
                return true;
            } finally {
                lock.unlock();
            }
        }
    }

    private static ClusterSettings newClusterSettings(int maxConcurrentRecoveries) {
        Settings settings = Settings.builder()
            .put(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), maxConcurrentRecoveries)
            .build();
        return new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    }

    private static RecoveryListener noopRecoveryListener() {
        return new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {}

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                fail(e);
            }
        };
    }
}
