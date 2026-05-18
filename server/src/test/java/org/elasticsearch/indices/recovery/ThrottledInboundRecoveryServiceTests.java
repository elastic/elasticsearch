/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService.RecoveryListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

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
        ThrottledInboundRecoveryService service = new ThrottledInboundRecoveryService(threadPool, between(2, 4));
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
        ThrottledInboundRecoveryService service = new ThrottledInboundRecoveryService(threadPool, 1);
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
        ThrottledInboundRecoveryService service = new ThrottledInboundRecoveryService(threadPool, 1);
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
        ThrottledInboundRecoveryService service = new ThrottledInboundRecoveryService(threadPool, maxConcurrentRecoveries);
        AtomicInteger running = new AtomicInteger();
        AtomicInteger peakConcurrent = new AtomicInteger();
        CountDownLatch maxConcurrentReached = new CountDownLatch(maxConcurrentRecoveries);
        CountDownLatch unblock = new CountDownLatch(1);
        int totalEnqueuedTasks = maxConcurrentRecoveries * 3;
        CountDownLatch allFinished = new CountDownLatch(totalEnqueuedTasks);

        RecoveryListener noopUserListener = new RecoveryListener() {
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
            service.enqueue(noopUserListener, recoveryBody);
        }

        assertTrue(
            "timed out waiting for expected number of concurrent tasks to execute",
            maxConcurrentReached.await(10, TimeUnit.SECONDS)
        );
        assertThat(enqueued, equalTo(maxConcurrentRecoveries));
        assertThat(running.get(), equalTo(maxConcurrentRecoveries));

        for (; enqueued < totalEnqueuedTasks; enqueued++) {
            service.enqueue(noopUserListener, recoveryBody);
        }

        unblock.countDown();

        assertTrue("timed out waiting for tasks to finish", allFinished.await(30, TimeUnit.SECONDS));
        assertThat(running.get(), equalTo(0));
        assertThat(peakConcurrent.get(), lessThanOrEqualTo(maxConcurrentRecoveries));
    }

    /** With one slot, synchronous completions preserve enqueue order. */
    public void testFifoWhenThrottledToOneConcurrentWithSynchronousCompletion() throws Exception {
        ThrottledInboundRecoveryService service = new ThrottledInboundRecoveryService(threadPool, 1);
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
}
