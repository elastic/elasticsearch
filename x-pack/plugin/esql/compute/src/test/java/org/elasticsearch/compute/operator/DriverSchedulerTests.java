/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class DriverSchedulerTests extends ESTestCase {

    /** A task queued behind a busy thread reports its wait once. */
    public void testReportsQueueTime() throws Exception {
        DriverScheduler scheduler = new DriverScheduler();
        var threadPool = new TestThreadPool(
            "test",
            new FixedExecutorBuilder(Settings.EMPTY, "test", 1, 2, "test", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
        Executor executor = threadPool.executor("test");
        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch ran = new CountDownLatch(1);
        AtomicInteger reports = new AtomicInteger();
        AtomicLong reportedMicros = new AtomicLong();
        try {
            executor.execute(() -> {
                blockerStarted.countDown();
                safeAwait(release);
            });
            safeAwait(blockerStarted);
            scheduler.scheduleOrRunTask(executor, new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError(e);
                }

                @Override
                protected void doRun() {
                    ran.countDown();
                }
            }, queueTimeMicros -> {
                reports.incrementAndGet();
                reportedMicros.set(queueTimeMicros);
            });
            long queuedAtLeastNanos = TimeUnit.MILLISECONDS.toNanos(20);
            long start = System.nanoTime();
            while (System.nanoTime() - start < queuedAtLeastNanos) {
                Thread.sleep(1);
            }
            release.countDown();
            safeAwait(ran);
            assertThat(reports.get(), equalTo(1));
            assertThat(reportedMicros.get(), greaterThanOrEqualTo(TimeUnit.NANOSECONDS.toMicros(queuedAtLeastNanos)));
        } finally {
            release.countDown();
            terminate(threadPool);
        }
    }

    /** A runnable superseded by {@link DriverScheduler#runPendingTasks()} reports no wait when it is finally dequeued. */
    public void testNoQueueTimeReportedForSupersededTask() throws Exception {
        DriverScheduler scheduler = new DriverScheduler();
        var threadPool = new TestThreadPool(
            "test",
            new FixedExecutorBuilder(Settings.EMPTY, "test", 1, 3, "test", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
        Executor executor = threadPool.executor("test");
        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch ranDirectly = new CountDownLatch(1);
        AtomicInteger reports = new AtomicInteger();
        try {
            executor.execute(() -> {
                blockerStarted.countDown();
                safeAwait(release);
            });
            safeAwait(blockerStarted);
            scheduler.scheduleOrRunTask(executor, new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError(e);
                }

                @Override
                protected void doRun() {
                    ranDirectly.countDown();
                }
            }, queueTimeMicros -> reports.incrementAndGet());
            scheduler.runPendingTasks();
            safeAwait(ranDirectly);

            // single thread, FIFO queue: the superseded runnable is dequeued before this runs
            CountDownLatch sentinelRan = new CountDownLatch(1);
            executor.execute(sentinelRan::countDown);
            release.countDown();
            safeAwait(sentinelRan);
            assertThat(reports.get(), equalTo(0));
        } finally {
            release.countDown();
            terminate(threadPool);
        }
    }

    public void testClearPendingTaskOnRejection() {
        DriverScheduler scheduler = new DriverScheduler();
        AtomicInteger counter = new AtomicInteger();
        var threadPool = new TestThreadPool(
            "test",
            new FixedExecutorBuilder(Settings.EMPTY, "test", 1, 2, "test", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
        CountDownLatch latch = new CountDownLatch(1);
        Executor executor = threadPool.executor("test");
        try {
            for (int i = 0; i < 10; i++) {
                try {
                    executor.execute(() -> safeAwait(latch));
                } catch (EsRejectedExecutionException e) {
                    break;
                }
            }
            scheduler.scheduleOrRunTask(executor, new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    counter.incrementAndGet();
                }

                @Override
                protected void doRun() {
                    counter.incrementAndGet();
                }
            }, queueTimeMicros -> {});
            scheduler.runPendingTasks();
            assertThat(counter.get(), equalTo(1));
        } finally {
            latch.countDown();
            terminate(threadPool);
        }
    }
}
