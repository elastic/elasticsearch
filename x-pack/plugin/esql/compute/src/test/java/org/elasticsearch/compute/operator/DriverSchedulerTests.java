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
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;

public class DriverSchedulerTests extends ESTestCase {

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
            });
            scheduler.runPendingTasks();
            assertThat(counter.get(), equalTo(1));
        } finally {
            latch.countDown();
            terminate(threadPool);
        }
    }
}
