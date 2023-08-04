/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class BoundedExecutorTests extends ESTestCase {

    public void testPositiveBound() {
        expectThrows(IllegalArgumentException.class, () -> new BoundedExecutor(command -> {}, 0));
        expectThrows(IllegalArgumentException.class, () -> new BoundedExecutor(command -> {}, -1));
    }

    public void testBound() {
        int numThreads = randomIntBetween(1, 10);
        ThreadPoolExecutor executor = null;
        try {
            executor = EsExecutors.newFixed(BoundedExecutorTests.class.getName(), numThreads, numThreads,
                EsExecutors.daemonThreadFactory(""), new ThreadContext(Settings.EMPTY), EsExecutors.TaskTrackingConfig.DO_NOT_TRACK);
            BoundedExecutor boundedExecutor = new BoundedExecutor(executor);
            assertEquals(numThreads, boundedExecutor.getBound());
        } finally {
            terminate(executor);
        }
    }

    public void testExecute() throws Exception {
        int numThreads = randomIntBetween(1, 10);
        int numTasks = randomIntBetween(500, 1000);
        ThreadPoolExecutor executor = EsExecutors.newFixed(BoundedExecutorTests.class.getName(), numThreads, numThreads,
            EsExecutors.daemonThreadFactory(""), new ThreadContext(Settings.EMPTY), EsExecutors.TaskTrackingConfig.DO_NOT_TRACK);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        try {
            BoundedExecutor boundedExecutor = new BoundedExecutor(executor);
            final AtomicInteger counter = new AtomicInteger(0);
            List<FutureTask<Boolean>> tasks = new ArrayList<>();
            for (int i = 0; i < numTasks; i++) {
                tasks.add(new FutureTask<>(() -> {
                    counter.incrementAndGet();
                    return null;
                }));
            }
            for (FutureTask<Boolean> task : tasks) {
                boundedExecutor.execute(task);
                // tasks queue up despite the semaphore, because the executor updates its internal state only after each permit is released
                // yet the number of items in queue is always less than or equal to the number of permits
                assertThat(executor.getQueue().size(), Matchers.lessThanOrEqualTo(numThreads));
            }
            for (FutureTask<Boolean> future : tasks) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    assert false;
                }
            }
            assertEquals(numTasks, counter.get());
            assertBusy(() -> assertEquals(numTasks, executor.getCompletedTaskCount()));
        } finally {
            terminate(executor);
        }
    }

    // TODO test that we release permits when the thread pool ends up rejecting for whatever reason

    // TODO test fairness?

    // TODO check that there is no starvation
}
