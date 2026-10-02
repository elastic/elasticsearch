/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.textstructure.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class TextStructExecutorTests extends ESTestCase {

    public void testMoreRequestsThanProcessorsShouldRunAtMostProcessorCountConcurrently() throws Exception {
        Settings settings = Settings.builder().put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), 2.0).build();
        try (TestThreadPool threadPool = new TestThreadPool(getTestName(), settings)) {
            TextStructExecutor executor = new TextStructExecutor(threadPool, settings);
            int numTasks = 5;
            CountDownLatch blockLatch = new CountDownLatch(1);
            CountDownLatch completeLatch = new CountDownLatch(numTasks);
            AtomicInteger running = new AtomicInteger();
            AtomicInteger peak = new AtomicInteger();

            for (int i = 0; i < numTasks; i++) {
                executor.execute(ActionListener.wrap(r -> completeLatch.countDown(), e -> fail(e)), () -> {
                    int current = running.incrementAndGet();
                    peak.getAndUpdate(p -> Math.max(p, current));
                    try {
                        blockLatch.await();
                        return null;
                    } finally {
                        running.decrementAndGet();
                    }
                });
            }

            assertBusy(() -> assertThat(peak.get(), greaterThan(0)));
            assertThat(peak.get(), lessThanOrEqualTo(2));
            blockLatch.countDown();
            assertTrue(completeLatch.await(10, TimeUnit.SECONDS));
        }
    }

    public void testSupplierThrowingShouldReleaseSlotAndFailListener() {
        Settings settings = Settings.builder().put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), 1.0).build();
        try (TestThreadPool threadPool = new TestThreadPool(getTestName(), settings)) {
            TextStructExecutor executor = new TextStructExecutor(threadPool, settings);

            PlainActionFuture<Void> failed = new PlainActionFuture<>();
            executor.execute(failed, () -> { throw new RuntimeException("boom"); });
            expectThrows(RuntimeException.class, failed::actionGet);

            PlainActionFuture<String> succeeded = new PlainActionFuture<>();
            executor.execute(succeeded, () -> "ok");
            assertEquals("ok", succeeded.actionGet());
        }
    }

    public void testAllRequestsShouldEventuallyComplete() throws Exception {
        Settings settings = Settings.builder().put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), 1.0).build();
        try (TestThreadPool threadPool = new TestThreadPool(getTestName(), settings)) {
            TextStructExecutor executor = new TextStructExecutor(threadPool, settings);
            int numTasks = 10;
            CountDownLatch completeLatch = new CountDownLatch(numTasks);
            AtomicInteger completed = new AtomicInteger();

            for (int i = 0; i < numTasks; i++) {
                final int taskId = i;
                executor.execute(ActionListener.wrap(r -> {
                    assertThat(completed.incrementAndGet(), equalTo(taskId + 1));
                    completeLatch.countDown();
                }, e -> fail(e)), () -> taskId);
            }

            assertTrue(completeLatch.await(10, TimeUnit.SECONDS));
            assertThat(completed.get(), equalTo(numTasks));
        }
    }

    public void testTwoActionTypesSharingExecutorShouldNotExceedProcessorCap() throws Exception {
        Settings settings = Settings.builder().put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), 2.0).build();
        try (TestThreadPool threadPool = new TestThreadPool(getTestName(), settings)) {
            TextStructExecutor shared = new TextStructExecutor(threadPool, settings);
            int tasksPerAction = 4;
            CountDownLatch blockLatch = new CountDownLatch(1);
            CountDownLatch completeLatch = new CountDownLatch(tasksPerAction * 2);
            AtomicInteger running = new AtomicInteger();
            AtomicInteger peak = new AtomicInteger();

            Runnable enqueueBurst = () -> {
                for (int i = 0; i < tasksPerAction; i++) {
                    shared.execute(ActionTestUtils.assertNoFailureListener(r -> completeLatch.countDown()), () -> {
                        int current = running.incrementAndGet();
                        peak.getAndUpdate(p -> Math.max(p, current));
                        try {
                            blockLatch.await();
                            return null;
                        } finally {
                            running.decrementAndGet();
                        }
                    });
                }
            };
            enqueueBurst.run();
            enqueueBurst.run();

            assertBusy(() -> assertThat(peak.get(), greaterThan(0)));
            assertThat(peak.get(), lessThanOrEqualTo(2));
            blockLatch.countDown();
            assertTrue(completeLatch.await(10, TimeUnit.SECONDS));
        }
    }
}
