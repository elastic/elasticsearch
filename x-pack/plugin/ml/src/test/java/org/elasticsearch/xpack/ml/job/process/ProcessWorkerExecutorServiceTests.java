/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isA;

public class ProcessWorkerExecutorServiceTests extends ESTestCase {

    private static final String TEST_PROCESS = "test";
    private static final int QUEUE_SIZE = 100;

    private final ThreadPool threadPool = new TestThreadPool("AutodetectWorkerExecutorServiceTests");

    @After
    public void stopThreadPool() {
        terminate(threadPool);
    }

    public void testAutodetectWorkerExecutorService_SubmitAfterShutdown() {
        ProcessWorkerExecutorService executor = createExecutorService();

        threadPool.generic().execute(executor::start);
        executor.shutdownNow();
        AtomicBoolean rejected = new AtomicBoolean(false);
        AtomicBoolean initialized = new AtomicBoolean(false);
        executor.execute(new AbstractInitializableRunnable() {
            @Override
            public void onRejection(Exception e) {
                assertThat(e, isA(EsRejectedExecutionException.class));
                rejected.set(true);
            }

            @Override
            public void init() {
                initialized.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                fail("onFailure should not be called after the worker is shutdown");
            }

            @Override
            protected void doRun() throws Exception {
                fail("doRun should not be called after the worker is shutdown");
            }
        });

        assertTrue(rejected.get());
        assertTrue(initialized.get());
    }

    public void testAutodetectWorkerExecutorService_TasksNotExecutedCallHandlerOnShutdown() throws Exception {
        ProcessWorkerExecutorService executor = createExecutorService();

        CountDownLatch latch = new CountDownLatch(1);

        Future<?> executorFinished = threadPool.generic().submit(executor::start);

        // run a task that will block while the others are queued up
        executor.execute(() -> {
            try {
                latch.await();
            } catch (InterruptedException e) {}
        });

        AtomicBoolean runnableShouldNotBeCalled = new AtomicBoolean(false);
        executor.execute(() -> runnableShouldNotBeCalled.set(true));

        ConcurrentLinkedQueue<Exception> exceptions = new ConcurrentLinkedQueue<>();
        AtomicInteger doRunCallCount = new AtomicInteger();
        for (int i = 0; i < 2; i++) {
            executor.execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    exceptions.add(e);
                }

                @Override
                protected void doRun() {
                    doRunCallCount.incrementAndGet();
                }
            });
        }

        boolean shutdownWithError = randomBoolean();
        // now shutdown
        if (shutdownWithError) {
            executor.shutdownNowWithError(new ElasticsearchException("stopping the executor because an error occurred"));
        } else {
            executor.shutdownNow();
        }
        latch.countDown();
        executorFinished.get();

        assertFalse(runnableShouldNotBeCalled.get());
        // the AbstractRunnables should have had their callbacks called
        assertEquals(0, doRunCallCount.get());
        assertThat(exceptions, hasSize(2));
        for (var e : exceptions) {
            if (shutdownWithError) {
                assertThat(e.getMessage(), containsString("stopping the executor because an error occurred"));
            } else {
                assertThat(e, isA(EsRejectedExecutionException.class));
            }
        }
    }

    public void testAutodetectWorkerExecutorServiceDoesNotSwallowErrors() {
        ProcessWorkerExecutorService executor = createExecutorService();
        if (randomBoolean()) {
            executor.submit(() -> { throw new Error("future error"); });
        } else {
            executor.execute(() -> { throw new Error("future error"); });
        }
        Error e = expectThrows(Error.class, executor::start);
        assertThat(e.getMessage(), containsString("future error"));
    }

    private ProcessWorkerExecutorService createExecutorService() {
        return new ProcessWorkerExecutorService(threadPool.getThreadContext(), TEST_PROCESS, QUEUE_SIZE);
    }
}
