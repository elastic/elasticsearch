/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.inference.external.http.HttpClientTests.createThreadPool;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class HttpRequestExecutorServiceTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private ThreadPool threadPool;

    @Before
    public void init() {
        threadPool = createThreadPool(getTestName());
    }

    @After
    public void shutdown() {
        terminate(threadPool);
    }

    public void testQueueSize_IsEmpty() throws Exception {
        var service = new HttpRequestExecutorService(threadPool.getThreadContext(), getTestName());
        startService(service);

        assertThat(service.queueSize(), is(0));
    }

    public void testQueueSize_IsOne() throws Exception {
        var service = new HttpRequestExecutorService(threadPool.getThreadContext(), getTestName());
        startService(service);

        CountDownLatch blockReturnLatch = new CountDownLatch(1);
        CountDownLatch wasExecutedLatch = new CountDownLatch(1);

        var blockingTask = mock(RequestTask.class);
        doAnswer(invocation -> {
            // notify that the request was executed
            wasExecutedLatch.countDown();

            // block so we can check the queue size
            blockReturnLatch.await();
            return Void.TYPE;
        }).when(blockingTask).doRun();

        var noopTask = mock(RequestTask.class);

        service.execute(blockingTask);
        service.execute(noopTask);
        wasExecutedLatch.await(TIMEOUT.millis(), TimeUnit.MILLISECONDS);

        // the blocking task should be currently executing so only the noop task is in the queue
        assertThat(service.queueSize(), is(1));

        // Unblock the executor by letting the task complete
        blockReturnLatch.countDown();
    }

    public void testIsTerminated_IsFalse() throws Exception {
        var service = new HttpRequestExecutorService(threadPool.getThreadContext(), getTestName());
        startService(service);

        assertFalse(service.isTerminated());
    }

    public void testIsTerminated_IsTrue() throws Exception {
        var service = new HttpRequestExecutorService(threadPool.getThreadContext(), getTestName());
        startService(service);

        service.shutdown();
        service.awaitTermination(TIMEOUT.millis(), TimeUnit.MILLISECONDS);
        assertTrue(service.isTerminated());
    }

    private void startService(HttpRequestExecutorService service) throws Exception {
        threadPool.executor(UTILITY_THREAD_POOL_NAME).submit(service::start);

        waitForStart(service);
    }

    private static void waitForStart(HttpRequestExecutorService service) throws Exception {
        CountDownLatch wasExecutedLatch = new CountDownLatch(1);

        var blockingTask = mock(RequestTask.class);
        doAnswer(invocation -> {
            wasExecutedLatch.countDown();
            return Void.TYPE;
        }).when(blockingTask).doRun();

        service.execute(blockingTask);

        wasExecutedLatch.await(TIMEOUT.millis(), TimeUnit.MILLISECONDS);
    }
}
