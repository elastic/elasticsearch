/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RequestBasedTaskRunnerTests extends ESTestCase {
    private ThreadPool threadPool;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = spy(createThreadPool(inferenceUtilityPool()));
    }

    @After
    public void tearDown() throws Exception {
        terminate(threadPool);
        super.tearDown();
    }

    public void testLoopOneAtATime() throws Exception {
        // count the number of times the runnable is called
        var counter = new AtomicInteger(0);

        // block the runnable and wait for the test thread to take an action
        var lock = new ReentrantLock();
        var condition = lock.newCondition();
        Runnable block = () -> {
            try {
                try {
                    lock.lock();
                    condition.await();
                } finally {
                    lock.unlock();
                }
            } catch (InterruptedException e) {
                fail(e, "did not unblock the thread in time, likely during threadpool terminate");
            }
        };
        Runnable unblock = () -> {
            try {
                lock.lock();
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        };

        var runner = new RequestBasedTaskRunner(() -> {
            counter.incrementAndGet();
            block.run();
        }, threadPool, UTILITY_THREAD_POOL_NAME);

        // given we have not called requestNextRun, then no thread should have started
        assertThat(counter.get(), equalTo(0));
        verify(threadPool, times(0)).executor(UTILITY_THREAD_POOL_NAME);

        runner.requestNextRun();

        // given that we have called requestNextRun, then 1 thread should run once
        assertBusy(() -> {
            verify(threadPool, times(1)).executor(UTILITY_THREAD_POOL_NAME);
            assertThat(counter.get(), equalTo(1));
        });

        // given that we have called requestNextRun while a thread was running, and the thread was blocked
        runner.requestNextRun();
        // then 1 thread should run once
        verify(threadPool, times(1)).executor(UTILITY_THREAD_POOL_NAME);
        assertThat(counter.get(), equalTo(1));

        // given the thread is unblocked
        unblock.run();
        // then 1 thread should run twice
        verify(threadPool, times(1)).executor(UTILITY_THREAD_POOL_NAME);
        assertBusy(() -> assertThat(counter.get(), equalTo(2)));

        // given the thread is unblocked again, but there were only two calls to requestNextRun
        unblock.run();
        // then 1 thread should run twice
        verify(threadPool, times(1)).executor(UTILITY_THREAD_POOL_NAME);
        assertBusy(() -> assertThat(counter.get(), equalTo(2)));

        // given no thread is running, when we call requestNextRun
        runner.requestNextRun();
        // then a second thread should start for the third run
        assertBusy(() -> {
            verify(threadPool, times(2)).executor(UTILITY_THREAD_POOL_NAME);
            assertThat(counter.get(), equalTo(3));
        });

        // given the thread is unblocked, then it should exit and rejoin the threadpool
        unblock.run();
        assertTrue("Test thread should unblock after all runs complete", terminate(threadPool));

        // final check - we ran three times on two threads
        verify(threadPool, times(2)).executor(UTILITY_THREAD_POOL_NAME);
        assertThat(counter.get(), equalTo(3));
    }

    public void testCancel() throws Exception {
        // count the number of times the runnable is called
        var counter = new AtomicInteger(0);
        var latch = new CountDownLatch(1);
        var runner = new RequestBasedTaskRunner(() -> {
            counter.incrementAndGet();
            try {
                latch.await();
            } catch (InterruptedException e) {
                fail(e, "did not unblock the thread in time, likely during threadpool terminate");
            }
        }, threadPool, UTILITY_THREAD_POOL_NAME);

        // given that we have called requestNextRun, then 1 thread should run once
        runner.requestNextRun();
        assertBusy(() -> {
            verify(threadPool, times(1)).executor(UTILITY_THREAD_POOL_NAME);
            assertThat(counter.get(), equalTo(1));
        });

        // given that a thread is running, three more calls will be queued
        runner.requestNextRun();
        runner.requestNextRun();
        runner.requestNextRun();

        // when we cancel the thread, then the thread should immediately exit and rejoin
        runner.cancel();
        latch.countDown();
        assertTrue("Test thread should unblock after all runs complete", terminate(threadPool));

        // given that we called cancel, when we call requestNextRun then no thread should start
        runner.requestNextRun();
        verify(threadPool, times(1)).executor(UTILITY_THREAD_POOL_NAME);
        assertThat(counter.get(), equalTo(1));
    }

}
