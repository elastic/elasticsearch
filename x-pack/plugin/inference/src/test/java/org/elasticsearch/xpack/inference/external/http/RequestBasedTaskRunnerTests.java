/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class RequestBasedTaskRunnerTests extends ESTestCase {
    private ThreadPool threadPool;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = mock();
        when(threadPool.executor(UTILITY_THREAD_POOL_NAME)).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
    }

    public void testRequestWhileLoopingWillRerunCommand() {
        var expectedTimesRerun = randomInt(5);
        AtomicInteger counter = new AtomicInteger(0);

        var requestNextRun = new AtomicReference<Runnable>();
        Runnable command = () -> {
            if (counter.getAndIncrement() < expectedTimesRerun) {
                requestNextRun.get().run();
            }
        };
        var runner = new RequestBasedTaskRunner(command, threadPool, UTILITY_THREAD_POOL_NAME);
        requestNextRun.set(runner::requestNextRun);
        runner.requestNextRun();

        verify(threadPool, times(1)).executor(eq(UTILITY_THREAD_POOL_NAME));
        verifyNoMoreInteractions(threadPool);
        assertThat(counter.get(), equalTo(expectedTimesRerun + 1));
    }

    public void testRequestWhileNotLoopingWillQueueCommand() {
        AtomicInteger counter = new AtomicInteger(0);

        var runner = new RequestBasedTaskRunner(counter::incrementAndGet, threadPool, UTILITY_THREAD_POOL_NAME);

        for (int i = 1; i < randomInt(10); i++) {
            runner.requestNextRun();
            verify(threadPool, times(i)).executor(eq(UTILITY_THREAD_POOL_NAME));
            assertThat(counter.get(), equalTo(i));
        }
        ;
    }

    public void testCancelBeforeRunning() {
        AtomicInteger counter = new AtomicInteger(0);

        var runner = new RequestBasedTaskRunner(counter::incrementAndGet, threadPool, UTILITY_THREAD_POOL_NAME);
        runner.cancel();
        runner.requestNextRun();

        verifyNoInteractions(threadPool);
        assertThat(counter.get(), equalTo(0));
    }

    public void testCancelWhileRunning() {
        var expectedTimesRerun = randomInt(5);
        AtomicInteger counter = new AtomicInteger(0);

        var runnerRef = new AtomicReference<RequestBasedTaskRunner>();
        Runnable command = () -> {
            if (counter.getAndIncrement() < expectedTimesRerun) {
                runnerRef.get().requestNextRun();
            }
            runnerRef.get().cancel();
        };
        var runner = new RequestBasedTaskRunner(command, threadPool, UTILITY_THREAD_POOL_NAME);
        runnerRef.set(runner);
        runner.requestNextRun();

        verify(threadPool, times(1)).executor(eq(UTILITY_THREAD_POOL_NAME));
        verifyNoMoreInteractions(threadPool);
        assertThat(counter.get(), equalTo(1));

        runner.requestNextRun();
        verifyNoMoreInteractions(threadPool);
        assertThat(counter.get(), equalTo(1));
    }

}
