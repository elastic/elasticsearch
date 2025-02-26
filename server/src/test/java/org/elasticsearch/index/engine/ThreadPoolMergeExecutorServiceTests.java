/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ThreadPoolMergeExecutorServiceTests extends ESTestCase {

    DeterministicTaskQueue deterministicTaskQueue;
    ThreadPool testThreadPool;
    IndexSettings indexSettings;

    @Before
    public void setUpThreadPool() {
        deterministicTaskQueue = new DeterministicTaskQueue();
        testThreadPool = deterministicTaskQueue.getThreadPool();
        indexSettings = IndexSettingsModule.newIndexSettings("index", Settings.EMPTY);
    }

    public void testMergeTasksAreAbortedWhenThreadPoolIsShutdown() {
        TestThreadPool testThreadPool = new TestThreadPool("test");
        ThreadPoolMergeExecutorService threadPoolMergeExecutorService = ThreadPoolMergeExecutorService
            .maybeCreateThreadPoolMergeExecutorService(
                testThreadPool,
                Settings.builder().put(ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), true).build()
            );
        assertNotNull(threadPoolMergeExecutorService);
        assertTrue(threadPoolMergeExecutorService.allDone());
        // shutdown the thread pool
        testThreadPool.shutdown();
        ThreadPoolMergeScheduler.MergeTask mergeTask = mock(ThreadPoolMergeScheduler.MergeTask.class);
        when(mergeTask.isRunning()).thenReturn(false);
        boolean mergeTaskSupportsIOThrottling = randomBoolean();
        when(mergeTask.supportsIOThrottling()).thenReturn(mergeTaskSupportsIOThrottling);
        assertFalse(threadPoolMergeExecutorService.submitMergeTask(mergeTask));
        verify(mergeTask).abortOnGoingMerge();
        verify(mergeTask, times(0)).runNowOrBacklog();
        verify(mergeTask, times(0)).run();
        assertTrue(threadPoolMergeExecutorService.allDone());
    }

    public void testBackloggedMergeTasksAreAllExecutedExactlyOnce() throws Exception {
        int mergeExecutorThreadCount = randomIntBetween(1, 2);
        Settings settings = Settings.builder()
                .put(ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), true)
                // results in few merge threads, in order to increase contention
                .put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), mergeExecutorThreadCount)
                .build();
        try (TestThreadPool testThreadPool = new TestThreadPool("test", settings)) {
            ThreadPoolMergeExecutorService threadPoolMergeExecutorService = ThreadPoolMergeExecutorService
                    .maybeCreateThreadPoolMergeExecutorService(testThreadPool, settings);
            assertNotNull(threadPoolMergeExecutorService);
            assertThat(threadPoolMergeExecutorService.getMaxConcurrentMerges(), equalTo(mergeExecutorThreadCount));
            int mergeTaskCount = randomIntBetween(3, 30);
            CountDownLatch mergeTasksDoneLatch = new CountDownLatch(mergeTaskCount);
            CountDownLatch mergeTasksReadyLatch = new CountDownLatch(mergeTaskCount);
            CountDownLatch submitTaskLatch = new CountDownLatch(1);
            Collection<ThreadPoolMergeScheduler.MergeTask> generatedMergeTasks = ConcurrentCollections.newConcurrentSet();
            for (int i = 0; i < mergeTaskCount; i++) {
                new Thread(()-> {
                    ThreadPoolMergeScheduler.MergeTask mergeTask = mock(ThreadPoolMergeScheduler.MergeTask.class);
                    when(mergeTask.isRunning()).thenReturn(false);
                    boolean supportsIOThrottling = randomBoolean();
                    when(mergeTask.supportsIOThrottling()).thenReturn(supportsIOThrottling);
                    long mergeSize = randomNonNegativeLong();
                    when(mergeTask.estimatedMergeSize()).thenReturn(mergeSize);
                    doAnswer(mock -> {
                        // each individual merge task can either "run" or be "backlogged"
                        boolean runNowOrBacklog = randomBoolean();
                        if (runNowOrBacklog) {
                            mergeTasksDoneLatch.countDown();
                        } else {
                            testThreadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                                // reenqueue backlogged merge task
                                threadPoolMergeExecutorService.reEnqueueBackloggedMergeTask(mergeTask);
                            });
                        }
                        return runNowOrBacklog;
                    }).when(mergeTask).runNowOrBacklog();
                    generatedMergeTasks.add(mergeTask);
                    mergeTasksReadyLatch.countDown();
                    // make all threads submit merge tasks at once
                    safeAwait(submitTaskLatch);
                    threadPoolMergeExecutorService.submitMergeTask(mergeTask);
                }).start();
            }
            safeAwait(mergeTasksReadyLatch);
            submitTaskLatch.countDown();
            safeAwait(mergeTasksDoneLatch);
            assertBusy(() -> {
                for (ThreadPoolMergeScheduler.MergeTask mergeTask : generatedMergeTasks) {
                    verify(mergeTask, times(1)).run();
                    if (mergeTask.supportsIOThrottling()) {
                        verify(mergeTask).setIORateLimit(anyDouble());
                    } else {
                        verify(mergeTask, times(0)).setIORateLimit(anyDouble());
                    }
                }
                threadPoolMergeExecutorService.allDone();
            });
        }
    }
}
