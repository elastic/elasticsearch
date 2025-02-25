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
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

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
        final TestThreadPool testThreadPool = new TestThreadPool("test");
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
}
