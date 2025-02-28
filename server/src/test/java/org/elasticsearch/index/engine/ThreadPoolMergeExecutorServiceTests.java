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
import org.elasticsearch.index.engine.ThreadPoolMergeScheduler.MergeTask;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ThreadPoolMergeExecutorServiceTests extends ESTestCase {

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
        MergeTask mergeTask = mock(MergeTask.class);
        boolean mergeTaskSupportsIOThrottling = randomBoolean();
        when(mergeTask.supportsIOThrottling()).thenReturn(mergeTaskSupportsIOThrottling);
        assertFalse(threadPoolMergeExecutorService.submitMergeTask(mergeTask));
        verify(mergeTask).abortOnGoingMerge();
        verify(mergeTask, times(0)).runNowOrBacklog();
        verify(mergeTask, times(0)).run();
        assertTrue(threadPoolMergeExecutorService.allDone());
    }

    public void testBackloggedMergeTasksExecuteExactlyOnce() throws Exception {
        int mergeExecutorThreadCount = randomIntBetween(1, 3);
        Settings settings = Settings.builder()
            .put(ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), true)
            // few merge threads, in order to increase contention
            .put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), mergeExecutorThreadCount)
            .build();
        try (TestThreadPool testThreadPool = new TestThreadPool("test", settings)) {
            ThreadPoolMergeExecutorService threadPoolMergeExecutorService = ThreadPoolMergeExecutorService
                .maybeCreateThreadPoolMergeExecutorService(testThreadPool, settings);
            assertNotNull(threadPoolMergeExecutorService);
            assertThat(threadPoolMergeExecutorService.getMaxConcurrentMerges(), equalTo(mergeExecutorThreadCount));
            // many merge tasks concurrently
            int mergeTaskCount = randomIntBetween(10, 100);
            CountDownLatch mergeTasksReadyLatch = new CountDownLatch(mergeTaskCount);
            CountDownLatch submitTaskLatch = new CountDownLatch(1);
            Collection<MergeTask> generatedMergeTasks = ConcurrentCollections.newConcurrentSet();
            for (int i = 0; i < mergeTaskCount; i++) {
                new Thread(() -> {
                    MergeTask mergeTask = mock(MergeTask.class);
                    boolean supportsIOThrottling = randomBoolean();
                    when(mergeTask.supportsIOThrottling()).thenReturn(supportsIOThrottling);
                    long mergeSize = randomNonNegativeLong();
                    when(mergeTask.estimatedMergeSize()).thenReturn(mergeSize);
                    doAnswer(mock -> {
                        // each individual merge task can either "run" or be "backlogged"
                        boolean runNowOrBacklog = randomBoolean();
                        if (runNowOrBacklog == false) {
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
            assertBusy(() -> {
                for (MergeTask mergeTask : generatedMergeTasks) {
                    verify(mergeTask, times(1)).run();
                    if (mergeTask.supportsIOThrottling() == false) {
                        verify(mergeTask, times(0)).setIORateLimit(anyDouble());
                    }
                }
                threadPoolMergeExecutorService.allDone();
            });
        }
    }

    public void testBackloggedMergeTasksExecuteInSizeOrder() {
        DeterministicTaskQueue mergeExecutorTaskQueue = new DeterministicTaskQueue();
        ThreadPool mergeExecutorThreadPool = mergeExecutorTaskQueue.getThreadPool();
        ThreadPoolMergeExecutorService threadPoolMergeExecutorService = ThreadPoolMergeExecutorService
            .maybeCreateThreadPoolMergeExecutorService(
                mergeExecutorThreadPool,
                Settings.builder().put(ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), true).build()
            );
        assertNotNull(threadPoolMergeExecutorService);
        DeterministicTaskQueue reEnqueueBackloggedTaskQueue = new DeterministicTaskQueue();
        int mergeTaskCount = randomIntBetween(10, 100);
        PriorityQueue<MergeTask> mergeTasksAvailableToRun = new PriorityQueue<>(
            mergeTaskCount,
            Comparator.comparingLong(MergeTask::estimatedMergeSize)
        );
        for (int i = 0; i < mergeTaskCount; i++) {
            MergeTask mergeTask = mock(MergeTask.class);
            when(mergeTask.isRunning()).thenReturn(false);
            boolean supportsIOThrottling = randomBoolean();
            when(mergeTask.supportsIOThrottling()).thenReturn(supportsIOThrottling);
            // merge tasks of various sizes (0 might be a valid value)
            long mergeSize = randomLongBetween(0, 10);
            when(mergeTask.estimatedMergeSize()).thenReturn(mergeSize);
            doAnswer(mock -> {
                // each individual merge task can either "run" or be "backlogged" at any point in time
                boolean runNowOrBacklog = randomBoolean();
                // in either case, the merge task is, at least temporarily, not "available" to run
                mergeTasksAvailableToRun.remove(mergeTask);
                // if merge task cannot run, it is backlogged, and should be re enqueued some time in the future
                if (runNowOrBacklog == false) {
                    // reenqueue backlogged merge task sometime in the future
                    reEnqueueBackloggedTaskQueue.scheduleNow(() -> {
                        // reenqueue backlogged merge task sometime in the future
                        threadPoolMergeExecutorService.reEnqueueBackloggedMergeTask(mergeTask);
                        // the merge task should once again be "available" to run
                        mergeTasksAvailableToRun.add(mergeTask);
                    });
                }
                // avoid blocking for unavailable merge task by running one re-enqueuing task now
                if (runNowOrBacklog == false && mergeTasksAvailableToRun.isEmpty()) {
                    assertTrue(runOneTask(reEnqueueBackloggedTaskQueue));
                }
                if (runNowOrBacklog && mergeTasksAvailableToRun.isEmpty() == false) {
                    // assert the merge task that's now going to run is the smallest of the ones currently available to run
                    assertTrue(mergeTask.estimatedMergeSize() <= mergeTasksAvailableToRun.peek().estimatedMergeSize());
                }
                return runNowOrBacklog;
            }).when(mergeTask).runNowOrBacklog();
            mergeTasksAvailableToRun.add(mergeTask);
            threadPoolMergeExecutorService.submitMergeTask(mergeTask);
        }
        while (true) {
            // re-enqueue merge tasks
            if (mergeTasksAvailableToRun.isEmpty() || randomBoolean()) {
                boolean backlogReEnqueued = runOneTask(reEnqueueBackloggedTaskQueue);
                if (mergeTasksAvailableToRun.isEmpty() && backlogReEnqueued == false) {
                    // test complete, all merges ran, and none is backlogged
                    assertFalse(mergeExecutorTaskQueue.hasAnyTasks());
                    assertFalse(reEnqueueBackloggedTaskQueue.hasAnyTasks());
                    assertTrue(threadPoolMergeExecutorService.allDone());
                    break;
                }
            } else {
                // run one merge task
                runOneTask(mergeExecutorTaskQueue);
            }
        }
    }

    private boolean runOneTask(DeterministicTaskQueue deterministicTaskQueue) {
        while (deterministicTaskQueue.hasAnyTasks()) {
            if (deterministicTaskQueue.hasRunnableTasks()) {
                deterministicTaskQueue.runRandomTask();
                return true;
            } else {
                deterministicTaskQueue.advanceTime();
            }
        }
        return false;
    }
}
