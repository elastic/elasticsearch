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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ThreadPoolMergeExecutorServiceTests extends ESTestCase {

    public void testMergeTaskIsAbortedWhenThreadPoolIsShutdown() {
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

    public void testIORateAdjustedForSubmittedTasksWhenExecutionRateIsSpeedy() {
        // the executor runs merge tasks at a faster rate than the rate that merge tasks are submitted
        int submittedVsExecutedRateOutOf1000 = randomIntBetween(0, 250);
        testIORateAdjustedForNewlySubmittedTasks(randomIntBetween(50, 1000), submittedVsExecutedRateOutOf1000, randomIntBetween(0, 5));
        // executor starts running merges only after a considerable amount of merge tasks have already been submitted
        testIORateAdjustedForNewlySubmittedTasks(randomIntBetween(50, 1000), submittedVsExecutedRateOutOf1000, randomIntBetween(5, 50));
    }

    public void testIORateAdjustedForSubmittedTasksWhenExecutionRateIsSluggish() {
        // the executor runs merge tasks at a faster rate than the rate that merge tasks are submitted
        int submittedVsExecutedRateOutOf1000 = randomIntBetween(750, 1000);
        testIORateAdjustedForNewlySubmittedTasks(randomIntBetween(50, 1000), submittedVsExecutedRateOutOf1000, randomIntBetween(0, 5));
        // executor starts running merges only after a considerable amount of merge tasks have already been submitted
        testIORateAdjustedForNewlySubmittedTasks(randomIntBetween(50, 1000), submittedVsExecutedRateOutOf1000, randomIntBetween(5, 50));
    }

    public void testIORateAdjustedForSubmittedTasksWhenExecutionRateIsOnPar() {
        // the executor runs merge tasks at a faster rate than the rate that merge tasks are submitted
        int submittedVsExecutedRateOutOf1000 = randomIntBetween(250, 750);
        testIORateAdjustedForNewlySubmittedTasks(randomIntBetween(50, 1000), submittedVsExecutedRateOutOf1000, randomIntBetween(0, 5));
        // executor starts running merges only after a considerable amount of merge tasks have already been submitted
        testIORateAdjustedForNewlySubmittedTasks(randomIntBetween(50, 1000), submittedVsExecutedRateOutOf1000, randomIntBetween(5, 50));
    }

    private void testIORateAdjustedForNewlySubmittedTasks(
        int totalTasksToSubmit,
        int submittedVsExecutedRateOutOf1000,
        int initialTasksToSubmit
    ) {
        DeterministicTaskQueue mergeExecutorTaskQueue = new DeterministicTaskQueue();
        ThreadPool mergeExecutorThreadPool = mergeExecutorTaskQueue.getThreadPool();
        ThreadPoolMergeExecutorService threadPoolMergeExecutorService = ThreadPoolMergeExecutorService
            .maybeCreateThreadPoolMergeExecutorService(
                mergeExecutorThreadPool,
                Settings.builder().put(ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), true).build()
            );
        assertNotNull(threadPoolMergeExecutorService);
        final AtomicInteger currentlySubmittedMergeTaskCount = new AtomicInteger();
        final AtomicLong targetIORateLimit = new AtomicLong(ThreadPoolMergeExecutorService.START_IO_RATE.getBytes());
        final AtomicBoolean setIORateForTaskInvoked = new AtomicBoolean();
        int initialTasksCounter = Math.min(initialTasksToSubmit, totalTasksToSubmit);
        while (totalTasksToSubmit > 0 || mergeExecutorTaskQueue.hasAnyTasks()) {
            if (mergeExecutorTaskQueue.hasAnyTasks() == false // always submit if there are no outstanding merge tasks
                || initialTasksCounter > 0 // first submit all the initial tasks
                || (randomIntBetween(0, 1000) < submittedVsExecutedRateOutOf1000 && totalTasksToSubmit > 0)) {
                // submit new merge task
                MergeTask mergeTask = mock(MergeTask.class);
                // all merge tasks support IO throttling in this test
                when(mergeTask.supportsIOThrottling()).thenReturn(true);
                // always run the task
                when(mergeTask.runNowOrBacklog()).thenReturn(true);
                doAnswer(mock -> {
                    long taskIORateLimit = (Long) mock.getArguments()[0];
                    // assert the IO rate for the task is set
                    assertThat(taskIORateLimit, equalTo(targetIORateLimit.get()));
                    assertFalse(setIORateForTaskInvoked.get());
                    setIORateForTaskInvoked.set(true);
                    return null;
                }).when(mergeTask).setIORateLimit(anyLong());
                doAnswer(mock -> {
                    // IO rate limit always set while running
                    assertTrue(setIORateForTaskInvoked.get());
                    // always run the tasks
                    return true;
                }).when(mergeTask).run();
                currentlySubmittedMergeTaskCount.incrementAndGet();
                totalTasksToSubmit--;
                initialTasksCounter--;
                threadPoolMergeExecutorService.submitMergeTask(mergeTask);
                long newTargetIORateLimit = threadPoolMergeExecutorService.getTargetIORateBytesPerSec();
                if (currentlySubmittedMergeTaskCount.get() < threadPoolMergeExecutorService.getConcurrentMergesFloorLimitForThrottling()) {
                    // assert the IO rate decreases, with a floor limit, when there are few merge tasks enqueued
                    assertTrue(
                        newTargetIORateLimit == ThreadPoolMergeExecutorService.MIN_IO_RATE.getBytes()
                            || newTargetIORateLimit < targetIORateLimit.get()
                    );
                } else if (currentlySubmittedMergeTaskCount.get() > threadPoolMergeExecutorService
                    .getConcurrentMergesCeilLimitForThrottling()) {
                        // assert the IO rate increases, with a ceiling limit, when there are many merge tasks enqueued
                        assertTrue(
                            newTargetIORateLimit == ThreadPoolMergeExecutorService.MAX_IO_RATE.getBytes()
                                || newTargetIORateLimit > targetIORateLimit.get()
                        );
                    } else {
                        // assert the IO rate does change, when there are a couple of merge tasks enqueued
                        assertThat(newTargetIORateLimit, equalTo(targetIORateLimit.get()));
                    }
                targetIORateLimit.set(newTargetIORateLimit);
            } else {
                setIORateForTaskInvoked.set(false);
                // execute already submitted merge task
                if (runOneTask(mergeExecutorTaskQueue)) {
                    assertTrue(setIORateForTaskInvoked.get());
                    // task is done, no longer just submitted
                    currentlySubmittedMergeTaskCount.decrementAndGet();
                }
            }
        }
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
                        verify(mergeTask, times(0)).setIORateLimit(anyLong());
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
        // sort merge tasks available to run by size
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
