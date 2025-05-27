/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.engine.ThreadPoolMergeScheduler.MergeTask;
import org.elasticsearch.index.engine.ThreadPoolMergeScheduler.Schedule;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.index.engine.ThreadPoolMergeExecutorService.MAX_IO_RATE;
import static org.elasticsearch.index.engine.ThreadPoolMergeExecutorService.MIN_IO_RATE;
import static org.elasticsearch.index.engine.ThreadPoolMergeScheduler.Schedule.ABORT;
import static org.elasticsearch.index.engine.ThreadPoolMergeScheduler.Schedule.BACKLOG;
import static org.elasticsearch.index.engine.ThreadPoolMergeScheduler.Schedule.RUN;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ThreadPoolMergeExecutorServiceTests extends ESTestCase {

    private NodeEnvironment nodeEnvironment;

    @After
    public void closeNodeEnv() {
        if (nodeEnvironment != null) {
            nodeEnvironment.close();
            nodeEnvironment = null;
        }
    }

    public void testNewMergeTaskIsAbortedWhenThreadPoolIsShutdown() throws IOException {
        TestThreadPool testThreadPool = new TestThreadPool("test", Settings.EMPTY);
        nodeEnvironment = newNodeEnvironment(Settings.EMPTY);
        ThreadPoolMergeExecutorService threadPoolMergeExecutorService = getThreadPoolMergeExecutorService(
            testThreadPool,
            Settings.EMPTY,
            nodeEnvironment
        );
        // shutdown the thread pool
        testThreadPool.shutdown();
        MergeTask mergeTask = mock(MergeTask.class);
        when(mergeTask.supportsIOThrottling()).thenReturn(randomBoolean());
        assertFalse(threadPoolMergeExecutorService.submitMergeTask(mergeTask));
        verify(mergeTask).abort();
        verify(mergeTask, times(0)).schedule();
        verify(mergeTask, times(0)).run();
        verify(mergeTask, times(1)).abort();
        assertTrue(threadPoolMergeExecutorService.allDone());
    }

    public void testEnqueuedAndBackloggedMergesAreStillExecutedWhenThreadPoolIsShutdown() throws Exception {
        int mergeExecutorThreadCount = randomIntBetween(1, 5);
        // more merges than threads so that some are enqueued
        int mergesToSubmit = mergeExecutorThreadCount + randomIntBetween(1, 5);
        Settings settings = Settings.builder()
            .put(ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), true)
            .put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), mergeExecutorThreadCount)
            // disable fs available disk space feature for this test
            .put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_CHECK_INTERVAL_SETTING.getKey(), "0s")
            .build();
        TestThreadPool testThreadPool = new TestThreadPool("test", settings);
        nodeEnvironment = newNodeEnvironment(settings);
        ThreadPoolMergeExecutorService threadPoolMergeExecutorService = getThreadPoolMergeExecutorService(
            testThreadPool,
            settings,
            nodeEnvironment
        );
        var countingListener = new CountingMergeEventListener();
        threadPoolMergeExecutorService.registerMergeEventListener(countingListener);
        assertThat(threadPoolMergeExecutorService.getMaxConcurrentMerges(), equalTo(mergeExecutorThreadCount));
        Semaphore runMergeSemaphore = new Semaphore(0);
        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) testThreadPool.executor(ThreadPool.Names.MERGE);
        AtomicInteger doneMergesCount = new AtomicInteger(0);
        AtomicInteger reEnqueuedBackloggedMergesCount = new AtomicInteger();
        AtomicInteger abortedMergesCount = new AtomicInteger();
        // submit more merge tasks than there are threads so that some are enqueued
        for (int i = 0; i < mergesToSubmit; i++) {
            MergeTask mergeTask = mock(MergeTask.class);
            when(mergeTask.supportsIOThrottling()).thenReturn(randomBoolean());
            Schedule runOrAbort = randomFrom(RUN, ABORT);
            doAnswer(mock -> {
                // merges can be backlogged, but will be re-enqueued
                Schedule schedule = randomFrom(BACKLOG, runOrAbort);
                if (schedule == BACKLOG) {
                    // reenqueue backlogged merge task
                    new Thread(() -> threadPoolMergeExecutorService.reEnqueueBackloggedMergeTask(mergeTask)).start();
                    reEnqueuedBackloggedMergesCount.incrementAndGet();
                }
                return schedule;
            }).when(mergeTask).schedule();
            doAnswer(mock -> {
                // wait to be signalled before completing
                if (runOrAbort == ABORT) {
                    fail("merge task ran but it should've aborted instead");
                }
                runMergeSemaphore.acquireUninterruptibly();
                doneMergesCount.incrementAndGet();
                return null;
            }).when(mergeTask).run();
            doAnswer(mock -> {
                // wait to be signalled before completing
                if (runOrAbort == RUN) {
                    fail("merge task aborted but it should've ran instead");
                }
                runMergeSemaphore.acquireUninterruptibly();
                doneMergesCount.incrementAndGet();
                abortedMergesCount.incrementAndGet();
                return null;
            }).when(mergeTask).abort();
            threadPoolMergeExecutorService.submitMergeTask(mergeTask);
        }
        // assert merges are running and enqueued
        assertBusy(() -> {
            // assert that there are merge tasks running concurrently at the max allowed concurrency rate
            assertThat(threadPoolExecutor.getActiveCount(), is(mergeExecutorThreadCount));
            // with the other merge tasks enqueued
            assertThat(threadPoolExecutor.getQueue().size(), is(mergesToSubmit - mergeExecutorThreadCount));
        });
        assertBusy(
            () -> assertThat(
                countingListener.queued.get(),
                equalTo(threadPoolExecutor.getActiveCount() + threadPoolExecutor.getQueue().size() + reEnqueuedBackloggedMergesCount.get())
            )
        );
        // shutdown prevents new merge tasks to be enqueued but existing ones should be allowed to continue
        testThreadPool.shutdown();
        // assert all executors, except the merge one, are terminated
        for (String executorName : ThreadPool.THREAD_POOL_TYPES.keySet()) {
            assertTrue(testThreadPool.executor(executorName).isShutdown());
            if (ThreadPool.Names.MERGE.equals(executorName)) {
                assertFalse(testThreadPool.executor(executorName).isTerminated());
            } else {
                assertTrue(testThreadPool.executor(executorName).isTerminated());
            }
        }
        for (int i = 0; i < mergesToSubmit; i++) {
            // closing the thread pool is delayed because there are running and/or enqueued merge tasks
            assertFalse(testThreadPool.awaitTermination(1, TimeUnit.NANOSECONDS));
            assertTrue(threadPoolExecutor.isShutdown());
            assertFalse(threadPoolExecutor.isTerminated());
            // let merges run one by one and check thread pool
            runMergeSemaphore.release();
            int completedMergesCount = i + 1;
            assertBusy(() -> {
                assertThat(doneMergesCount.get(), is(completedMergesCount));
                assertThat(threadPoolExecutor.getCompletedTaskCount(), is((long) completedMergesCount));
                // active threads still working on the remaining merges
                assertThat(
                    threadPoolExecutor.getActiveCount(),
                    is(Math.min(mergeExecutorThreadCount, mergesToSubmit - completedMergesCount))
                );
                // with any of the other merges still enqueued
                assertThat(
                    threadPoolExecutor.getQueue().size(),
                    is(Math.max(mergesToSubmit - mergeExecutorThreadCount - completedMergesCount, 0))
                );
            });
        }
        assertBusy(() -> {
            assertTrue(testThreadPool.awaitTermination(1, TimeUnit.NANOSECONDS));
            assertTrue(threadPoolExecutor.isShutdown());
            assertTrue(threadPoolExecutor.isTerminated());
            assertTrue(threadPoolMergeExecutorService.allDone());
        });
        assertThat(countingListener.aborted.get() + countingListener.completed.get(), equalTo(doneMergesCount.get()));
        assertThat(countingListener.aborted.get(), equalTo(abortedMergesCount.get()));
    }

    public void testTargetIORateChangesWhenSubmittingMergeTasks() throws Exception {
        int mergeExecutorThreadCount = randomIntBetween(1, 5);
        int mergesStillToSubmit = randomIntBetween(10, 100);
        int mergesStillToComplete = mergesStillToSubmit;
        Settings settings = Settings.builder()
            .put(ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), true)
            .put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), mergeExecutorThreadCount)
            // disable fs available disk space feature for this test
            .put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_CHECK_INTERVAL_SETTING.getKey(), "0s")
            .build();
        nodeEnvironment = newNodeEnvironment(settings);
        try (TestThreadPool testThreadPool = new TestThreadPool("test", settings)) {
            ThreadPoolMergeExecutorService threadPoolMergeExecutorService = getThreadPoolMergeExecutorService(
                testThreadPool,
                settings,
                nodeEnvironment
            );
            assertThat(threadPoolMergeExecutorService.getMaxConcurrentMerges(), equalTo(mergeExecutorThreadCount));
            Semaphore runMergeSemaphore = new Semaphore(0);
            AtomicInteger submittedIOThrottledMergeTasks = new AtomicInteger();
            while (mergesStillToComplete > 0) {
                if (mergesStillToSubmit > 0 && (threadPoolMergeExecutorService.getRunningMergeTasks().isEmpty() || randomBoolean())) {
                    // submit new merge task
                    MergeTask mergeTask = mock(MergeTask.class);
                    boolean supportsIOThrottling = randomBoolean();
                    when(mergeTask.supportsIOThrottling()).thenReturn(supportsIOThrottling);
                    doAnswer(mock -> {
                        Schedule schedule = randomFrom(Schedule.values());
                        if (schedule == BACKLOG) {
                            testThreadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                                // reenqueue backlogged merge task
                                threadPoolMergeExecutorService.reEnqueueBackloggedMergeTask(mergeTask);
                            });
                        }
                        return schedule;
                    }).when(mergeTask).schedule();
                    doAnswer(mock -> {
                        // wait to be signalled before completing
                        runMergeSemaphore.acquire();
                        if (supportsIOThrottling) {
                            submittedIOThrottledMergeTasks.decrementAndGet();
                        }
                        return null;
                    }).when(mergeTask).run();
                    doAnswer(mock -> {
                        // wait to be signalled before completing
                        runMergeSemaphore.acquire();
                        if (supportsIOThrottling) {
                            submittedIOThrottledMergeTasks.decrementAndGet();
                        }
                        return null;
                    }).when(mergeTask).abort();
                    long currentIORate = threadPoolMergeExecutorService.getTargetIORateBytesPerSec();
                    threadPoolMergeExecutorService.submitMergeTask(mergeTask);
                    if (supportsIOThrottling) {
                        submittedIOThrottledMergeTasks.incrementAndGet();
                    }
                    long newIORate = threadPoolMergeExecutorService.getTargetIORateBytesPerSec();
                    if (supportsIOThrottling) {
                        if (submittedIOThrottledMergeTasks.get() < 2) {
                            // assert the IO rate decreases, with a floor limit, when there is just a single merge task running
                            assertThat(newIORate, either(is(MIN_IO_RATE.getBytes())).or(lessThan(currentIORate)));
                        } else if (submittedIOThrottledMergeTasks.get() > threadPoolMergeExecutorService.getMaxConcurrentMerges() * 2) {
                            // assert the IO rate increases, with a ceiling limit, when there are many merge tasks enqueued
                            assertThat(newIORate, either(is(MAX_IO_RATE.getBytes())).or(greaterThan(currentIORate)));
                        } else {
                            // assert the IO rate does NOT change when there are a couple of merge tasks enqueued
                            assertThat(newIORate, equalTo(currentIORate));
                        }
                    } else {
                        // assert the IO rate does not change, when the merge task doesn't support IO throttling
                        assertThat(newIORate, equalTo(currentIORate));
                    }
                    mergesStillToSubmit--;
                } else {
                    ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) testThreadPool.executor(ThreadPool.Names.MERGE);
                    long completedMerges = threadPoolExecutor.getCompletedTaskCount();
                    runMergeSemaphore.release();
                    // await merge to finish
                    assertBusy(() -> assertThat(threadPoolExecutor.getCompletedTaskCount(), is(completedMerges + 1)));
                    mergesStillToComplete--;
                }
            }
            assertBusy(() -> assertTrue(threadPoolMergeExecutorService.allDone()));
        }
    }

    public void testIORateIsAdjustedForRunningMergeTasks() throws Exception {
        int mergeExecutorThreadCount = randomIntBetween(1, 3);
        int mergesStillToSubmit = randomIntBetween(1, 10);
        int mergesStillToComplete = mergesStillToSubmit;
        Settings settings = Settings.builder()
            .put(ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), true)
            .put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), mergeExecutorThreadCount)
            // disable fs available disk space feature for this test
            .put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_CHECK_INTERVAL_SETTING.getKey(), "0s")
            .build();
        nodeEnvironment = newNodeEnvironment(settings);
        try (TestThreadPool testThreadPool = new TestThreadPool("test", settings)) {
            ThreadPoolMergeExecutorService threadPoolMergeExecutorService = getThreadPoolMergeExecutorService(
                testThreadPool,
                settings,
                nodeEnvironment
            );
            assertThat(threadPoolMergeExecutorService.getMaxConcurrentMerges(), equalTo(mergeExecutorThreadCount));
            ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) testThreadPool.executor(ThreadPool.Names.MERGE);
            Semaphore runMergeSemaphore = new Semaphore(0);
            Set<MergeTask> currentlyRunningMergeTasksSet = ConcurrentCollections.newConcurrentSet();
            while (mergesStillToComplete > 0) {
                if (mergesStillToSubmit > 0 && (currentlyRunningMergeTasksSet.isEmpty() || randomBoolean())) {
                    MergeTask mergeTask = mock(MergeTask.class);
                    // all tasks support IO throttling in this test case
                    when(mergeTask.supportsIOThrottling()).thenReturn(true);
                    doAnswer(mock -> {
                        Schedule schedule = randomFrom(Schedule.values());
                        if (schedule == BACKLOG) {
                            testThreadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                                // reenqueue backlogged merge task
                                threadPoolMergeExecutorService.reEnqueueBackloggedMergeTask(mergeTask);
                            });
                        }
                        return schedule;
                    }).when(mergeTask).schedule();
                    doAnswer(mock -> {
                        currentlyRunningMergeTasksSet.add(mergeTask);
                        // wait to be signalled before completing
                        runMergeSemaphore.acquire();
                        currentlyRunningMergeTasksSet.remove(mergeTask);
                        return null;
                    }).when(mergeTask).run();
                    doAnswer(mock -> {
                        // wait to be signalled before completing
                        runMergeSemaphore.acquire();
                        return null;
                    }).when(mergeTask).abort();
                    int activeMergeTasksCount = threadPoolExecutor.getActiveCount();
                    threadPoolMergeExecutorService.submitMergeTask(mergeTask);
                    long newIORate = threadPoolMergeExecutorService.getTargetIORateBytesPerSec();
                    // all currently running merge tasks must be IO throttled
                    assertBusy(() -> {
                        // await new merge to start executing
                        if (activeMergeTasksCount < mergeExecutorThreadCount) {
                            assertThat(threadPoolExecutor.getActiveCount(), is(activeMergeTasksCount + 1));
                        }
                        // assert IO throttle is set on the running merge tasks
                        for (MergeTask currentlyRunningMergeTask : currentlyRunningMergeTasksSet) {
                            var ioRateCaptor = ArgumentCaptor.forClass(Long.class);
                            // only interested in the last invocation
                            verify(currentlyRunningMergeTask, atLeastOnce()).setIORateLimit(ioRateCaptor.capture());
                            assertThat(ioRateCaptor.getValue(), is(newIORate));
                        }
                    });
                    mergesStillToSubmit--;
                } else {
                    long completedMerges = threadPoolExecutor.getCompletedTaskCount();
                    runMergeSemaphore.release();
                    // await merge to finish
                    assertBusy(() -> assertThat(threadPoolExecutor.getCompletedTaskCount(), is(completedMerges + 1)));
                    mergesStillToComplete--;
                }
            }
            assertBusy(() -> assertTrue(threadPoolMergeExecutorService.allDone()));
        }
    }

    public void testIORateAdjustedForSubmittedTasksWhenExecutionRateIsSpeedy() throws IOException {
        // the executor runs merge tasks at a faster rate than the rate that merge tasks are submitted
        int submittedVsExecutedRateOutOf1000 = randomIntBetween(0, 250);
        testIORateAdjustedForSubmittedTasks(randomIntBetween(50, 1000), submittedVsExecutedRateOutOf1000, randomIntBetween(0, 5));
        // executor starts running merges only after a considerable amount of merge tasks have already been submitted
        testIORateAdjustedForSubmittedTasks(randomIntBetween(50, 1000), submittedVsExecutedRateOutOf1000, randomIntBetween(5, 50));
    }

    public void testIORateAdjustedForSubmittedTasksWhenExecutionRateIsSluggish() throws IOException {
        // the executor runs merge tasks at a faster rate than the rate that merge tasks are submitted
        int submittedVsExecutedRateOutOf1000 = randomIntBetween(750, 1000);
        testIORateAdjustedForSubmittedTasks(randomIntBetween(50, 1000), submittedVsExecutedRateOutOf1000, randomIntBetween(0, 5));
        // executor starts running merges only after a considerable amount of merge tasks have already been submitted
        testIORateAdjustedForSubmittedTasks(randomIntBetween(50, 1000), submittedVsExecutedRateOutOf1000, randomIntBetween(5, 50));
    }

    public void testIORateAdjustedForSubmittedTasksWhenExecutionRateIsOnPar() throws IOException {
        // the executor runs merge tasks at a faster rate than the rate that merge tasks are submitted
        int submittedVsExecutedRateOutOf1000 = randomIntBetween(250, 750);
        testIORateAdjustedForSubmittedTasks(randomIntBetween(50, 1000), submittedVsExecutedRateOutOf1000, randomIntBetween(0, 5));
        // executor starts running merges only after a considerable amount of merge tasks have already been submitted
        testIORateAdjustedForSubmittedTasks(randomIntBetween(50, 1000), submittedVsExecutedRateOutOf1000, randomIntBetween(5, 50));
    }

    private void testIORateAdjustedForSubmittedTasks(int totalTasksToSubmit, int submittedVsExecutedRateOutOf1000, int initialTasksToSubmit)
        throws IOException {
        DeterministicTaskQueue mergeExecutorTaskQueue = new DeterministicTaskQueue();
        ThreadPool mergeExecutorThreadPool = mergeExecutorTaskQueue.getThreadPool();
        Settings settings = Settings.builder()
            // disable fs available disk space feature for this test
            .put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_CHECK_INTERVAL_SETTING.getKey(), "0s")
            .build();
        if (nodeEnvironment != null) {
            nodeEnvironment.close();
            nodeEnvironment = null;
        }
        nodeEnvironment = newNodeEnvironment(settings);
        ThreadPoolMergeExecutorService threadPoolMergeExecutorService = getThreadPoolMergeExecutorService(
            mergeExecutorThreadPool,
            settings,
            nodeEnvironment
        );
        final AtomicInteger currentlySubmittedMergeTaskCount = new AtomicInteger();
        final AtomicLong targetIORateLimit = new AtomicLong(ThreadPoolMergeExecutorService.START_IO_RATE.getBytes());
        final AtomicReference<MergeTask> lastRunTask = new AtomicReference<>();
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
                when(mergeTask.schedule()).thenReturn(RUN);
                doAnswer(mock -> {
                    lastRunTask.set(mergeTask);
                    return null;
                }).when(mergeTask).run();
                currentlySubmittedMergeTaskCount.incrementAndGet();
                totalTasksToSubmit--;
                initialTasksCounter--;
                threadPoolMergeExecutorService.submitMergeTask(mergeTask);
                long newTargetIORateLimit = threadPoolMergeExecutorService.getTargetIORateBytesPerSec();
                if (currentlySubmittedMergeTaskCount.get() < 2) {
                    // assert the IO rate decreases, with a floor limit, when there are few merge tasks enqueued
                    assertThat(newTargetIORateLimit, either(is(MIN_IO_RATE.getBytes())).or(lessThan(targetIORateLimit.get())));
                } else if (currentlySubmittedMergeTaskCount.get() > threadPoolMergeExecutorService.getMaxConcurrentMerges() * 2) {
                    // assert the IO rate increases, with a ceiling limit, when there are many merge tasks enqueued
                    assertThat(newTargetIORateLimit, either(is(MAX_IO_RATE.getBytes())).or(greaterThan(targetIORateLimit.get())));
                } else {
                    // assert the IO rate does not change, when there are a couple of merge tasks enqueued
                    assertThat(newTargetIORateLimit, equalTo(targetIORateLimit.get()));
                }
                targetIORateLimit.set(newTargetIORateLimit);
            } else {
                // execute already submitted merge task
                if (runOneTask(mergeExecutorTaskQueue)) {
                    // task is done, no longer just submitted
                    currentlySubmittedMergeTaskCount.decrementAndGet();
                    // assert IO rate is invoked on the merge task that just ran
                    assertNotNull(lastRunTask.get());
                    var ioRateCaptor = ArgumentCaptor.forClass(Long.class);
                    verify(lastRunTask.get(), times(1)).setIORateLimit(ioRateCaptor.capture());
                    assertThat(ioRateCaptor.getValue(), is(targetIORateLimit.get()));
                    lastRunTask.set(null);
                }
            }
        }
        assertTrue(threadPoolMergeExecutorService.allDone());
    }

    public void testMergeTasksRunConcurrently() throws Exception {
        // at least 2 merges allowed to run concurrently
        int mergeExecutorThreadCount = randomIntBetween(2, 5);
        Settings settings = Settings.builder()
            .put(ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), true)
            .put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), mergeExecutorThreadCount)
            // disable fs available disk space feature for this test
            .put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_CHECK_INTERVAL_SETTING.getKey(), "0s")
            .build();
        nodeEnvironment = newNodeEnvironment(settings);
        try (TestThreadPool testThreadPool = new TestThreadPool("test", settings)) {
            ThreadPoolMergeExecutorService threadPoolMergeExecutorService = getThreadPoolMergeExecutorService(
                testThreadPool,
                settings,
                nodeEnvironment
            );
            assertThat(threadPoolMergeExecutorService.getMaxConcurrentMerges(), equalTo(mergeExecutorThreadCount));
            // more merge tasks than max concurrent merges allowed to run concurrently
            int totalMergeTasksCount = mergeExecutorThreadCount + randomIntBetween(1, 5);
            Semaphore runMergeSemaphore = new Semaphore(0);
            ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) testThreadPool.executor(ThreadPool.Names.MERGE);
            // submit all merge tasks
            for (int i = 0; i < totalMergeTasksCount; i++) {
                MergeTask mergeTask = mock(MergeTask.class);
                when(mergeTask.supportsIOThrottling()).thenReturn(randomBoolean());
                doAnswer(mock -> {
                    // each individual merge task can either "run" or be "backlogged"
                    Schedule schedule = randomFrom(RUN, BACKLOG);
                    if (schedule == BACKLOG) {
                        testThreadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                            // reenqueue backlogged merge task
                            threadPoolMergeExecutorService.reEnqueueBackloggedMergeTask(mergeTask);
                        });
                    }
                    return schedule;
                }).when(mergeTask).schedule();
                doAnswer(mock -> {
                    // wait to be signalled before completing
                    runMergeSemaphore.acquire();
                    return null;
                }).when(mergeTask).run();
                doAnswer(mock -> {
                    fail("This test doesn't deal with aborted merge tasks");
                    return null;
                }).when(mergeTask).abort();
                threadPoolMergeExecutorService.submitMergeTask(mergeTask);
            }
            // assert stats while merge tasks finish
            for (int completedTasksCount = 0; completedTasksCount < totalMergeTasksCount
                - mergeExecutorThreadCount; completedTasksCount++) {
                int finalCompletedTasksCount = completedTasksCount;
                assertBusy(() -> {
                    // assert that there are merge tasks running concurrently at the max allowed concurrency rate
                    assertThat(threadPoolMergeExecutorService.getRunningMergeTasks().size(), is(mergeExecutorThreadCount));
                    // with the other merge tasks enqueued
                    assertThat(
                        threadPoolMergeExecutorService.getMergeTasksQueueLength(),
                        is(totalMergeTasksCount - mergeExecutorThreadCount - finalCompletedTasksCount)
                    );
                    // also check thread-pool stats for the same
                    assertThat(threadPoolExecutor.getActiveCount(), is(mergeExecutorThreadCount));
                    assertThat(
                        threadPoolExecutor.getQueue().size(),
                        is(totalMergeTasksCount - mergeExecutorThreadCount - finalCompletedTasksCount)
                    );
                });
                // let one merge task finish running
                runMergeSemaphore.release();
            }
            // there are now fewer merge tasks still running than available threads
            for (int remainingMergeTasksCount = mergeExecutorThreadCount; remainingMergeTasksCount >= 0; remainingMergeTasksCount--) {
                int finalRemainingMergeTasksCount = remainingMergeTasksCount;
                assertBusy(() -> {
                    // there are fewer available merges than available threads
                    assertThat(threadPoolMergeExecutorService.getRunningMergeTasks().size(), is(finalRemainingMergeTasksCount));
                    // no more merges enqueued
                    assertThat(threadPoolMergeExecutorService.getMergeTasksQueueLength(), is(0));
                    // also check thread-pool stats for the same
                    assertThat(threadPoolExecutor.getActiveCount(), is(finalRemainingMergeTasksCount));
                    assertThat(threadPoolExecutor.getQueue().size(), is(0));
                });
                // let one merge task finish running
                runMergeSemaphore.release();
            }
            assertBusy(() -> assertTrue(threadPoolMergeExecutorService.allDone()));
        }
    }

    public void testThreadPoolStatsWithBackloggedMergeTasks() throws Exception {
        int mergeExecutorThreadCount = randomIntBetween(1, 3);
        Settings settings = Settings.builder()
            .put(ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), true)
            .put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), mergeExecutorThreadCount)
            // disable fs available disk space feature for this test
            .put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_CHECK_INTERVAL_SETTING.getKey(), "0s")
            .build();
        nodeEnvironment = newNodeEnvironment(settings);
        try (TestThreadPool testThreadPool = new TestThreadPool("test", settings)) {
            ThreadPoolMergeExecutorService threadPoolMergeExecutorService = getThreadPoolMergeExecutorService(
                testThreadPool,
                settings,
                nodeEnvironment
            );
            assertThat(threadPoolMergeExecutorService.getMaxConcurrentMerges(), equalTo(mergeExecutorThreadCount));
            int totalMergeTasksCount = randomIntBetween(1, 10);
            ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) testThreadPool.executor(ThreadPool.Names.MERGE);
            List<MergeTask> backloggedMergeTasksList = new ArrayList<>();
            for (int i = 0; i < totalMergeTasksCount; i++) {
                MergeTask mergeTask = mock(MergeTask.class);
                when(mergeTask.supportsIOThrottling()).thenReturn(randomBoolean());
                boolean runNowOrBacklog = randomBoolean();
                if (runNowOrBacklog) {
                    when(mergeTask.schedule()).thenReturn(randomFrom(RUN, ABORT));
                } else {
                    // first backlog, then run
                    when(mergeTask.schedule()).thenReturn(BACKLOG, randomFrom(RUN, ABORT));
                    backloggedMergeTasksList.add(mergeTask);
                }
                threadPoolMergeExecutorService.submitMergeTask(mergeTask);
            }
            assertBusy(() -> {
                // all runnable merge tasks should show as "completed"
                assertThat(threadPoolExecutor.getCompletedTaskCount(), is((long) (totalMergeTasksCount - backloggedMergeTasksList.size())));
                if (backloggedMergeTasksList.size() >= mergeExecutorThreadCount) {
                    // active tasks waiting for backlogged merge tasks to be re-enqueued
                    assertThat(threadPoolExecutor.getActiveCount(), is(mergeExecutorThreadCount));
                    assertThat(threadPoolExecutor.getQueue().size(), is(backloggedMergeTasksList.size() - mergeExecutorThreadCount));
                } else {
                    assertThat(threadPoolExecutor.getActiveCount(), is(backloggedMergeTasksList.size()));
                    assertThat(threadPoolExecutor.getQueue().size(), is(0));
                }
                assertThat(threadPoolMergeExecutorService.getMergeTasksQueueLength(), is(0));
            });
            // re-enqueue backlogged merge tasks
            for (MergeTask backloggedMergeTask : backloggedMergeTasksList) {
                threadPoolMergeExecutorService.reEnqueueBackloggedMergeTask(backloggedMergeTask);
            }
            assertBusy(() -> {
                // all merge tasks should now show as "completed"
                assertThat(threadPoolExecutor.getCompletedTaskCount(), is((long) totalMergeTasksCount));
                assertThat(threadPoolExecutor.getActiveCount(), is(0));
                assertThat(threadPoolExecutor.getQueue().size(), is(0));
                assertTrue(threadPoolMergeExecutorService.allDone());
            });
        }
    }

    public void testBackloggedMergeTasksExecuteExactlyOnce() throws Exception {
        int mergeExecutorThreadCount = randomIntBetween(1, 3);
        Settings settings = Settings.builder()
            .put(ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), true)
            // few merge threads, in order to increase contention
            .put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), mergeExecutorThreadCount)
            // disable fs available disk space feature for this test
            .put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_CHECK_INTERVAL_SETTING.getKey(), "0s")
            .build();
        nodeEnvironment = newNodeEnvironment(settings);
        try (TestThreadPool testThreadPool = new TestThreadPool("test", settings)) {
            ThreadPoolMergeExecutorService threadPoolMergeExecutorService = getThreadPoolMergeExecutorService(
                testThreadPool,
                settings,
                nodeEnvironment
            );
            assertThat(threadPoolMergeExecutorService.getMaxConcurrentMerges(), equalTo(mergeExecutorThreadCount));
            // many merge tasks concurrently
            int mergeTaskCount = randomIntBetween(10, 100);
            CountDownLatch mergeTasksReadyLatch = new CountDownLatch(mergeTaskCount);
            CountDownLatch submitTaskLatch = new CountDownLatch(1);
            Collection<MergeTask> runMergeTasks = ConcurrentCollections.newConcurrentSet();
            Collection<MergeTask> abortMergeTasks = ConcurrentCollections.newConcurrentSet();
            for (int i = 0; i < mergeTaskCount; i++) {
                new Thread(() -> {
                    MergeTask mergeTask = mock(MergeTask.class);
                    when(mergeTask.supportsIOThrottling()).thenReturn(randomBoolean());
                    doAnswer(mock -> {
                        // each individual merge task can either "run" or be "backlogged"
                        Schedule schedule = randomFrom(RUN, ABORT, BACKLOG);
                        if (schedule == BACKLOG) {
                            testThreadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                                // reenqueue backlogged merge task
                                threadPoolMergeExecutorService.reEnqueueBackloggedMergeTask(mergeTask);
                            });
                        }
                        if (schedule == RUN) {
                            runMergeTasks.add(mergeTask);
                        }
                        if (schedule == ABORT) {
                            abortMergeTasks.add(mergeTask);
                        }
                        return schedule;
                    }).when(mergeTask).schedule();
                    mergeTasksReadyLatch.countDown();
                    // make all threads submit merge tasks at once
                    safeAwait(submitTaskLatch);
                    threadPoolMergeExecutorService.submitMergeTask(mergeTask);
                }).start();
            }
            safeAwait(mergeTasksReadyLatch);
            submitTaskLatch.countDown();
            assertBusy(() -> {
                assertThat(runMergeTasks.size() + abortMergeTasks.size(), is(mergeTaskCount));
                for (MergeTask mergeTask : runMergeTasks) {
                    verify(mergeTask, times(1)).run();
                    verify(mergeTask, times(0)).abort();
                    if (mergeTask.supportsIOThrottling() == false) {
                        verify(mergeTask, times(0)).setIORateLimit(anyLong());
                    }
                }
                for (MergeTask mergeTask : abortMergeTasks) {
                    verify(mergeTask, times(0)).run();
                    verify(mergeTask, times(1)).abort();
                    verify(mergeTask, times(0)).setIORateLimit(anyLong());
                }
                assertTrue(threadPoolMergeExecutorService.allDone());
            });
        }
    }

    public void testMergeTasksExecuteInSizeOrder() throws IOException {
        DeterministicTaskQueue mergeExecutorTaskQueue = new DeterministicTaskQueue();
        ThreadPool mergeExecutorThreadPool = mergeExecutorTaskQueue.getThreadPool();
        Settings settings = Settings.builder()
            // disable fs available disk space feature for this test
            .put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_CHECK_INTERVAL_SETTING.getKey(), "0s")
            .build();
        nodeEnvironment = newNodeEnvironment(settings);
        ThreadPoolMergeExecutorService threadPoolMergeExecutorService = getThreadPoolMergeExecutorService(
            mergeExecutorThreadPool,
            settings,
            nodeEnvironment
        );
        DeterministicTaskQueue reEnqueueBackloggedTaskQueue = new DeterministicTaskQueue();
        int mergeTaskCount = randomIntBetween(10, 100);
        // sort merge tasks available to run by size
        PriorityQueue<MergeTask> mergeTasksAvailableToRun = new PriorityQueue<>(
            mergeTaskCount,
            Comparator.comparingLong(MergeTask::estimatedRemainingMergeSize)
        );
        for (int i = 0; i < mergeTaskCount; i++) {
            MergeTask mergeTask = mock(MergeTask.class);
            when(mergeTask.supportsIOThrottling()).thenReturn(randomBoolean());
            // merge tasks of various sizes (0 might be a valid value)
            when(mergeTask.estimatedRemainingMergeSize()).thenReturn(randomLongBetween(0, 10));
            doAnswer(mock -> {
                // each individual merge task can either "run" or be "backlogged" at any point in time
                Schedule schedule = randomFrom(Schedule.values());
                // in either case, the merge task is, at least temporarily, not "available" to run
                mergeTasksAvailableToRun.remove(mergeTask);
                // if merge task cannot run, it is backlogged, and should be re enqueued some time in the future
                if (schedule == BACKLOG) {
                    // reenqueue backlogged merge task sometime in the future
                    reEnqueueBackloggedTaskQueue.scheduleNow(() -> {
                        // reenqueue backlogged merge task sometime in the future
                        threadPoolMergeExecutorService.reEnqueueBackloggedMergeTask(mergeTask);
                        // the merge task should once again be "available" to run
                        mergeTasksAvailableToRun.add(mergeTask);
                    });
                }
                // hack: avoid blocking for unavailable merge task by running one re-enqueuing task now
                if (schedule == BACKLOG && mergeTasksAvailableToRun.isEmpty()) {
                    assertTrue(runOneTask(reEnqueueBackloggedTaskQueue));
                }
                if (schedule == RUN && mergeTasksAvailableToRun.isEmpty() == false) {
                    // assert the merge task that's now going to run is the smallest of the ones currently available to run
                    assertThat(
                        mergeTask.estimatedRemainingMergeSize(),
                        lessThanOrEqualTo(mergeTasksAvailableToRun.peek().estimatedRemainingMergeSize())
                    );
                }
                return schedule;
            }).when(mergeTask).schedule();
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

    private static class CountingMergeEventListener implements MergeEventListener {
        AtomicInteger queued = new AtomicInteger();
        AtomicInteger aborted = new AtomicInteger();
        AtomicInteger completed = new AtomicInteger();

        @Override
        public void onMergeQueued(OnGoingMerge merge, long estimateMergeMemoryBytes) {
            queued.incrementAndGet();
        }

        @Override
        public void onMergeCompleted(OnGoingMerge merge) {
            completed.incrementAndGet();
        }

        @Override
        public void onMergeAborted(OnGoingMerge merge) {
            aborted.incrementAndGet();
        }
    }

    static ThreadPoolMergeExecutorService getThreadPoolMergeExecutorService(
        ThreadPool threadPool,
        Settings settings,
        NodeEnvironment nodeEnvironment
    ) {
        ThreadPoolMergeExecutorService threadPoolMergeExecutorService = ThreadPoolMergeExecutorService
            .maybeCreateThreadPoolMergeExecutorService(threadPool, ClusterSettings.createBuiltInClusterSettings(settings), nodeEnvironment);
        assertNotNull(threadPoolMergeExecutorService);
        assertTrue(threadPoolMergeExecutorService.allDone());
        return threadPoolMergeExecutorService;
    }

    private static boolean runOneTask(DeterministicTaskQueue deterministicTaskQueue) {
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
