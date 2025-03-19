/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.index.MergeScheduler.MergeSource;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.store.MergeInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergeSchedulerConfig;
import org.elasticsearch.index.engine.ThreadPoolMergeScheduler.MergeTask;
import org.elasticsearch.index.engine.ThreadPoolMergeScheduler.Schedule;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class ThreadPoolMergeSchedulerTests extends ESTestCase {

    public void testMergesExecuteInSizeOrder() throws IOException {
        DeterministicTaskQueue threadPoolTaskQueue = new DeterministicTaskQueue();
        ThreadPoolMergeExecutorService threadPoolMergeExecutorService = ThreadPoolMergeExecutorServiceTests
            .getThreadPoolMergeExecutorService(threadPoolTaskQueue.getThreadPool());
        try (
            ThreadPoolMergeScheduler threadPoolMergeScheduler = new ThreadPoolMergeScheduler(
                new ShardId("index", "_na_", 1),
                IndexSettingsModule.newIndexSettings("index", Settings.EMPTY),
                threadPoolMergeExecutorService
            )
        ) {
            List<OneMerge> executedMergesList = new ArrayList<>();
            int mergeCount = randomIntBetween(2, 10);
            for (int i = 0; i < mergeCount; i++) {
                MergeSource mergeSource = mock(MergeSource.class);
                OneMerge oneMerge = mock(OneMerge.class);
                when(oneMerge.getStoreMergeInfo()).thenReturn(getNewMergeInfo(randomLongBetween(1L, 10L)));
                when(oneMerge.getMergeProgress()).thenReturn(new MergePolicy.OneMergeProgress());
                when(mergeSource.getNextMerge()).thenReturn(oneMerge, (OneMerge) null);
                doAnswer(invocation -> {
                    OneMerge merge = (OneMerge) invocation.getArguments()[0];
                    assertFalse(merge.isAborted());
                    executedMergesList.add(merge);
                    return null;
                }).when(mergeSource).merge(any(OneMerge.class));
                threadPoolMergeScheduler.merge(mergeSource, randomFrom(MergeTrigger.values()));
            }
            threadPoolTaskQueue.runAllTasks();
            assertThat(executedMergesList.size(), is(mergeCount));
            // assert merges are executed in ascending size order
            for (int i = 1; i < mergeCount; i++) {
                assertThat(
                    executedMergesList.get(i - 1).getStoreMergeInfo().estimatedMergeBytes(),
                    lessThanOrEqualTo(executedMergesList.get(i).getStoreMergeInfo().estimatedMergeBytes())
                );
            }
        }
        assertTrue(threadPoolMergeExecutorService.allDone());
    }

    public void testSimpleMergeTaskBacklogging() {
        int mergeExecutorThreadCount = randomIntBetween(1, 5);
        Settings mergeSchedulerSettings = Settings.builder()
            .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), mergeExecutorThreadCount)
            .build();
        ThreadPoolMergeExecutorService threadPoolMergeExecutorService = mock(ThreadPoolMergeExecutorService.class);
        // close method waits for running merges to finish, but this test leaves running merges around
        ThreadPoolMergeScheduler threadPoolMergeScheduler = new ThreadPoolMergeScheduler(
            new ShardId("index", "_na_", 1),
            IndexSettingsModule.newIndexSettings("index", mergeSchedulerSettings),
            threadPoolMergeExecutorService
        );
        // more merge tasks than merge threads
        int mergeCount = mergeExecutorThreadCount + randomIntBetween(1, 5);
        for (int i = 0; i < mergeCount; i++) {
            MergeSource mergeSource = mock(MergeSource.class);
            OneMerge oneMerge = mock(OneMerge.class);
            when(oneMerge.getStoreMergeInfo()).thenReturn(getNewMergeInfo(randomLongBetween(1L, 10L)));
            when(oneMerge.getMergeProgress()).thenReturn(new MergePolicy.OneMergeProgress());
            when(mergeSource.getNextMerge()).thenReturn(oneMerge, (OneMerge) null);
            Schedule schedule = threadPoolMergeScheduler.schedule(
                threadPoolMergeScheduler.newMergeTask(mergeSource, oneMerge, randomFrom(MergeTrigger.values()))
            );
            if (i < mergeExecutorThreadCount) {
                assertThat(schedule, is(Schedule.RUN));
            } else {
                assertThat(schedule, is(Schedule.BACKLOG));
            }
        }
        assertThat(threadPoolMergeScheduler.getRunningMergeTasks().size(), is(mergeExecutorThreadCount));
        assertThat(threadPoolMergeScheduler.getBackloggedMergeTasks().size(), is(mergeCount - mergeExecutorThreadCount));
    }

    public void testSimpleMergeTaskReEnqueueingBySize() {
        int mergeExecutorThreadCount = randomIntBetween(1, 5);
        Settings mergeSchedulerSettings = Settings.builder()
            .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), mergeExecutorThreadCount)
            .build();
        ThreadPoolMergeExecutorService threadPoolMergeExecutorService = mock(ThreadPoolMergeExecutorService.class);
        // close method waits for running merges to finish, but this test leaves running merges around
        ThreadPoolMergeScheduler threadPoolMergeScheduler = new ThreadPoolMergeScheduler(
            new ShardId("index", "_na_", 1),
            IndexSettingsModule.newIndexSettings("index", mergeSchedulerSettings),
            threadPoolMergeExecutorService
        );
        // sort backlogged merges by size
        PriorityQueue<MergeTask> backloggedMergeTasks = new PriorityQueue<>(16, Comparator.comparingLong(MergeTask::estimatedMergeSize));
        // more merge tasks than merge threads
        int mergeCount = mergeExecutorThreadCount + randomIntBetween(2, 10);
        for (int i = 0; i < mergeCount; i++) {
            MergeSource mergeSource = mock(MergeSource.class);
            OneMerge oneMerge = mock(OneMerge.class);
            when(oneMerge.getStoreMergeInfo()).thenReturn(getNewMergeInfo(randomLongBetween(1L, 10L)));
            when(oneMerge.getMergeProgress()).thenReturn(new MergePolicy.OneMergeProgress());
            when(mergeSource.getNextMerge()).thenReturn(oneMerge, (OneMerge) null);
            MergeTask mergeTask = threadPoolMergeScheduler.newMergeTask(mergeSource, oneMerge, randomFrom(MergeTrigger.values()));
            Schedule schedule = threadPoolMergeScheduler.schedule(mergeTask);
            if (i < mergeExecutorThreadCount) {
                assertThat(schedule, is(Schedule.RUN));
            } else {
                assertThat(schedule, is(Schedule.BACKLOG));
                backloggedMergeTasks.add(mergeTask);
            }
        }
        assertThat(threadPoolMergeScheduler.getRunningMergeTasks().size(), is(mergeExecutorThreadCount));
        assertThat(threadPoolMergeScheduler.getBackloggedMergeTasks().size(), is(backloggedMergeTasks.size()));
        int enqueuedTasksCount = mergeCount - mergeExecutorThreadCount;
        for (int i = 0; i < enqueuedTasksCount; i++) {
            assertThat(threadPoolMergeScheduler.getBackloggedMergeTasks().size(), is(enqueuedTasksCount - i));
            MergeTask runningMergeTask = randomFrom(threadPoolMergeScheduler.getRunningMergeTasks().values());
            runningMergeTask.run();
            var submittedMergeTaskCaptor = ArgumentCaptor.forClass(MergeTask.class);
            verify(threadPoolMergeExecutorService, times(i + 1)).reEnqueueBackloggedMergeTask(submittedMergeTaskCaptor.capture());
            assertThat(submittedMergeTaskCaptor.getValue(), is(backloggedMergeTasks.poll()));
            Schedule schedule = threadPoolMergeScheduler.schedule(submittedMergeTaskCaptor.getValue());
            assertThat(schedule, is(Schedule.RUN));
            assertThat(threadPoolMergeScheduler.getRunningMergeTasks().size(), is(mergeExecutorThreadCount));
        }
    }

    public void testMergeSourceWithFollowUpMergesRunSequentially() throws Exception {
        // test with min 2 allowed concurrent merges
        int mergeExecutorThreadCount = randomIntBetween(2, 5);
        Settings settings = Settings.builder()
            .put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), mergeExecutorThreadCount)
            .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), mergeExecutorThreadCount)
            .build();
        try (TestThreadPool testThreadPool = new TestThreadPool("test", settings)) {
            ThreadPoolMergeExecutorService threadPoolMergeExecutorService = ThreadPoolMergeExecutorServiceTests
                .getThreadPoolMergeExecutorService(testThreadPool);
            assertThat(threadPoolMergeExecutorService.getMaxConcurrentMerges(), equalTo(mergeExecutorThreadCount));
            try (
                ThreadPoolMergeScheduler threadPoolMergeScheduler = new ThreadPoolMergeScheduler(
                    new ShardId("index", "_na_", 1),
                    IndexSettingsModule.newIndexSettings("index", settings),
                    threadPoolMergeExecutorService
                )
            ) {
                MergeSource mergeSource = mock(MergeSource.class);
                OneMerge firstMerge = mock(OneMerge.class);
                when(firstMerge.getStoreMergeInfo()).thenReturn(getNewMergeInfo(randomLongBetween(1L, 10L)));
                when(firstMerge.getMergeProgress()).thenReturn(new MergePolicy.OneMergeProgress());
                // at least one followup merge + null (i.e. no more followups)
                int followUpMergeCount = randomIntBetween(2, 10);
                OneMerge[] followUpMerges = new OneMerge[followUpMergeCount];
                followUpMerges[followUpMergeCount - 1] = null;
                for (int i = 0; i < followUpMergeCount - 1; i++) {
                    OneMerge oneMerge = mock(OneMerge.class);
                    when(oneMerge.getStoreMergeInfo()).thenReturn(getNewMergeInfo(randomLongBetween(1L, 10L)));
                    when(oneMerge.getMergeProgress()).thenReturn(new MergePolicy.OneMergeProgress());
                    followUpMerges[i] = oneMerge;
                }
                // the merge source with follow-up merges
                when(mergeSource.getNextMerge()).thenReturn(firstMerge, followUpMerges);
                AtomicBoolean isMergeInProgress = new AtomicBoolean();
                AtomicInteger runMergeIdx = new AtomicInteger();
                Semaphore runMergeSemaphore = new Semaphore(0);
                Semaphore nextMergeSemaphore = new Semaphore(0);
                doAnswer(invocation -> {
                    // assert only one merge can be in-progress at any point-in-time
                    assertTrue(isMergeInProgress.compareAndSet(false, true));
                    OneMerge mergeInvocation = (OneMerge) invocation.getArguments()[0];
                    assertFalse(mergeInvocation.isAborted());
                    // assert merges run in the order they are produced by the merge source
                    if (runMergeIdx.get() == 0) {
                        assertThat(mergeInvocation, is(firstMerge));
                    } else {
                        assertThat(mergeInvocation, is(followUpMerges[runMergeIdx.get() - 1]));
                    }
                    runMergeIdx.incrementAndGet();
                    // await before returning from the merge in order to really ensure that follow-up merges don't run concurrently
                    nextMergeSemaphore.release();
                    runMergeSemaphore.acquire();
                    assertTrue(isMergeInProgress.compareAndSet(true, false));
                    return null;
                }).when(mergeSource).merge(any(OneMerge.class));
                // trigger run merges on the merge source
                threadPoolMergeScheduler.merge(mergeSource, randomFrom(MergeTrigger.values()));
                do {
                    // let merges run, but wait for the in-progress one to signal it is running
                    nextMergeSemaphore.acquire();
                    runMergeSemaphore.release();
                } while (runMergeIdx.get() < followUpMergeCount);
                assertBusy(() -> assertTrue(threadPoolMergeExecutorService.allDone()));
            }
        }
    }

    public void testMergesRunConcurrently() throws Exception {
        // min 2 allowed concurrent merges, per scheduler
        int mergeSchedulerMaxThreadCount = randomIntBetween(2, 4);
        // the merge executor has at least 1 extra thread available
        int mergeExecutorThreadCount = mergeSchedulerMaxThreadCount + randomIntBetween(1, 3);
        Settings settings = Settings.builder()
            .put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), mergeExecutorThreadCount)
            .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), mergeSchedulerMaxThreadCount)
            .build();
        try (TestThreadPool testThreadPool = new TestThreadPool("test", settings)) {
            ThreadPoolMergeExecutorService threadPoolMergeExecutorService = ThreadPoolMergeExecutorServiceTests
                .getThreadPoolMergeExecutorService(testThreadPool);
            assertThat(threadPoolMergeExecutorService.getMaxConcurrentMerges(), equalTo(mergeExecutorThreadCount));
            ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) testThreadPool.executor(ThreadPool.Names.MERGE);
            try (
                ThreadPoolMergeScheduler threadPoolMergeScheduler = new ThreadPoolMergeScheduler(
                    new ShardId("index", "_na_", 1),
                    IndexSettingsModule.newIndexSettings("index", settings),
                    threadPoolMergeExecutorService
                )
            ) {
                // at least 1 extra merge than there are concurrently allowed
                int mergeCount = mergeExecutorThreadCount + randomIntBetween(1, 10);
                Semaphore runMergeSemaphore = new Semaphore(0);
                for (int i = 0; i < mergeCount; i++) {
                    MergeSource mergeSource = mock(MergeSource.class);
                    OneMerge oneMerge = mock(OneMerge.class);
                    when(oneMerge.getStoreMergeInfo()).thenReturn(getNewMergeInfo(randomLongBetween(1L, 10L)));
                    when(oneMerge.getMergeProgress()).thenReturn(new MergePolicy.OneMergeProgress());
                    when(mergeSource.getNextMerge()).thenReturn(oneMerge, (OneMerge) null);
                    doAnswer(invocation -> {
                        OneMerge merge = (OneMerge) invocation.getArguments()[0];
                        assertFalse(merge.isAborted());
                        // wait to be signalled before completing
                        runMergeSemaphore.acquire();
                        return null;
                    }).when(mergeSource).merge(any(OneMerge.class));
                    threadPoolMergeScheduler.merge(mergeSource, randomFrom(MergeTrigger.values()));
                }
                for (int completedMergesCount = 0; completedMergesCount < mergeCount
                    - mergeSchedulerMaxThreadCount; completedMergesCount++) {
                    int finalCompletedMergesCount = completedMergesCount;
                    assertBusy(() -> {
                        // assert that there are merges running concurrently at the max allowed concurrency rate
                        assertThat(threadPoolMergeScheduler.getRunningMergeTasks().size(), is(mergeSchedulerMaxThreadCount));
                        // with the other merges backlogged
                        assertThat(
                            threadPoolMergeScheduler.getBackloggedMergeTasks().size(),
                            is(mergeCount - mergeSchedulerMaxThreadCount - finalCompletedMergesCount)
                        );
                        // also check the same for the thread-pool executor
                        assertThat(threadPoolMergeExecutorService.getRunningMergeTasks().size(), is(mergeSchedulerMaxThreadCount));
                        // queued merge tasks do not include backlogged merges
                        assertThat(threadPoolMergeExecutorService.getQueuedMergeTasks().size(), is(0));
                        // also check thread-pool stats for the same
                        // there are active thread-pool threads waiting for the backlogged merge tasks to be re-enqueued
                        int activeMergeThreads = Math.min(mergeCount - finalCompletedMergesCount, mergeExecutorThreadCount);
                        assertThat(threadPoolExecutor.getActiveCount(), is(activeMergeThreads));
                        assertThat(threadPoolExecutor.getQueue().size(), is(mergeCount - finalCompletedMergesCount - activeMergeThreads));
                    });
                    // let one merge task finish running
                    runMergeSemaphore.release();
                }
                // there are now fewer merges still running than available threads
                for (int remainingMergesCount = mergeSchedulerMaxThreadCount; remainingMergesCount >= 0; remainingMergesCount--) {
                    int finalRemainingMergesCount = remainingMergesCount;
                    assertBusy(() -> {
                        // there are fewer available merges than available threads
                        assertThat(threadPoolMergeScheduler.getRunningMergeTasks().size(), is(finalRemainingMergesCount));
                        // no more backlogged merges
                        assertThat(threadPoolMergeScheduler.getBackloggedMergeTasks().size(), is(0));
                        // also check thread-pool executor for the same
                        assertThat(threadPoolMergeExecutorService.getRunningMergeTasks().size(), is(finalRemainingMergesCount));
                        // no more backlogged merges
                        assertThat(threadPoolMergeExecutorService.getQueuedMergeTasks().size(), is(0));
                        // also check thread-pool stats for the same
                        assertThat(threadPoolExecutor.getActiveCount(), is(finalRemainingMergesCount));
                        assertThat(threadPoolExecutor.getQueue().size(), is(0));
                    });
                    // let one merge task finish running
                    runMergeSemaphore.release();
                }
                assertBusy(() -> assertTrue(threadPoolMergeExecutorService.allDone()));
            }
        }
    }

    public void testSchedulerCloseWaitsForRunningMerge() throws Exception {
        int mergeSchedulerMaxThreadCount = randomIntBetween(1, 3);
        int mergeExecutorThreadCount = randomIntBetween(1, 3);
        Settings settings = Settings.builder()
            .put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), mergeExecutorThreadCount)
            .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), mergeSchedulerMaxThreadCount)
            .build();
        try (TestThreadPool testThreadPool = new TestThreadPool("test", settings)) {
            ThreadPoolMergeExecutorService threadPoolMergeExecutorService = ThreadPoolMergeExecutorServiceTests
                .getThreadPoolMergeExecutorService(testThreadPool);
            assertThat(threadPoolMergeExecutorService.getMaxConcurrentMerges(), equalTo(mergeExecutorThreadCount));
            try (
                ThreadPoolMergeScheduler threadPoolMergeScheduler = new ThreadPoolMergeScheduler(
                    new ShardId("index", "_na_", 1),
                    IndexSettingsModule.newIndexSettings("index", settings),
                    threadPoolMergeExecutorService
                )
            ) {
                CountDownLatch mergeDoneLatch = new CountDownLatch(1);
                MergeSource mergeSource = mock(MergeSource.class);
                OneMerge oneMerge = mock(OneMerge.class);
                when(oneMerge.getStoreMergeInfo()).thenReturn(getNewMergeInfo(randomLongBetween(1L, 10L)));
                when(oneMerge.getMergeProgress()).thenReturn(new MergePolicy.OneMergeProgress());
                when(mergeSource.getNextMerge()).thenReturn(oneMerge, (OneMerge) null);
                doAnswer(invocation -> {
                    OneMerge merge = (OneMerge) invocation.getArguments()[0];
                    assertFalse(merge.isAborted());
                    // wait to be signalled before completing the merge
                    mergeDoneLatch.await();
                    return null;
                }).when(mergeSource).merge(any(OneMerge.class));
                threadPoolMergeScheduler.merge(mergeSource, randomFrom(MergeTrigger.values()));
                Thread t = new Thread(() -> {
                    try {
                        threadPoolMergeScheduler.close();
                    } catch (IOException e) {
                        fail(e);
                    }
                });
                t.start();
                try {
                    assertTrue(t.isAlive());
                    // ensure the merge scheduler is effectively "closed"
                    assertBusy(() -> {
                        MergeSource mergeSource2 = mock(MergeSource.class);
                        threadPoolMergeScheduler.merge(mergeSource2, randomFrom(MergeTrigger.values()));
                        // when the merge scheduler is closed it won't pull in any new merges from the merge source
                        verifyNoInteractions(mergeSource2);
                    });
                    // assert the merge still shows up as "running"
                    assertThat(threadPoolMergeScheduler.getRunningMergeTasks().keySet(), contains(oneMerge));
                    assertThat(threadPoolMergeScheduler.getBackloggedMergeTasks().size(), is(0));
                    assertTrue(t.isAlive());
                    // signal the merge to finish
                    mergeDoneLatch.countDown();
                } finally {
                    t.join();
                }
                assertBusy(() -> {
                    assertThat(threadPoolMergeScheduler.getRunningMergeTasks().size(), is(0));
                    assertThat(threadPoolMergeScheduler.getBackloggedMergeTasks().size(), is(0));
                    assertTrue(threadPoolMergeExecutorService.allDone());
                });
            }
        }
    }

    public void testAutoIOThrottleForMergeTasksWhenSchedulerDisablesIt() throws Exception {
        // merge scheduler configured with auto IO throttle disabled
        Settings settings = Settings.builder().put(MergeSchedulerConfig.AUTO_THROTTLE_SETTING.getKey(), false).build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("index", settings);
        ThreadPoolMergeExecutorService threadPoolMergeExecutorService = mock(ThreadPoolMergeExecutorService.class);
        MergePolicy.OneMergeProgress oneMergeProgress = new MergePolicy.OneMergeProgress();
        OneMerge oneMerge = mock(OneMerge.class);
        when(oneMerge.getStoreMergeInfo()).thenReturn(getNewMergeInfo(randomNonNegativeLong()));
        when(oneMerge.getMergeProgress()).thenReturn(oneMergeProgress);
        MergeSource mergeSource = mock(MergeSource.class);
        when(mergeSource.getNextMerge()).thenReturn(oneMerge);
        try (
            ThreadPoolMergeScheduler threadPoolMergeScheduler = new ThreadPoolMergeScheduler(
                new ShardId("index", "_na_", 1),
                indexSettings,
                threadPoolMergeExecutorService
            )
        ) {
            threadPoolMergeScheduler.merge(mergeSource, randomFrom(MergeTrigger.values()));
            var submittedMergeTaskCaptor = ArgumentCaptor.forClass(MergeTask.class);
            verify(threadPoolMergeExecutorService).submitMergeTask(submittedMergeTaskCaptor.capture());
            assertFalse(submittedMergeTaskCaptor.getValue().supportsIOThrottling());
        }
    }

    public void testAutoIOThrottleForMergeTasks() throws Exception {
        final Settings.Builder settingsBuilder = Settings.builder();
        // merge scheduler configured with auto IO throttle enabled
        if (randomBoolean()) {
            settingsBuilder.put(MergeSchedulerConfig.AUTO_THROTTLE_SETTING.getKey(), true);
        }
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("index", settingsBuilder.build());
        MergePolicy.OneMergeProgress oneMergeProgress = new MergePolicy.OneMergeProgress();
        OneMerge oneMerge = mock(OneMerge.class);
        // forced merge with a set number of segments
        when(oneMerge.getStoreMergeInfo()).thenReturn(getNewMergeInfo(randomNonNegativeLong(), randomNonNegativeInt()));
        when(oneMerge.getMergeProgress()).thenReturn(oneMergeProgress);
        MergeSource mergeSource = mock(MergeSource.class);
        when(mergeSource.getNextMerge()).thenReturn(oneMerge);
        ThreadPoolMergeExecutorService threadPoolMergeExecutorService = mock(ThreadPoolMergeExecutorService.class);
        try (
            ThreadPoolMergeScheduler threadPoolMergeScheduler = new ThreadPoolMergeScheduler(
                new ShardId("index", "_na_", 1),
                indexSettings,
                threadPoolMergeExecutorService
            )
        ) {
            threadPoolMergeScheduler.merge(mergeSource, randomFrom(MergeTrigger.values()));
            var submittedMergeTaskCaptor = ArgumentCaptor.forClass(MergeTask.class);
            verify(threadPoolMergeExecutorService).submitMergeTask(submittedMergeTaskCaptor.capture());
            // forced merge tasks should not be IO throttled
            assertFalse(submittedMergeTaskCaptor.getValue().supportsIOThrottling());
        }
        // NOT a forced merge
        when(oneMerge.getStoreMergeInfo()).thenReturn(getNewMergeInfo(randomNonNegativeLong(), -1));
        threadPoolMergeExecutorService = mock(ThreadPoolMergeExecutorService.class);
        try (
            ThreadPoolMergeScheduler threadPoolMergeScheduler = new ThreadPoolMergeScheduler(
                new ShardId("index", "_na_", 1),
                indexSettings,
                threadPoolMergeExecutorService
            )
        ) {
            // merge submitted upon closing
            threadPoolMergeScheduler.merge(mergeSource, MergeTrigger.CLOSING);
            var submittedMergeTaskCaptor = ArgumentCaptor.forClass(MergeTask.class);
            verify(threadPoolMergeExecutorService).submitMergeTask(submittedMergeTaskCaptor.capture());
            // merge tasks submitted when closing should not be IO throttled
            assertFalse(submittedMergeTaskCaptor.getValue().supportsIOThrottling());
        }
        // otherwise, merge tasks should be auto IO throttled
        threadPoolMergeExecutorService = mock(ThreadPoolMergeExecutorService.class);
        try (
            ThreadPoolMergeScheduler threadPoolMergeScheduler = new ThreadPoolMergeScheduler(
                new ShardId("index", "_na_", 1),
                indexSettings,
                threadPoolMergeExecutorService
            )
        ) {
            // merge submitted upon closing
            threadPoolMergeScheduler.merge(
                mergeSource,
                randomValueOtherThan(MergeTrigger.CLOSING, () -> randomFrom(MergeTrigger.values()))
            );
            var submittedMergeTaskCaptor = ArgumentCaptor.forClass(MergeTask.class);
            verify(threadPoolMergeExecutorService).submitMergeTask(submittedMergeTaskCaptor.capture());
            // merge tasks should be auto IO throttled
            assertTrue(submittedMergeTaskCaptor.getValue().supportsIOThrottling());
        }
    }

    private static MergeInfo getNewMergeInfo(long estimatedMergeBytes) {
        return getNewMergeInfo(estimatedMergeBytes, randomFrom(-1, randomNonNegativeInt()));
    }

    private static MergeInfo getNewMergeInfo(long estimatedMergeBytes, int maxNumSegments) {
        return new MergeInfo(randomNonNegativeInt(), estimatedMergeBytes, randomBoolean(), maxNumSegments);
    }
}
