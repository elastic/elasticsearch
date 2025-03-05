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
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ThreadPoolMergeSchedulerTests extends ESTestCase {

    DeterministicTaskQueue deterministicTaskQueue;
    ThreadPool mergesExecutorThreadPool;
    Settings settingsWithMergeScheduler;
    IndexSettings indexSettings;
    ThreadPoolMergeExecutorService threadPoolMergeExecutorService;

    @Before
    public void setUpThreadPool() {
        deterministicTaskQueue = new DeterministicTaskQueue();
        mergesExecutorThreadPool = deterministicTaskQueue.getThreadPool();
        settingsWithMergeScheduler = Settings.builder()
            .put(ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), true)
            .build();
        indexSettings = IndexSettingsModule.newIndexSettings("index", settingsWithMergeScheduler);
        threadPoolMergeExecutorService = ThreadPoolMergeExecutorService.maybeCreateThreadPoolMergeExecutorService(
            mergesExecutorThreadPool,
            settingsWithMergeScheduler
        );
    }

    public void testMergesExecuteInSizeOrder() throws IOException {
        try (
            ThreadPoolMergeScheduler threadPoolMergeScheduler = new ThreadPoolMergeScheduler(
                new ShardId("index", "_na_", 1),
                indexSettings,
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
            deterministicTaskQueue.runAllTasks();
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

    public void testMergeSourceWithFollowUpMergesRunSequentially() throws Exception {
        // test with min 2 allowed concurrent merges
        int mergeExecutorThreadCount = randomIntBetween(2, 5);
        Settings settings = Settings.builder()
            .put(settingsWithMergeScheduler)
            .put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), mergeExecutorThreadCount)
            .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), mergeExecutorThreadCount)
            .build();
        try (TestThreadPool testThreadPool = new TestThreadPool("test", settings)) {
            ThreadPoolMergeExecutorService threadPoolMergeExecutorService = ThreadPoolMergeExecutorService
                .maybeCreateThreadPoolMergeExecutorService(testThreadPool, settings);
            assertNotNull(threadPoolMergeExecutorService);
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
                    // assert merges run in the order they are submitted
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
                threadPoolMergeScheduler.merge(mergeSource, randomFrom(MergeTrigger.values()));
                do {
                    nextMergeSemaphore.acquire();
                    runMergeSemaphore.release();
                } while (runMergeIdx.get() < followUpMergeCount);
                assertBusy(() -> assertTrue(threadPoolMergeExecutorService.allDone()));
            }
        }
    }

    public void testAutoIOThrottleForMergeTasksWhenSchedulerDisablesIt() throws Exception {
        // merge scheduler configured with auto IO throttle disabled
        Settings settings = Settings.builder()
            .put(settingsWithMergeScheduler)
            .put(MergeSchedulerConfig.AUTO_THROTTLE_SETTING.getKey(), false)
            .build();
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
            var submittedMergeTaskCaptor = ArgumentCaptor.forClass(ThreadPoolMergeScheduler.MergeTask.class);
            verify(threadPoolMergeExecutorService).submitMergeTask(submittedMergeTaskCaptor.capture());
            assertFalse(submittedMergeTaskCaptor.getValue().supportsIOThrottling());
        }
    }

    public void testAutoIOThrottleForMergeTasks() throws Exception {
        // merge scheduler configured with auto IO throttle disabled
        final Settings settings;
        if (randomBoolean()) {
            settings = Settings.builder()
                .put(settingsWithMergeScheduler)
                .put(MergeSchedulerConfig.AUTO_THROTTLE_SETTING.getKey(), true)
                .build();
        } else {
            settings = Settings.builder().put(settingsWithMergeScheduler).build();
        }
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("index", settings);
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
            var submittedMergeTaskCaptor = ArgumentCaptor.forClass(ThreadPoolMergeScheduler.MergeTask.class);
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
            var submittedMergeTaskCaptor = ArgumentCaptor.forClass(ThreadPoolMergeScheduler.MergeTask.class);
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
            var submittedMergeTaskCaptor = ArgumentCaptor.forClass(ThreadPoolMergeScheduler.MergeTask.class);
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
