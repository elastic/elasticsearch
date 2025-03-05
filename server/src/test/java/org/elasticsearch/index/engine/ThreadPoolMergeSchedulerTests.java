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
import org.apache.lucene.index.MergeScheduler.MergeSource;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.store.MergeInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergeSchedulerConfig;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
            List<MergePolicy.OneMerge> executedMergesList = new ArrayList<>();
            int mergeCount = randomIntBetween(2, 10);
            for (int i = 0; i < mergeCount; i++) {
                MergeSource mergeSource = mock(MergeSource.class);
                MergePolicy.OneMerge oneMerge = mock(MergePolicy.OneMerge.class);
                when(oneMerge.getStoreMergeInfo()).thenReturn(
                        new MergeInfo(randomNonNegativeInt(), randomLongBetween(1L, 10L), randomBoolean(), randomFrom(-1, randomNonNegativeInt()))
                );
                when(oneMerge.getMergeProgress()).thenReturn(new MergePolicy.OneMergeProgress());
                when(mergeSource.getNextMerge()).thenReturn(oneMerge, (MergePolicy.OneMerge) null);
                doAnswer(invocation -> {
                    executedMergesList.add((MergePolicy.OneMerge) invocation.getArguments()[0]);
                    return null;
                }).when(mergeSource).merge(any(MergePolicy.OneMerge.class));
                threadPoolMergeScheduler.merge(mergeSource, randomFrom(MergeTrigger.values()));
            }
            deterministicTaskQueue.runAllTasks();
            assertThat(executedMergesList.size(), is(mergeCount));
            // assert merges are executed in ascending size order
            for (int i = 1; i < mergeCount; i++) {
                assertThat(executedMergesList.get(i - 1).getStoreMergeInfo().estimatedMergeBytes(),
                        lessThanOrEqualTo(executedMergesList.get(i).getStoreMergeInfo().estimatedMergeBytes()));
            }
        }
        assertTrue(threadPoolMergeExecutorService.allDone());
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
        MergePolicy.OneMerge oneMerge = mock(MergePolicy.OneMerge.class);
        when(oneMerge.getStoreMergeInfo()).thenReturn(
            new MergeInfo(randomNonNegativeInt(), randomNonNegativeLong(), randomBoolean(), randomFrom(-1, randomNonNegativeInt()))
        );
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
        MergePolicy.OneMerge oneMerge = mock(MergePolicy.OneMerge.class);
        // forced merge with a set number of segments
        when(oneMerge.getStoreMergeInfo()).thenReturn(
            new MergeInfo(randomNonNegativeInt(), randomNonNegativeLong(), randomBoolean(), randomNonNegativeInt())
        );
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
        when(oneMerge.getStoreMergeInfo()).thenReturn(new MergeInfo(randomNonNegativeInt(), randomNonNegativeLong(), randomBoolean(), -1));
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
}
