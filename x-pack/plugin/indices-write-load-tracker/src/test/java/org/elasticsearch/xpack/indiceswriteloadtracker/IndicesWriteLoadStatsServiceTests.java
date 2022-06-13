/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.indiceswriteloadtracker;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.TestThreadPool;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.indiceswriteloadtracker.IndicesWriteLoadTrackerPlugin.WRITE_LOAD_COLLECTOR_THREAD_POOL_NAME;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class IndicesWriteLoadStatsServiceTests extends ESTestCase {

    public void testSamplingAndStoreAreScheduled() throws Exception {
        final var samplingFrequency = TimeValue.timeValueSeconds(2);
        final var storeFrequency = TimeValue.timeValueMinutes(1);
        final var settings = Settings.builder()
            .put(IndicesWriteLoadStatsService.SAMPLING_FREQUENCY_SETTING.getKey(), samplingFrequency)
            .put(IndicesWriteLoadStatsService.STORE_FREQUENCY_SETTING.getKey(), storeFrequency)
            .build();
        runTest(settings, testContext -> {
            final var threadPool = testContext.threadPool();
            final var statsCollector = testContext.statsCollector();
            final var writeLoadStore = testContext.writeLoadStore();
            final var scheduledTasks = threadPool.getScheduledTasks();
            assertThat(scheduledTasks, hasSize(2));

            {
                final var scheduledTask = threadPool.runNextTask();
                assertThat(scheduledTask.delay, is(equalTo(samplingFrequency)));
                // The task is rescheduled after executing
                assertThat(scheduledTasks, hasSize(2));
            }

            {
                final var scheduledTask = threadPool.runNextTask();
                assertThat(scheduledTask.delay, is(equalTo(storeFrequency)));
                assertThat(scheduledTasks, hasSize(2));
            }

            assertThat(statsCollector.collectCalls.get(), is(equalTo(1)));
            assertThat(statsCollector.getAndResetCalls.get(), is(equalTo(1)));
            assertThat(writeLoadStore.putAsyncCalls.get(), is(equalTo(1)));
        });
    }

    public void testSamplingAndStoreAreScheduledOnlyAfterRunning() throws Exception {
        runTest(testContext -> {
            final var threadPool = testContext.threadPool();
            final var scheduledTasks = threadPool.getScheduledTasks();
            assertThat(scheduledTasks, hasSize(2));

            final var task1 = scheduledTasks.remove();
            final var task2 = scheduledTasks.remove();

            assertThat(scheduledTasks, is(empty()));

            task1.run();
            assertThat(scheduledTasks, hasSize(1));

            task2.run();
            assertThat(scheduledTasks, hasSize(2));
        });
    }

    public void testTasksAreCancelledAfterDisablingTheService() throws Exception {
        final var settings = Settings.builder().put(IndicesWriteLoadStatsService.ENABLED_SETTING.getKey(), true).build();
        runTest(settings, testContext -> {
            final var threadPool = testContext.threadPool();
            final var scheduledTasks = threadPool.getScheduledTasks();

            final var clusterSettings = testContext.clusterSettings();
            final var updatedSettings = Settings.builder().put(IndicesWriteLoadStatsService.ENABLED_SETTING.getKey(), false).build();
            clusterSettings.applySettings(updatedSettings);

            assertThat(scheduledTasks, hasSize(2));
            var task1 = scheduledTasks.remove();
            assertThat(task1.isCancelled(), is(equalTo(true)));
            var task2 = scheduledTasks.remove();
            assertThat(task2.isCancelled(), is(equalTo(true)));
            assertThat(scheduledTasks, is(empty()));
        });
    }

    public void testStatsAreNotCollectedWhenServiceIsDisabled() throws Exception {
        final var settings = Settings.builder().put(IndicesWriteLoadStatsService.ENABLED_SETTING.getKey(), false).build();
        runTest(settings, testContext -> {
            final var threadPool = testContext.threadPool();
            final var scheduledTasks = threadPool.getScheduledTasks();

            assertThat(scheduledTasks, is(empty()));
        });
    }

    private void runTest(Consumer<TestContext> testBody) throws Exception {
        runTest(Settings.EMPTY, testBody);
    }

    private void runTest(Settings settings, Consumer<TestContext> testBody) throws Exception {
        SequentialSchedulerThreadPool testThreadPool = new SequentialSchedulerThreadPool();

        final var clusterSettings = new ClusterSettings(
            settings,
            Set.of(
                IndicesWriteLoadStatsService.ENABLED_SETTING,
                IndicesWriteLoadStatsService.SAMPLING_FREQUENCY_SETTING,
                IndicesWriteLoadStatsService.STORE_FREQUENCY_SETTING,
                IndicesWriteLoadStore.ENABLED_SETTING
            )
        );

        final var statsCollector = new InstrumentedStatsCollector();
        final var writeLoadStore = new InstrumentedWriteLoadStore(clusterSettings, settings);

        try (
            var indicesWriteLoadStatsService = new IndicesWriteLoadStatsService(
                statsCollector,
                writeLoadStore,
                testThreadPool,
                clusterSettings,
                settings
            )
        ) {
            indicesWriteLoadStatsService.start();
            testBody.accept(new TestContext(indicesWriteLoadStatsService, statsCollector, writeLoadStore, testThreadPool, clusterSettings));
        } finally {
            testThreadPool.shutdown();
            testThreadPool.awaitTermination(1, TimeUnit.SECONDS);
        }
    }

    static class InstrumentedWriteLoadStore extends IndicesWriteLoadStore {
        final AtomicInteger putAsyncCalls = new AtomicInteger();

        InstrumentedWriteLoadStore(ClusterSettings clusterSettings, Settings settings) {
            super(mock(BulkProcessor.class), clusterSettings, settings);
        }

        @Override
        public void putAsync(List<ShardWriteLoadHistogramSnapshot> shardWriteLoadHistogramSnapshots) {
            putAsyncCalls.incrementAndGet();
            if (randomBoolean()) {
                throw new RuntimeException("Failed storing write load");
            }
        }
    }

    static class InstrumentedStatsCollector extends IndicesWriteLoadStatsCollector {
        final AtomicInteger collectCalls = new AtomicInteger();
        final AtomicInteger getAndResetCalls = new AtomicInteger();

        InstrumentedStatsCollector() {
            super(mock(ClusterService.class), () -> 0L);
        }

        @Override
        public void collectWriteLoadStats() {
            collectCalls.incrementAndGet();
            if (randomBoolean()) {
                throw new RuntimeException("Failed collecting stats");
            }
        }

        @Override
        public List<ShardWriteLoadHistogramSnapshot> getWriteLoadHistogramSnapshotsAndReset() {
            getAndResetCalls.incrementAndGet();
            return Collections.emptyList();
        }

    }

    static class SequentialSchedulerThreadPool extends TestThreadPool {
        final Queue<ScheduledTask> scheduledTasks = new ArrayDeque<>();

        SequentialSchedulerThreadPool() {
            super(getTestClass().getName());
        }

        @Override
        public ScheduledCancellable schedule(Runnable command, TimeValue delay, String executor) {
            assertThat(executor, is(equalTo(WRITE_LOAD_COLLECTOR_THREAD_POOL_NAME)));
            var scheduledTask = new ScheduledTask(command, delay);
            scheduledTasks.add(scheduledTask);
            return scheduledTask;
        }

        ScheduledTask runNextTask() {
            var scheduledTask = scheduledTasks.remove();
            if (scheduledTask != null) {
                scheduledTask.run();
            }
            return scheduledTask;
        }

        Queue<ScheduledTask> getScheduledTasks() {
            return scheduledTasks;
        }
    }

    static class ScheduledTask implements Scheduler.ScheduledCancellable, Runnable {
        private final Runnable command;
        private final TimeValue delay;
        private final AtomicBoolean cancelled = new AtomicBoolean();

        ScheduledTask(Runnable command, TimeValue delay) {
            this.command = command;
            this.delay = delay;
        }

        @Override
        public void run() {
            assert cancelled.get() == false;
            command.run();
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return 0;
        }

        @Override
        public int compareTo(Delayed o) {
            return 0;
        }

        @Override
        public boolean cancel() {
            return cancelled.compareAndSet(false, true);
        }

        @Override
        public boolean isCancelled() {
            return cancelled.get();
        }
    }

    record TestContext(
        IndicesWriteLoadStatsService statsService,
        InstrumentedStatsCollector statsCollector,
        InstrumentedWriteLoadStore writeLoadStore,
        SequentialSchedulerThreadPool threadPool,
        ClusterSettings clusterSettings
    ) {}
}
