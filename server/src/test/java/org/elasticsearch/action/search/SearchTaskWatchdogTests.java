/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.Level;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static org.elasticsearch.action.search.SearchTaskWatchdog.COOLDOWN_PERIOD;
import static org.elasticsearch.action.search.SearchTaskWatchdog.COORDINATOR_THRESHOLD;
import static org.elasticsearch.action.search.SearchTaskWatchdog.DATA_NODE_THRESHOLD;
import static org.elasticsearch.action.search.SearchTaskWatchdog.ENABLED;
import static org.elasticsearch.action.search.SearchTaskWatchdog.INTERVAL;
import static org.hamcrest.Matchers.is;

public class SearchTaskWatchdogTests extends ESTestCase {

    private static final String WATCHDOG_LOGGER = "org.elasticsearch.action.search.SearchTaskWatchdog";

    public void testCoordinatorOnlyLogsWhenNoOutstandingChildren() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();

        final var hasOutstandingChildren = new AtomicBoolean(true);
        final long thresholdMillis = 3000;
        final long intervalMillis = 1000;

        final var settings = Settings.builder()
            .put(ENABLED.getKey(), true)
            .put(COORDINATOR_THRESHOLD.getKey(), thresholdMillis + "ms")
            .put(DATA_NODE_THRESHOLD.getKey(), "-1")
            .put(INTERVAL.getKey(), intervalMillis + "ms")
            .put(COOLDOWN_PERIOD.getKey(), "0s")
            .build();

        final var clusterSettings = new ClusterSettings(
            settings,
            Set.of(ENABLED, COORDINATOR_THRESHOLD, DATA_NODE_THRESHOLD, INTERVAL, COOLDOWN_PERIOD)
        );

        final var mockTaskManager = new TaskManager(Settings.EMPTY, deterministicTaskQueue.getThreadPool(), Set.of()) {
            @Override
            public void forEachCancellableTask(long minElapsedNanos, Predicate<CancellableTaskInfo> processor) {
                var mockTask = new SearchTask(
                    1L,
                    "transport",
                    "indices:data/read/search",
                    () -> "test search",
                    TaskId.EMPTY_TASK_ID,
                    Map.of()
                );
                // Report task with hasOutstandingChildren based on test state
                processor.test(new CancellableTaskInfo(mockTask, thresholdMillis * 1_000_000L + 1, hasOutstandingChildren.get()));
            }
        };

        final var watchdog = new SearchTaskWatchdog(clusterSettings, mockTaskManager, deterministicTaskQueue.getThreadPool());
        watchdog.start();

        // coordinator has outstanding children, no logging expected
        deterministicTaskQueue.advanceTime();
        MockLog.assertThatLogger(
            deterministicTaskQueue::runAllRunnableTasks,
            SearchTaskWatchdog.class,
            new MockLog.UnseenEventExpectation("no logging when waiting for shards", WATCHDOG_LOGGER, Level.INFO, "*Slow*")
        );

        deterministicTaskQueue.advanceTime();
        MockLog.assertThatLogger(
            deterministicTaskQueue::runAllRunnableTasks,
            SearchTaskWatchdog.class,
            new MockLog.UnseenEventExpectation("still no logging when waiting for shards", WATCHDOG_LOGGER, Level.INFO, "*Slow*")
        );

        // all shards responded, coordinator is doing reduce work
        hasOutstandingChildren.set(false);

        deterministicTaskQueue.advanceTime();
        MockLog.assertThatLogger(
            deterministicTaskQueue::runAllRunnableTasks,
            SearchTaskWatchdog.class,
            new MockLog.SeenEventExpectation("coordinator logs during reduce", WATCHDOG_LOGGER, Level.INFO, "slow search coordinator task*")
        );

        watchdog.stop();
        deterministicTaskQueue.runAllTasksInTimeOrder();
    }

    public void testDataNodeTaskLogsWhenExceedsThreshold() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();

        final long thresholdMillis = 3000;
        final long intervalMillis = 1000;

        final var settings = Settings.builder()
            .put(ENABLED.getKey(), true)
            .put(COORDINATOR_THRESHOLD.getKey(), "-1")
            .put(DATA_NODE_THRESHOLD.getKey(), thresholdMillis + "ms")
            .put(INTERVAL.getKey(), intervalMillis + "ms")
            .put(COOLDOWN_PERIOD.getKey(), "0s")
            .build();

        final var clusterSettings = new ClusterSettings(
            settings,
            Set.of(ENABLED, COORDINATOR_THRESHOLD, DATA_NODE_THRESHOLD, INTERVAL, COOLDOWN_PERIOD)
        );

        final var taskExceedsThreshold = new AtomicBoolean(false);

        final var mockTaskManager = new TaskManager(Settings.EMPTY, deterministicTaskQueue.getThreadPool(), Set.of()) {
            @Override
            public void forEachCancellableTask(long minElapsedNanos, Predicate<CancellableTaskInfo> processor) {
                if (taskExceedsThreshold.get()) {
                    var mockTask = new SearchShardTask(
                        2L,
                        "transport",
                        "indices:data/read/search[phase/query]",
                        "test shard task",
                        TaskId.EMPTY_TASK_ID,
                        Map.of()
                    );
                    processor.test(new CancellableTaskInfo(mockTask, thresholdMillis * 1_000_000L + 1, false));
                }
            }
        };

        final var watchdog = new SearchTaskWatchdog(clusterSettings, mockTaskManager, deterministicTaskQueue.getThreadPool());
        watchdog.start();

        // task not exceeding threshold yet
        deterministicTaskQueue.advanceTime();
        MockLog.assertThatLogger(
            deterministicTaskQueue::runAllRunnableTasks,
            SearchTaskWatchdog.class,
            new MockLog.UnseenEventExpectation("no logging yet", WATCHDOG_LOGGER, Level.INFO, "*Slow*")
        );

        // task now exceeds threshold
        taskExceedsThreshold.set(true);

        deterministicTaskQueue.advanceTime();
        MockLog.assertThatLogger(
            deterministicTaskQueue::runAllRunnableTasks,
            SearchTaskWatchdog.class,
            new MockLog.SeenEventExpectation("shard task logs", WATCHDOG_LOGGER, Level.INFO, "slow search shard task*")
        );

        watchdog.stop();
        deterministicTaskQueue.runAllTasksInTimeOrder();
    }

    public void testDisabledWatchdogDoesNotSchedule() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();

        final var settings = Settings.builder()
            .put(ENABLED.getKey(), false)
            .put(DATA_NODE_THRESHOLD.getKey(), "3s")
            .put(COORDINATOR_THRESHOLD.getKey(), "3s")
            .put(INTERVAL.getKey(), "1s")
            .build();

        final var clusterSettings = new ClusterSettings(
            settings,
            Set.of(ENABLED, COORDINATOR_THRESHOLD, DATA_NODE_THRESHOLD, INTERVAL, COOLDOWN_PERIOD)
        );

        final var taskManagerCalled = new AtomicBoolean(false);
        final var mockTaskManager = new TaskManager(Settings.EMPTY, deterministicTaskQueue.getThreadPool(), Set.of()) {
            @Override
            public void forEachCancellableTask(long minElapsedNanos, Predicate<CancellableTaskInfo> processor) {
                taskManagerCalled.set(true);
            }
        };

        final var watchdog = new SearchTaskWatchdog(clusterSettings, mockTaskManager, deterministicTaskQueue.getThreadPool());
        watchdog.start();

        assertThat(deterministicTaskQueue.hasDeferredTasks(), is(false));
        assertThat(taskManagerCalled.get(), is(false));

        watchdog.stop();
    }
}
