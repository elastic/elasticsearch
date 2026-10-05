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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.monitor.jvm.HotThreads;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.core.Strings.format;

/**
 * Monitors search tasks for slow execution and logs hot threads when thresholds are exceeded.
 * <p>
 * On data nodes, logs hot threads when a shard-level search operation (query/fetch phase) exceeds
 * the data node threshold. On coordinator nodes, logs hot threads when the reduce/merge phase
 * exceeds the coordinator threshold (only after all shards have responded).
 * <p>
 * This helps diagnose slow searches by capturing what threads are doing while the search is
 * still running, rather than just logging after completion.
 */
public class SearchTaskWatchdog extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(SearchTaskWatchdog.class);

    public static final Setting<Boolean> ENABLED = Setting.boolSetting(
        "search.task_watchdog.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> COORDINATOR_THRESHOLD = Setting.timeSetting(
        "search.task_watchdog.coordinator_threshold",
        TimeValue.timeValueSeconds(3),
        TimeValue.MINUS_ONE,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> DATA_NODE_THRESHOLD = Setting.timeSetting(
        "search.task_watchdog.data_node_threshold",
        TimeValue.timeValueSeconds(3),
        TimeValue.MINUS_ONE,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> INTERVAL = Setting.timeSetting(
        "search.task_watchdog.interval",
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueMillis(100),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> COOLDOWN_PERIOD = Setting.timeSetting(
        "search.task_watchdog.cooldown_period",
        TimeValue.timeValueSeconds(30),
        TimeValue.ZERO,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /// Sampling interval of the hot threads dump. The dump sleeps this long before taking any stack snapshot, so a short value gets
    /// stacks closer to the moment of detection at the cost of a meaningless CPU percentage column.
    public static final Setting<TimeValue> HOT_THREADS_INTERVAL = Setting.timeSetting(
        "search.task_watchdog.hot_threads.interval",
        TimeValue.timeValueMillis(10),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Integer> HOT_THREADS_SNAPSHOTS = Setting.intSetting(
        "search.task_watchdog.hot_threads.snapshots",
        10,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /// Delay between consecutive stack snapshots of the hot threads dump. Together with [#HOT_THREADS_SNAPSHOTS] this defines how
    /// much of the slow task's execution one dump spans.
    public static final Setting<TimeValue> HOT_THREADS_SNAPSHOT_DELAY = Setting.timeSetting(
        "search.task_watchdog.hot_threads.snapshot_delay",
        TimeValue.timeValueMillis(30),
        TimeValue.ZERO,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final int MAX_DESCRIPTION_LENGTH = 256;

    private final TaskManager taskManager;
    private final ThreadPool threadPool;

    private volatile boolean enabled;
    private volatile long coordinatorThresholdNanos;
    private volatile long dataNodeThresholdNanos;
    private volatile long minThresholdNanos;
    private volatile TimeValue interval;
    private volatile long cooldownPeriodNanos;
    private volatile TimeValue hotThreadsInterval;
    private volatile int hotThreadsSnapshots;
    private volatile TimeValue hotThreadsSnapshotDelay;
    private volatile long lastLoggedNanos = 0;
    private final AtomicBoolean scheduled = new AtomicBoolean(false);

    public SearchTaskWatchdog(ClusterSettings clusterSettings, TaskManager taskManager, ThreadPool threadPool) {
        this.taskManager = taskManager;
        this.threadPool = threadPool;

        clusterSettings.initializeAndWatch(INTERVAL, v -> this.interval = v);
        clusterSettings.initializeAndWatch(COORDINATOR_THRESHOLD, v -> setCoordinatorThreshold(v.nanos()));
        clusterSettings.initializeAndWatch(DATA_NODE_THRESHOLD, v -> setDataNodeThreshold(v.nanos()));
        clusterSettings.initializeAndWatch(COOLDOWN_PERIOD, v -> this.cooldownPeriodNanos = v.nanos());
        clusterSettings.initializeAndWatch(HOT_THREADS_INTERVAL, v -> this.hotThreadsInterval = v);
        clusterSettings.initializeAndWatch(HOT_THREADS_SNAPSHOTS, v -> this.hotThreadsSnapshots = v);
        clusterSettings.initializeAndWatch(HOT_THREADS_SNAPSHOT_DELAY, v -> this.hotThreadsSnapshotDelay = v);
        clusterSettings.initializeAndWatch(ENABLED, this::setEnabled);
    }

    private void setEnabled(boolean enabled) {
        this.enabled = enabled;
        if (enabled) {
            logger.info("enabling SearchTaskWatchdog");
            scheduleNext();
        }
    }

    private void setCoordinatorThreshold(long newCoordinatorThresholdValue) {
        this.coordinatorThresholdNanos = newCoordinatorThresholdValue;
        this.minThresholdNanos = computeMinThreshold(newCoordinatorThresholdValue, dataNodeThresholdNanos);
    }

    private void setDataNodeThreshold(long newDataNodeThresholdValue) {
        this.dataNodeThresholdNanos = newDataNodeThresholdValue;
        this.minThresholdNanos = computeMinThreshold(coordinatorThresholdNanos, newDataNodeThresholdValue);
    }

    private static long computeMinThreshold(long coordinatorNanos, long dataNodeNanos) {
        long coordValue = coordinatorNanos > 0 ? coordinatorNanos : Long.MAX_VALUE;
        long dataValue = dataNodeNanos > 0 ? dataNodeNanos : Long.MAX_VALUE;
        return Math.min(coordValue, dataValue);
    }

    @Override
    protected void doStart() {
        if (enabled) {
            scheduleNext();
        }
    }

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() {}

    private void scheduleNext() {
        if (enabled && lifecycle.stoppedOrClosed() == false && scheduled.compareAndSet(false, true)) {
            threadPool.scheduleUnlessShuttingDown(interval, threadPool.generic(), this::run);
        }
    }

    private void run() {
        try {
            if (enabled == false || lifecycle.stoppedOrClosed()) {
                return;
            }

            final long now = threadPool.relativeTimeInNanos();

            boolean isInCooldownPeriod = lastLoggedNanos > 0 && (now - lastLoggedNanos) < cooldownPeriodNanos;
            if (isInCooldownPeriod) {
                return;
            }

            if (minThresholdNanos < Long.MAX_VALUE) {
                taskManager.forEachCancellableTask(minThresholdNanos, info -> {
                    try {
                        handleTask(info, now);
                    } catch (Exception e) {
                        logger.debug(() -> "error processing task [" + info.task().getId() + "]", e);
                    }
                    // we logged a slow task in this iteration, so skip checking other tasks
                    // as we're now in the cooldown period
                    return lastLoggedNanos != now;
                });
            }
        } finally {
            scheduled.set(false);
            scheduleNext();
        }
    }

    private void handleTask(TaskManager.CancellableTaskInfo info, long now) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);
        if (info.task() instanceof SearchShardTask) {
            if (dataNodeThresholdNanos > 0 && info.elapsedNanos() > dataNodeThresholdNanos) {
                logSlowTask(info, "shard", now);
            }
        } else if (info.task() instanceof SearchTask) {
            if (coordinatorThresholdNanos > 0
                && info.elapsedNanos() > coordinatorThresholdNanos
                && info.hasOutstandingChildren() == false) {
                logSlowTask(info, "coordinator", now);
            }
        }
    }

    private void logSlowTask(TaskManager.CancellableTaskInfo info, String type, long now) {
        lastLoggedNanos = now;

        long taskId = info.task().getId();
        String description = Strings.cleanTruncate(info.task().getDescription(), MAX_DESCRIPTION_LENGTH);
        HotThreads.logLocalHotThreads(
            logger,
            Level.INFO,
            format("slow search %s task [%d] parent [%s] description [%s]", type, taskId, info.task().getParentTaskId(), description),
            ReferenceDocs.SEARCH_TASK_WATCHDOG,
            new HotThreads().busiestThreads(500)
                .ignoreIdleThreads(false)
                .interval(hotThreadsInterval)
                .threadElementsSnapshotCount(hotThreadsSnapshots)
                .threadElementsSnapshotDelay(hotThreadsSnapshotDelay)
        );
    }
}
