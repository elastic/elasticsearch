/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class SnapshotShutdownProgressTrackerTests extends ESTestCase {
    private static final Logger logger = LogManager.getLogger(SnapshotShutdownProgressTrackerTests.class);

    final Settings settings = Settings.builder()
        .put(
            SnapshotShutdownProgressTracker.SNAPSHOT_PROGRESS_DURING_SHUTDOWN_LOG_INTERVAL_SETTING.getKey(),
            TimeValue.timeValueMillis(500)
        )
        .build();
    final Settings disabledTrackerLoggingSettings = Settings.builder()
        .put(SnapshotShutdownProgressTracker.SNAPSHOT_PROGRESS_DURING_SHUTDOWN_LOG_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
        .build();
    ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

    DeterministicTaskQueue deterministicTaskQueue;

    // Construction parameters for the Tracker.
    ThreadPool testThreadPool;
    private final Supplier<String> getLocalNodeIdSupplier = () -> "local-node-id-for-test";
    private final BiConsumer<Setting<TimeValue>, Consumer<TimeValue>> addSettingsUpdateConsumerNoOp = (setting, updateMethod) -> {};

    // Set up some dummy shard snapshot information to feed the Tracker.
    private final ShardId dummyShardId = new ShardId(new Index("index-name-for-test", "index-uuid-for-test"), 0);
    private final Snapshot dummySnapshot = new Snapshot(
        "snapshot-repo-name-for-test",
        new SnapshotId("snapshot-name-for-test", "snapshot-uuid-for-test")
    );
    Function<IndexShardSnapshotStatus.Stage, IndexShardSnapshotStatus> dummyShardSnapshotStatusSupplier = (stage) -> {
        var shardGen = new ShardGeneration("shard-gen-string-for-test");
        IndexShardSnapshotStatus newStatus = IndexShardSnapshotStatus.newInitializing(new ShardGeneration("shard-gen-string-for-test"));
        switch (stage) {
            case DONE -> {
                newStatus.moveToStarted(0L, 1, 10, 2L, 20L);
                newStatus.moveToFinalize();
                newStatus.moveToDone(10L, new ShardSnapshotResult(shardGen, ByteSizeValue.MINUS_ONE, 2));
            }
            case ABORTED -> newStatus.abortIfNotCompleted("snapshot-aborted-for-test", (listener) -> {});
            case FAILURE -> newStatus.moveToFailed(300, "shard-snapshot-failure-string for-test");
            case PAUSED -> {
                newStatus.pauseIfNotCompleted((listener) -> {});
                newStatus.moveToUnsuccessful(IndexShardSnapshotStatus.Stage.PAUSED, "shard-paused-string-for-test", 100L);
            }
            default -> newStatus.pauseIfNotCompleted((listener) -> {});
        }
        return newStatus;
    };

    @Before
    public void setUpThreadPool() {
        deterministicTaskQueue = new DeterministicTaskQueue();
        testThreadPool = deterministicTaskQueue.getThreadPool();
    }

    /**
     * Increments the tracker's shard snapshot completion stats. Evenly adds to each type of {@link IndexShardSnapshotStatus.Stage} stat
     * supported by the tracker.
     */
    void simulateShardSnapshotsCompleting(SnapshotShutdownProgressTracker tracker, int numShardSnapshots) {
        for (int i = 0; i < numShardSnapshots; ++i) {
            tracker.incNumberOfShardSnapshotsInProgress(dummyShardId, dummySnapshot);
            IndexShardSnapshotStatus status;
            switch (i % 4) {
                case 0 -> status = dummyShardSnapshotStatusSupplier.apply(IndexShardSnapshotStatus.Stage.DONE);
                case 1 -> status = dummyShardSnapshotStatusSupplier.apply(IndexShardSnapshotStatus.Stage.ABORTED);
                case 2 -> status = dummyShardSnapshotStatusSupplier.apply(IndexShardSnapshotStatus.Stage.FAILURE);
                case 3 -> status = dummyShardSnapshotStatusSupplier.apply(IndexShardSnapshotStatus.Stage.PAUSED);
                // decNumberOfShardSnapshotsInProgress will throw an assertion if this value is ever set.
                default -> status = dummyShardSnapshotStatusSupplier.apply(IndexShardSnapshotStatus.Stage.PAUSING);
            }
            logger.info("---> Generated shard snapshot status in stage (" + status.getStage() + ") for switch case (" + (i % 4) + ")");
            tracker.decNumberOfShardSnapshotsInProgress(dummyShardId, dummySnapshot, status);
        }
    }

    public void testTrackerLogsStats() {
        SnapshotShutdownProgressTracker tracker = new SnapshotShutdownProgressTracker(
            getLocalNodeIdSupplier,
            clusterSettings,
            testThreadPool
        );

        try (var mockLog = MockLog.capture(Coordinator.class, SnapshotShutdownProgressTracker.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "unset shard snapshot completion stats",
                    SnapshotShutdownProgressTracker.class.getCanonicalName(),
                    Level.INFO,
                    "*snapshots to pause [-1]*Done [0]; Failed [0]; Aborted [0]; Paused [0]*"
                )
            );

            // Simulate starting shutdown -- should reset the completion stats and start logging
            tracker.onClusterStateAddShutdown();

            // Wait for the initial progress log message with no shard snapshot completions.
            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();
            mockLog.awaitAllExpectationsMatched();
        }

        try (var mockLog = MockLog.capture(Coordinator.class, SnapshotShutdownProgressTracker.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "shard snapshot completed stats",
                    SnapshotShutdownProgressTracker.class.getCanonicalName(),
                    Level.INFO,
                    "*Shard snapshot completion stats since shutdown began: Done [2]; Failed [1]; Aborted [1]; Paused [1]*"
                )
            );

            // Simulate updating the shard snapshot completion stats.
            simulateShardSnapshotsCompleting(tracker, 5);
            tracker.assertStatsForTesting(2, 1, 1, 1);

            // Wait for the next periodic log message to include the new completion stats.
            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();
            mockLog.awaitAllExpectationsMatched();
        }
    }

    /**
     * Test that {@link SnapshotShutdownProgressTracker#SNAPSHOT_PROGRESS_DURING_SHUTDOWN_LOG_INTERVAL_SETTING} can be disabled by setting
     * a value of {@link TimeValue#MINUS_ONE}. This will disable progress logging, though the Tracker will continue to track things.
     */
    @TestLogging(
        value = "org.elasticsearch.snapshots.SnapshotShutdownProgressTracker:DEBUG",
        reason = "Test checks for DEBUG-level log message"
    )
    public void testTrackerProgressLoggingIntervalSettingCanBeDisabled() {
        ClusterSettings clusterSettingsDisabledLogging = new ClusterSettings(
            disabledTrackerLoggingSettings,
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS
        );
        SnapshotShutdownProgressTracker tracker = new SnapshotShutdownProgressTracker(
            getLocalNodeIdSupplier,
            clusterSettingsDisabledLogging,
            testThreadPool
        );

        try (var mockLog = MockLog.capture(Coordinator.class, SnapshotShutdownProgressTracker.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "disabled logging message",
                    SnapshotShutdownProgressTracker.class.getName(),
                    Level.DEBUG,
                    "Snapshot progress logging during shutdown is disabled"
                )
            );
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "no progress logging message",
                    SnapshotShutdownProgressTracker.class.getName(),
                    Level.INFO,
                    "Current active shard snapshot stats on data node*"
                )
            );

            // Simulate starting shutdown -- no logging will start because the Tracker logging is disabled.
            tracker.onClusterStateAddShutdown();
            tracker.onClusterStatePausingSetForAllShardSnapshots();

            // Wait for the logging disabled message.
            deterministicTaskQueue.runAllTasks();
            mockLog.awaitAllExpectationsMatched();
        }
    }

    @TestLogging(
        value = "org.elasticsearch.snapshots.SnapshotShutdownProgressTracker:DEBUG",
        reason = "Test checks for DEBUG-level log message"
    )
    public void testTrackerIntervalSettingDynamically() {
        ClusterSettings clusterSettingsDisabledLogging = new ClusterSettings(
            disabledTrackerLoggingSettings,
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS
        );
        SnapshotShutdownProgressTracker tracker = new SnapshotShutdownProgressTracker(
            getLocalNodeIdSupplier,
            clusterSettingsDisabledLogging,
            testThreadPool
        );
        // Re-enable the progress logging
        clusterSettingsDisabledLogging.applySettings(settings);

        // Check that the logging is active.
        try (var mockLog = MockLog.capture(Coordinator.class, SnapshotShutdownProgressTracker.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "disabled logging message",
                    SnapshotShutdownProgressTracker.class.getName(),
                    Level.DEBUG,
                    "Snapshot progress logging during shutdown is disabled"
                )
            );
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "progress logging message",
                    SnapshotShutdownProgressTracker.class.getName(),
                    Level.INFO,
                    "Current active shard snapshot stats on data node*"
                )
            );

            // Simulate starting shutdown -- progress logging should begin.
            tracker.onClusterStateAddShutdown();
            tracker.onClusterStatePausingSetForAllShardSnapshots();

            // Wait for the progress logging message
            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();
            mockLog.awaitAllExpectationsMatched();
        }
    }

    public void testTrackerPauseTimestamp() {
        SnapshotShutdownProgressTracker tracker = new SnapshotShutdownProgressTracker(
            getLocalNodeIdSupplier,
            clusterSettings,
            testThreadPool
        );

        try (var mockLog = MockLog.capture(Coordinator.class, SnapshotShutdownProgressTracker.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "pausing timestamp should be set",
                    SnapshotShutdownProgressTracker.class.getName(),
                    Level.INFO,
                    "*Finished signalling shard snapshots to pause at [" + testThreadPool.relativeTimeInMillis() + "]*"
                )
            );

            // Simulate starting shutdown -- start logging.
            tracker.onClusterStateAddShutdown();

            // Set a pausing complete timestamp.
            tracker.onClusterStatePausingSetForAllShardSnapshots();

            // Wait for the first log message to ensure the pausing timestamp was set.
            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();
            mockLog.awaitAllExpectationsMatched();
        }
    }

    public void testTrackerRequestsToMaster() {
        SnapshotShutdownProgressTracker tracker = new SnapshotShutdownProgressTracker(
            getLocalNodeIdSupplier,
            clusterSettings,
            testThreadPool
        );
        Snapshot snapshot = new Snapshot("repositoryName", new SnapshotId("snapshotName", "snapshotUUID"));
        ShardId shardId = new ShardId(new Index("indexName", "indexUUID"), 0);

        // Simulate starting shutdown -- start logging.
        tracker.onClusterStateAddShutdown();

        // Set a pausing complete timestamp.
        tracker.onClusterStatePausingSetForAllShardSnapshots();

        try (var mockLog = MockLog.capture(Coordinator.class, SnapshotShutdownProgressTracker.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "one master status update request",
                    SnapshotShutdownProgressTracker.class.getName(),
                    Level.INFO,
                    "*master node reply to status update request [1]*"
                )
            );

            tracker.trackRequestSentToMaster(snapshot, shardId);

            // Wait for the first log message to ensure the pausing timestamp was set.
            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();
            mockLog.awaitAllExpectationsMatched();
        }

        try (var mockLog = MockLog.capture(Coordinator.class, SnapshotShutdownProgressTracker.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "no master status update requests",
                    SnapshotShutdownProgressTracker.class.getName(),
                    Level.INFO,
                    "*master node reply to status update request [0]*"
                )
            );

            tracker.releaseRequestSentToMaster(snapshot, shardId);

            // Wait for the first log message to ensure the pausing timestamp was set.
            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();
            mockLog.awaitAllExpectationsMatched();
        }
    }

    public void testTrackerClearShutdown() {
        SnapshotShutdownProgressTracker tracker = new SnapshotShutdownProgressTracker(
            getLocalNodeIdSupplier,
            clusterSettings,
            testThreadPool
        );

        try (var mockLog = MockLog.capture(Coordinator.class, SnapshotShutdownProgressTracker.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "pausing timestamp should be unset",
                    SnapshotShutdownProgressTracker.class.getName(),
                    Level.INFO,
                    "*Finished signalling shard snapshots to pause at [-1]*"
                )
            );

            // Simulate starting shutdown -- start logging.
            tracker.onClusterStateAddShutdown();

            // Set a pausing complete timestamp.
            tracker.onClusterStatePausingSetForAllShardSnapshots();

            // Wait for the first log message to ensure the pausing timestamp was set.
            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();
            mockLog.awaitAllExpectationsMatched();
        }

        try (var mockLog = MockLog.capture(Coordinator.class, SnapshotShutdownProgressTracker.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "logging completed shard snapshot stats",
                    SnapshotShutdownProgressTracker.class.getName(),
                    Level.INFO,
                    "*Done [2]; Failed [2]; Aborted [2]; Paused [1]*"
                )
            );

            // Simulate updating the shard snapshot completion stats.
            simulateShardSnapshotsCompleting(tracker, 7);
            tracker.assertStatsForTesting(2, 2, 2, 1);

            // Wait for the first log message to ensure the pausing timestamp was set.
            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();
            mockLog.awaitAllExpectationsMatched();
        }

        // Clear start and pause timestamps
        tracker.onClusterStateRemoveShutdown();

        try (var mockLog = MockLog.capture(Coordinator.class, SnapshotShutdownProgressTracker.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "completed shard snapshot stats are reset",
                    SnapshotShutdownProgressTracker.class.getName(),
                    Level.INFO,
                    "*Done [0]; Failed [0]; Aborted [0]; Paused [0]"
                )
            );

            // Start logging again and check that the pause timestamp was reset from the last time.
            tracker.onClusterStateAddShutdown();

            // Wait for the first log message to ensure the pausing timestamp was set.
            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();
            mockLog.awaitAllExpectationsMatched();
        }

    }
}
