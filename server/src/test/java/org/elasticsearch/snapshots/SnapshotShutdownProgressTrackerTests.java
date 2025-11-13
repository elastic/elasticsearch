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
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
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

    private void setLogPeriodicProgressReportSeenEventExpectation(
        MockLog mockLog,
        int numberOfSnapshotsInProgress,
        int done,
        int failed,
        int aborted,
        int paused
    ) {
        mockLog.addExpectation(
            new MockLog.SeenEventExpectation(
                "log progress report",
                SnapshotShutdownProgressTracker.class.getCanonicalName(),
                Level.INFO,
                "*Current active shard snapshot stats on data node ["
                    + getLocalNodeIdSupplier.get()
                    + "]*"
                    + "Number shard snapshots running ["
                    + numberOfSnapshotsInProgress
                    + "]*"
                    + "Shard snapshot completion stats since shutdown began: Done ["
                    + done
                    + "]; Failed ["
                    + failed
                    + "]; Aborted ["
                    + aborted
                    + "]; Paused ["
                    + paused
                    + "]*"
            )
        );
    }

    private void setLogPeriodicProgressReportUnseenEventExpectation(MockLog mockLog, int done, int failed, int aborted, int paused) {
        mockLog.addExpectation(
            new MockLog.UnseenEventExpectation(
                "does not log progress report",
                SnapshotShutdownProgressTracker.class.getCanonicalName(),
                Level.INFO,
                "*Current active shard snapshot stats on data node ["
                    + getLocalNodeIdSupplier.get()
                    + "]*"
                    + "Shard snapshot completion stats since shutdown began: Done ["
                    + done
                    + "]; Failed ["
                    + failed
                    + "]; Aborted ["
                    + aborted
                    + "]; Paused ["
                    + paused
                    + "]*"
            )
        );
    }

    private void setInFlightSnapshots(SnapshotShutdownProgressTracker tracker) {
        for (int i = 0; i < 5; ++i) {
            tracker.incNumberOfShardSnapshotsInProgress(dummyShardId, dummySnapshot);
        }
    }

    private void ensurePeriodicLogMessagesAsSnapshotsComplete(SnapshotShutdownProgressTracker tracker, MockLog mockLog) {
        // We are going to simulate 5 in-progress snapshots while the node is shutting down, so we expect the periodic logger
        // to update the completion stats
        setLogPeriodicProgressReportSeenEventExpectation(mockLog, 4, 1, 0, 0, 0);
        setLogPeriodicProgressReportSeenEventExpectation(mockLog, 3, 1, 0, 1, 0);
        setLogPeriodicProgressReportSeenEventExpectation(mockLog, 2, 1, 1, 1, 0);
        setLogPeriodicProgressReportSeenEventExpectation(mockLog, 1, 1, 1, 1, 1);

        // When there are no more in-progress snapshots, the tracker emits a final log message, and terminates the periodic logger.
        setLogPeriodicProgressReportUnseenEventExpectation(mockLog, 2, 1, 1, 1);

        // We expect that the tracker sees no more snapshots in-progress and logs the final exit message.
        // This exit message should only be logged once, and then there should be no more subsequent logging
        mockLog.addExpectation(new MockLog.LoggingExpectation() {
            int count = 0;

            @Override
            public void match(LogEvent event) {
                if (event.getLevel() != Level.INFO) {
                    return;
                }
                if (event.getLoggerName().equals(SnapshotShutdownProgressTracker.class.getCanonicalName()) == false) {
                    return;
                }

                String msg = event.getMessage().getFormattedMessage();
                if (msg.matches(
                    ".*All shard snapshots have finished or been paused on data node \\["
                        + getLocalNodeIdSupplier.get()
                        + "].*Shard snapshot completion stats since shutdown began: Done \\[2]; Failed \\[1]; Aborted \\[1]; Paused \\[1].*"
                )) {
                    count++;
                }
            }

            @Override
            public void assertMatched() {
                assertEquals(1, count);
            }
        });

        // Finish each snapshot, expecting the periodic progress report to update
        for (int i = 0; i < 5; ++i) {
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
            // Advance time to the next periodic log message and expect it to include the new completion stats.
            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();
        }

        tracker.assertStatsForTesting(2, 1, 1, 1);
        mockLog.awaitAllExpectationsMatched();
    }

    public void testTrackerLogsStats() {
        final String dummyStatusMsg = "Dummy log message for index shard snapshot statuses";
        SnapshotShutdownProgressTracker tracker = new SnapshotShutdownProgressTracker(
            getLocalNodeIdSupplier,
            (callerLogger) -> callerLogger.info(dummyStatusMsg),
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

            // Adds in-progress snapshots so that the tracker doesn't immediately exit on seeing that all snapshots are finished,
            // and should begin logging the periodic progress report
            setInFlightSnapshots(tracker);

            // Simulate starting shutdown -- should reset the completion stats and start logging as there are snapshots in-flight
            tracker.onClusterStateAddShutdown();

            // Skip forward to the initial progress log message with no shard snapshot completions.
            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();
            mockLog.awaitAllExpectationsMatched();

            // In the tracker we use a callback to log the shard snapshot details. Here, we have mocked that with a dummy message,
            // but we expect this dummy message to still be logged with each periodic report.
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "index shard snapshot statuses",
                    SnapshotShutdownProgressTracker.class.getCanonicalName(),
                    Level.INFO,
                    dummyStatusMsg
                )
            );

            // A helper function to run the report periodically and assert the stats are as expected
            ensurePeriodicLogMessagesAsSnapshotsComplete(tracker, mockLog);
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
            (callerLogger) -> {},
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

            // Adds in-progress snapshots so that the tracker doesn't immediately exit on seeing that all snapshots are finished,
            // and should begin logging the periodic progress report
            tracker.incNumberOfShardSnapshotsInProgress(dummyShardId, dummySnapshot);

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
            (callerLogger) -> {},
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
                    "*Current active shard snapshot stats on data node*"
                )
            );

            // Adds in-progress snapshots so that the tracker doesn't immediately exit on seeing that all snapshots are finished,
            // and should begin logging the periodic progress report
            tracker.incNumberOfShardSnapshotsInProgress(dummyShardId, dummySnapshot);

            // Simulate starting shutdown -- progress logging should begin as there are in-flight snapshots
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
            (callerLogger) -> {},
            clusterSettings,
            testThreadPool
        );

        try (var mockLog = MockLog.capture(Coordinator.class, SnapshotShutdownProgressTracker.class)) {
            long timeInMillis = testThreadPool.relativeTimeInMillis();
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "pausing timestamp should be set",
                    SnapshotShutdownProgressTracker.class.getName(),
                    Level.INFO,
                    "*Finished signalling shard snapshots to pause at ["
                        + DateFormatter.forPattern("strict_date_optional_time").formatMillis(timeInMillis)
                        + " UTC]*"
                )
            );

            // Adds in-progress snapshots so that the tracker doesn't immediately exit on seeing that all snapshots are finished,
            // and should begin logging the periodic progress report
            tracker.incNumberOfShardSnapshotsInProgress(dummyShardId, dummySnapshot);

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
            (callerLogger) -> {},
            clusterSettings,
            testThreadPool
        );
        Snapshot snapshot = new Snapshot("repositoryName", new SnapshotId("snapshotName", "snapshotUUID"));
        ShardId shardId = new ShardId(new Index("indexName", "indexUUID"), 0);

        // Adds in-progress snapshots so that the tracker doesn't immediately exit on seeing that all snapshots are finished,
        // and should begin logging the periodic progress report
        tracker.incNumberOfShardSnapshotsInProgress(dummyShardId, dummySnapshot);

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
            (callerLogger) -> {},
            clusterSettings,
            testThreadPool
        );

        // Adds in-progress snapshots so that the tracker doesn't immediately exit on seeing that all snapshots are finished,
        // and should begin logging the periodic progress report
        setInFlightSnapshots(tracker);

        // 1. Simulate a node shutdown
        try (var mockLog = MockLog.capture(Coordinator.class, SnapshotShutdownProgressTracker.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "pausing timestamp should be unset",
                    SnapshotShutdownProgressTracker.class.getName(),
                    Level.INFO,
                    "*Finished signalling shard snapshots to pause at [-1 millis]*"
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

        // 2. Simulate 5 snapshots finishing, and assert whether the periodic logger works as expected
        try (var mockLog = MockLog.capture(Coordinator.class, SnapshotShutdownProgressTracker.class)) {
            ensurePeriodicLogMessagesAsSnapshotsComplete(tracker, mockLog);
        }

        // 3. Clear start and pause timestamps, and expect the periodic logger to reset
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
