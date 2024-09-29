/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.snapshots;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
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
            TimeValue.timeValueMillis(200)
        )
        .build();

    // Construction parameters for the Tracker.
    TestThreadPool testThreadPool;
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
        ;
        return newStatus;
    };

    @Before
    public void setUpThreadPool() {
        testThreadPool = new TestThreadPool("thread-pool-for-test");
    }

    @After
    public void shutdownThreadPool() {
        boolean successfulTermination = terminate(testThreadPool);
        assert successfulTermination;
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

    public void testTrackerLogsStats() throws Exception {
        SnapshotShutdownProgressTracker tracker = new SnapshotShutdownProgressTracker(
            getLocalNodeIdSupplier,
            addSettingsUpdateConsumerNoOp,
            settings,
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
            assertBusy(mockLog::assertAllExpectationsMatched);
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
            assertBusy(mockLog::assertAllExpectationsMatched);
        }
    }

    public void testTrackerPauseTimestamp() throws Exception {
        SnapshotShutdownProgressTracker tracker = new SnapshotShutdownProgressTracker(
            getLocalNodeIdSupplier,
            addSettingsUpdateConsumerNoOp,
            settings,
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
            assertBusy(mockLog::assertAllExpectationsMatched);
        }
    }

    public void testTrackerRequestsToMaster() throws Exception {
        SnapshotShutdownProgressTracker tracker = new SnapshotShutdownProgressTracker(
            getLocalNodeIdSupplier,
            addSettingsUpdateConsumerNoOp,
            settings,
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
            assertBusy(mockLog::assertAllExpectationsMatched);
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
            assertBusy(mockLog::assertAllExpectationsMatched);
        }
    }

    public void testTrackerClearShutdown() throws Exception {
        SnapshotShutdownProgressTracker tracker = new SnapshotShutdownProgressTracker(
            getLocalNodeIdSupplier,
            addSettingsUpdateConsumerNoOp,
            settings,
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
            assertBusy(mockLog::assertAllExpectationsMatched);
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
            assertBusy(mockLog::assertAllExpectationsMatched);
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
            assertBusy(mockLog::assertAllExpectationsMatched);
        }

    }
}
