/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ResultDeduplicator;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Tracks progress of shard snapshots during shutdown, on this single data node. Periodically reports progress via logging, the interval for
 * which see {@link #SNAPSHOT_PROGRESS_DURING_SHUTDOWN_LOG_INTERVAL_SETTING}.
 */
public class SnapshotShutdownProgressTracker {

    /** How frequently shard snapshot progress is logged after receiving local node shutdown metadata. */
    public static final Setting<TimeValue> SNAPSHOT_PROGRESS_DURING_SHUTDOWN_LOG_INTERVAL_SETTING = Setting.timeSetting(
        "snapshots.shutdown.progress.interval",
        TimeValue.timeValueSeconds(5),
        TimeValue.MINUS_ONE,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final Logger logger = LogManager.getLogger(SnapshotShutdownProgressTracker.class);

    private final Supplier<String> getLocalNodeId;
    private final Consumer<Logger> logIndexShardSnapshotStatuses;
    private final ThreadPool threadPool;

    private volatile TimeValue progressLoggerInterval;
    private Scheduler.Cancellable scheduledProgressLoggerFuture;

    /**
     * The time at which the cluster state update began that found a shutdown signal for this node. Negative value means unset (node is not
     * shutting down).
     */
    private volatile long shutdownStartMillis = -1;

    /**
     * The time at which the cluster state finished setting shard snapshot states to PAUSING, which the shard snapshot operations will
     * discover asynchronously. Negative value means unset (node is not shutting down)
     */
    private volatile long shutdownFinishedSignallingPausingMillis = -1;

    /**
     * Tracks the number of shard snapshots that have started on the data node but not yet finished.
     */
    private final AtomicLong numberOfShardSnapshotsInProgressOnDataNode = new AtomicLong();

    /**
     * The logic to track shard snapshot status update requests to master can result in duplicate requests (see
     * {@link ResultDeduplicator}), as well as resending requests if the elected master changes.
     * Tracking specific requests uniquely by snapshot ID + shard ID de-duplicates requests for tracking.
     * Also tracks the absolute start time of registration, to report duration on de-registration.
     */
    private final Map<String, Long> shardSnapshotRequests = ConcurrentCollections.newConcurrentMap();

    /**
     * Track how the shard snapshots reach completion during shutdown: did they fail, succeed or pause?
     */
    private final AtomicLong doneCount = new AtomicLong();
    private final AtomicLong failureCount = new AtomicLong();
    private final AtomicLong abortedCount = new AtomicLong();
    private final AtomicLong pausedCount = new AtomicLong();

    public SnapshotShutdownProgressTracker(
        Supplier<String> localNodeIdSupplier,
        Consumer<Logger> logShardStatuses,
        ClusterSettings clusterSettings,
        ThreadPool threadPool
    ) {
        this.getLocalNodeId = localNodeIdSupplier;
        this.logIndexShardSnapshotStatuses = logShardStatuses;
        clusterSettings.initializeAndWatch(
            SNAPSHOT_PROGRESS_DURING_SHUTDOWN_LOG_INTERVAL_SETTING,
            value -> this.progressLoggerInterval = value
        );
        this.threadPool = threadPool;
    }

    private void scheduleProgressLogger() {
        if (progressLoggerInterval.millis() > 0) {
            scheduledProgressLoggerFuture = threadPool.scheduleWithFixedDelay(
                this::logProgressReport,
                progressLoggerInterval,
                threadPool.executor(ThreadPool.Names.GENERIC)
            );
            logger.debug(
                () -> Strings.format(
                    "Starting shutdown snapshot progress logging on node [%s], runs every [%s]",
                    getLocalNodeId.get(),
                    progressLoggerInterval
                )
            );
        } else {
            logger.debug("Snapshot progress logging during shutdown is disabled");
        }
    }

    private void cancelProgressLogger() {
        assert scheduledProgressLoggerFuture != null : "Somehow shutdown mode was removed before it was added.";
        scheduledProgressLoggerFuture.cancel();
        if (progressLoggerInterval.millis() > 0) {
            // Only log cancellation if it was most likely started. Theoretically the interval setting could be updated during shutdown,
            // such that the progress logger is already running and ignores the new value, but that does not currently happen.
            logger.debug(() -> Strings.format("Cancelling shutdown snapshot progress logging on node [%s]", getLocalNodeId.get()));
        }
    }

    /**
     * Logs information about shard snapshot progress.
     */
    private void logProgressReport() {
        logger.info(
            """
                Current active shard snapshot stats on data node [{}]. \
                Node shutdown cluster state update received at [{} millis]. \
                Finished signalling shard snapshots to pause at [{} millis]. \
                Number shard snapshots running [{}]. \
                Number shard snapshots waiting for master node reply to status update request [{}] \
                Shard snapshot completion stats since shutdown began: Done [{}]; Failed [{}]; Aborted [{}]; Paused [{}]\
                """,
            getLocalNodeId.get(),
            shutdownStartMillis,
            shutdownFinishedSignallingPausingMillis,
            numberOfShardSnapshotsInProgressOnDataNode.get(),
            shardSnapshotRequests.size(),
            doneCount.get(),
            failureCount.get(),
            abortedCount.get(),
            pausedCount.get()
        );
        // Use a callback to log the shard snapshot details.
        logIndexShardSnapshotStatuses.accept(logger);
    }

    /**
     * Called as soon as a node shutdown signal is received.
     */
    public void onClusterStateAddShutdown() {
        assert this.shutdownStartMillis == -1 : "Expected not to be tracking anything. Call shutdown remove before adding shutdown again";

        // Reset these values when a new shutdown occurs, to minimize/eliminate chances of racing if shutdown is later removed and async
        // shard snapshots updates continue to occur.
        doneCount.set(0);
        failureCount.set(0);
        abortedCount.set(0);
        pausedCount.set(0);

        // Track the timestamp of shutdown signal, on which to base periodic progress logging.
        this.shutdownStartMillis = threadPool.relativeTimeInMillis();

        // Start logging periodic progress reports.
        scheduleProgressLogger();
    }

    /**
     * Called when the cluster state update processing a shutdown signal has finished signalling (setting PAUSING) all shard snapshots to
     * pause.
     */
    public void onClusterStatePausingSetForAllShardSnapshots() {
        assert this.shutdownStartMillis != -1
            : "Should not have left shutdown mode before finishing processing the cluster state update with shutdown";
        this.shutdownFinishedSignallingPausingMillis = threadPool.relativeTimeInMillis();
        logger.debug(() -> Strings.format("Pause signals have been set for all shard snapshots on data node [%s]", getLocalNodeId.get()));
    }

    /**
     * The cluster state indicating that a node is to be shutdown may be cleared instead of following through with node shutdown. In that
     * case, no further shutdown shard snapshot progress reporting is desired.
     */
    public void onClusterStateRemoveShutdown() {
        assert shutdownStartMillis != -1 : "Expected a call to add shutdown mode before a call to remove shutdown mode.";

        // Reset the shutdown specific trackers.
        this.shutdownStartMillis = -1;
        this.shutdownFinishedSignallingPausingMillis = -1;

        // Turn off the progress logger, which we only want to run during shutdown.
        cancelProgressLogger();
    }

    /**
     * Tracks how many shard snapshots are started.
     */
    public void incNumberOfShardSnapshotsInProgress(ShardId shardId, Snapshot snapshot) {
        logger.debug(() -> Strings.format("Started shard (shard ID: [%s]) in snapshot ([%s])", shardId, snapshot));
        numberOfShardSnapshotsInProgressOnDataNode.incrementAndGet();
    }

    /**
     * Tracks how many shard snapshots have finished since shutdown mode began.
     */
    public void decNumberOfShardSnapshotsInProgress(ShardId shardId, Snapshot snapshot, IndexShardSnapshotStatus shardSnapshotStatus) {
        logger.debug(
            () -> Strings.format(
                "Finished shard (shard ID: [%s]) in snapshot ([%s]) with status ([%s]): ",
                shardId,
                snapshot,
                shardSnapshotStatus.toString()
            )
        );

        numberOfShardSnapshotsInProgressOnDataNode.decrementAndGet();
        if (shutdownStartMillis != -1) {
            switch (shardSnapshotStatus.getStage()) {
                case DONE -> doneCount.incrementAndGet();
                case FAILURE -> failureCount.incrementAndGet();
                case ABORTED -> abortedCount.incrementAndGet();
                case PAUSED -> pausedCount.incrementAndGet();
                // The other stages are active, we should only see the end result because this method is called upon completion.
                default -> {
                    assert false : "unexpected shard snapshot stage transition during shutdown: " + shardSnapshotStatus.getStage();
                }
            }
        }
    }

    /**
     * Uniquely tracks a request to update a shard snapshot status sent to the master node. Idempotent, safe to call multiple times.
     *
     * @param snapshot first part of a unique tracking identifier
     * @param shardId second part of a unique tracking identifier
     */
    public void trackRequestSentToMaster(Snapshot snapshot, ShardId shardId) {
        logger.debug(() -> Strings.format("Tracking shard (shard ID: [%s]) snapshot ([%s]) request to master", shardId, snapshot));
        shardSnapshotRequests.put(snapshot.toString() + shardId.getIndexName() + shardId.getId(), threadPool.relativeTimeInNanos());
    }

    /**
     * Stops tracking a request to update a shard snapshot status sent to the master node. Idempotent, safe to call multiple times.
     *
     * @param snapshot first part of a unique tracking identifier
     * @param shardId second part of a unique tracking identifier
     */
    public void releaseRequestSentToMaster(Snapshot snapshot, ShardId shardId) {
        var masterRequestStartTime = shardSnapshotRequests.remove(snapshot.toString() + shardId.getIndexName() + shardId.getId());
        // This method is may be called multiple times. Only log if this is the first time, and the entry hasn't already been removed.
        if (masterRequestStartTime != null) {
            logger.debug(
                () -> Strings.format(
                    "Finished shard (shard ID: [%s]) snapshot ([%s]) update request to master in [%s]",
                    shardId,
                    snapshot,
                    new TimeValue(threadPool.relativeTimeInNanos() - masterRequestStartTime.longValue(), TimeUnit.NANOSECONDS)
                )
            );
        }
    }

    // Test only
    void assertStatsForTesting(long done, long failure, long aborted, long paused) {
        assert doneCount.get() == done : "doneCount is " + doneCount.get() + ", expected count was " + done;
        assert failureCount.get() == failure : "failureCount is " + doneCount.get() + ", expected count was " + failure;
        assert abortedCount.get() == aborted : "abortedCount is " + doneCount.get() + ", expected count was " + aborted;
        assert pausedCount.get() == paused : "pausedCount is " + doneCount.get() + ", expected count was " + paused;
    }
}
