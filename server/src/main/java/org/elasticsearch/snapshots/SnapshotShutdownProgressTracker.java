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
import org.elasticsearch.action.ResultDeduplicator;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.monitor.jvm.HotThreads;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks progress of shard snapshots during shutdown, on this single data node. Periodically reports progress via logging.
 */
public class SnapshotShutdownProgressTracker {
    /**
     * Runnable that logs shard snapshot progress.
     */
    private class ProgressLogger implements Runnable {
        @Override
        public void run() {
            logProgressReport();
        }
    }

    /** How frequently shard snapshot progress is logged after receiving local node shutdown metadata. */
    public static final Setting<TimeValue> SNAPSHOT_PROGRESS_DURING_SHUTDOWN_INTERVAL_TIME_SETTING = Setting.positiveTimeSetting(
        "snapshots.shutdown.progress.interval",
        TimeValue.timeValueSeconds(5),
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(SnapshotShutdownProgressTracker.class);

    private final ThreadPool threadPool;
    private final SnapshotShardsService snapshotShardsService;

    private final ProgressLogger progressLogger = new ProgressLogger();
    private final TimeValue progressLoggerInterval;
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
     */
    private final Set<String> shardSnapshotRequests = ConcurrentCollections.newConcurrentSet();

    /**
     * Track how the shard snapshots reach completion during shutdown: did they fail, succeed or pause?
     */
    private final AtomicLong doneCount = new AtomicLong();
    private final AtomicLong failureCount = new AtomicLong();
    private final AtomicLong abortedCount = new AtomicLong();
    private final AtomicLong pausedCount = new AtomicLong();

    public SnapshotShutdownProgressTracker(Settings settings, ThreadPool threadPool, SnapshotShardsService snapshotShardsService) {
        this.progressLoggerInterval = SNAPSHOT_PROGRESS_DURING_SHUTDOWN_INTERVAL_TIME_SETTING.get(settings);
        this.threadPool = threadPool;
        this.snapshotShardsService = snapshotShardsService;
    }

    protected void scheduleProgressLogger() {
        scheduledProgressLoggerFuture = threadPool.scheduleWithFixedDelay(
            progressLogger,
            progressLoggerInterval,
            threadPool.executor(ThreadPool.Names.GENERIC)
        );
    }

    protected void cancelProgressLogger() {
        assert scheduledProgressLoggerFuture != null : "Somehow shutdown mode was removed before it was added.";
        scheduledProgressLoggerFuture.cancel();
    }

    /**
     * Called as soon as a node shutdown signal is received.
     */
    public void onClusterStateAddShutdown() {
        assert this.shutdownStartMillis == -1 : "Expected not to be tracking anything, since we're only just entering shutdown mode";

        // Reset these values when a new shutdown occurs, to minimize/eliminate chances of racing if shutdown is later removed and async
        // shard snapshots updates continue to occur.
        doneCount.set(0);
        failureCount.set(0);
        abortedCount.set(0);
        pausedCount.set(0);

        // Track the timestamp of shutdown signal, on which to base periodic progress logging.
        this.shutdownStartMillis = threadPool.relativeTimeInMillis();
        scheduleProgressLogger();
    }

    /**
     * Called when the cluster state update processing a shutdown signal has finished signalling (setting PAUSING) all shard snapshots to
     * pause.
     */
    public void onClusterStatePausingSetForAllShardSnapshots() {
        this.shutdownFinishedSignallingPausingMillis = threadPool.relativeTimeInMillis();
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
    public void incNumberOfShardSnapshotsInProgress() {
        numberOfShardSnapshotsInProgressOnDataNode.incrementAndGet();
    }

    /**
     * Tracks how many shard snapshots have finished.
     */
    public void decNumberOfShardSnapshotsInProgress(IndexShardSnapshotStatus.Stage stage) {
        numberOfShardSnapshotsInProgressOnDataNode.decrementAndGet();
        if (shutdownStartMillis != -1) {
            switch (stage) {
                case DONE -> doneCount.incrementAndGet();
                case FAILURE -> failureCount.incrementAndGet();
                case ABORTED -> abortedCount.incrementAndGet();
                case PAUSED -> pausedCount.incrementAndGet();
                // The other stages are active, we should only see the end result because this method is called upon completion.
                default -> {
                    assert false : "unexpected shard snapshot stage: " + stage;
                }
            }
        }
    }

    /**
     * Uniquely tracks a request to update a shard snapshot status sent to the master node. Idempotent, safe to call multiple times.
     *
     * @param snapshot first part of a unique identifier
     * @param shardId second part of a unique identifier
     */
    public void trackRequestSentToMaster(Snapshot snapshot, ShardId shardId) {
        shardSnapshotRequests.add(snapshot.toString() + shardId.getIndexName() + shardId.getId());
    }

    /**
     * Stops tracking a request to update a shard snapshot status sent to the master node. Idempotent, safe to call multiple times.
     *
     * @param snapshot first part of a unique identifier
     * @param shardId second part of a unique identifier
     */
    public void releaseRequestSentToMaster(Snapshot snapshot, ShardId shardId) {
        shardSnapshotRequests.remove(snapshot.toString() + shardId.getIndexName() + shardId.getId());
    }

    /**
     * Logs some statistics about shard snapshot progress, as well as {@link HotThreads#logLocalHotThreads} to surface anything unexpected.
     */
    private void logProgressReport() {
        // A shard snapshot has two phases for tracking, for reporting purposes. The first is while the shard snapshot is running on the
        // data node. The second, to update the shard snapshot status in the cluster state on the master node, occurs asynchronously upon
        // shard snapshot completion.
        logger.info(
            """
                Current active shard snapshot stats. \
                Node shutdown cluster state update received at [{}}]. \
                Finished signalling shard snapshots to pause [{}]. \
                [Phase 1 of 2] Number shard snapshots running on the data node (have not yet noticed pause signal) [{}]. \
                [Phase 2 of 2] Number shard snapshots running according to the master node (awaiting response to status update \
                requests sent to master node) [{}]. \
                Shard snapshot completion stats during shutdown: Done [{}]; Failed [{}]; Aborted [{}]; Paused [{}] \
                """,
            shutdownStartMillis,
            shutdownFinishedSignallingPausingMillis,
            numberOfShardSnapshotsInProgressOnDataNode.get(),
            shardSnapshotRequests.size(),
            doneCount.get(),
            failureCount.get(),
            abortedCount.get(),
            pausedCount.get()
        );

        HotThreads.logLocalHotThreads(logger, Level.INFO, "slow snapshot pausing", ReferenceDocs.STATELESS_EMPTY);
    }
}
