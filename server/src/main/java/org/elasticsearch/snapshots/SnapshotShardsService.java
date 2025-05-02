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
import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ResultDeduplicator;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardState;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus.Stage;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryShardId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.repositories.SnapshotIndexCommit;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.core.Strings.format;

/**
 * This service runs on data nodes and controls currently running shard snapshots on these nodes. It is responsible for
 * starting and stopping shard level snapshots.
 * See package level documentation of {@link org.elasticsearch.snapshots} for details.
 * See {@link SnapshotsService} for the master node snapshotting steps.
 */
public final class SnapshotShardsService extends AbstractLifecycleComponent implements ClusterStateListener, IndexEventListener {
    private static final Logger logger = LogManager.getLogger(SnapshotShardsService.class);

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final RepositoriesService repositoriesService;

    private final TransportService transportService;

    private final ThreadPool threadPool;

    private final SnapshotShutdownProgressTracker snapshotShutdownProgressTracker;

    /** Maps each snapshot to its collection of individual shard snapshots */
    private final Map<Snapshot, Map<ShardId, IndexShardSnapshotStatus>> shardSnapshots = new HashMap<>();

    // A map of snapshots to the shardIds that we already reported to the master as failed
    private final ResultDeduplicator<UpdateIndexShardSnapshotStatusRequest, Void> remoteFailedRequestDeduplicator;

    // Runs the tasks that start each shard snapshot (e.g. acquiring the index commit)
    private final ThrottledTaskRunner startShardSnapshotTaskRunner;

    // Runs the tasks that promptly notify shards of aborted snapshots so that resources can be released ASAP
    private final ThrottledTaskRunner notifyOnAbortTaskRunner;

    public SnapshotShardsService(
        Settings settings,
        ClusterService clusterService,
        RepositoriesService repositoriesService,
        TransportService transportService,
        IndicesService indicesService
    ) {
        this.indicesService = indicesService;
        this.repositoriesService = repositoriesService;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = transportService.getThreadPool();
        this.snapshotShutdownProgressTracker = new SnapshotShutdownProgressTracker(
            () -> clusterService.state().nodes().getLocalNodeId(),
            (callerLogger) -> logIndexShardSnapshotStatuses(callerLogger),
            clusterService.getClusterSettings(),
            threadPool
        );
        this.remoteFailedRequestDeduplicator = new ResultDeduplicator<>(threadPool.getThreadContext());
        if (DiscoveryNode.canContainData(settings)) {
            // this is only useful on the nodes that can hold data
            clusterService.addListener(this);
        }

        // Abort notification may release the last store ref, closing the shard, so we do them in the background on a generic thread.
        this.notifyOnAbortTaskRunner = new ThrottledTaskRunner(
            "notify-on-abort",
            threadPool.info(ThreadPool.Names.SNAPSHOT).getMax(),
            threadPool.generic()
        );
        this.startShardSnapshotTaskRunner = new ThrottledTaskRunner(
            "start-shard-snapshots",
            threadPool.info(ThreadPool.Names.SNAPSHOT).getMax(),
            threadPool.executor(ThreadPool.Names.SNAPSHOT)
        );
    }

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() {
        clusterService.removeListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        try {
            final var localNodeId = clusterService.localNode().getId();

            // Track when this node enters and leaves shutdown mode because we pause shard snapshots for shutdown.
            // The snapshotShutdownProgressTracker will report (via logging) on the progress shard snapshots make
            // towards either completing (successfully or otherwise) or pausing.
            NodesShutdownMetadata currentShutdownMetadata = event.state().metadata().custom(NodesShutdownMetadata.TYPE);
            NodesShutdownMetadata previousShutdownMetadata = event.previousState().metadata().custom(NodesShutdownMetadata.TYPE);
            SingleNodeShutdownMetadata currentLocalNodeShutdownMetadata = currentShutdownMetadata != null
                ? currentShutdownMetadata.get(localNodeId)
                : null;
            SingleNodeShutdownMetadata previousLocalNodeShutdownMetadata = previousShutdownMetadata != null
                ? previousShutdownMetadata.get(localNodeId)
                : null;

            boolean isLocalNodeAddingShutdown = false;
            if (isPausingProgressTrackedShutdown(previousLocalNodeShutdownMetadata) == false
                && isPausingProgressTrackedShutdown(currentLocalNodeShutdownMetadata)) {
                snapshotShutdownProgressTracker.onClusterStateAddShutdown();
                isLocalNodeAddingShutdown = true;
            } else if (isPausingProgressTrackedShutdown(previousLocalNodeShutdownMetadata)
                && isPausingProgressTrackedShutdown(currentLocalNodeShutdownMetadata) == false) {
                    snapshotShutdownProgressTracker.onClusterStateRemoveShutdown();
                }

            final var currentSnapshots = SnapshotsInProgress.get(event.state());

            if (SnapshotsInProgress.get(event.previousState()).equals(currentSnapshots) == false) {
                synchronized (shardSnapshots) {
                    // Cancel any snapshots that have been removed from the cluster state.
                    cancelRemoved(currentSnapshots);

                    // Update running snapshots or start any snapshots that are set to run.
                    for (final var oneRepoSnapshotsInProgress : currentSnapshots.entriesByRepo()) {
                        for (final var snapshotsInProgressEntry : oneRepoSnapshotsInProgress) {
                            handleUpdatedSnapshotsInProgressEntry(
                                localNodeId,
                                currentSnapshots.isNodeIdForRemoval(localNodeId),
                                snapshotsInProgressEntry
                            );
                        }
                    }
                }
            }

            if (isLocalNodeAddingShutdown) {
                // Any active snapshots would have been signalled to pause in the previous code block.
                snapshotShutdownProgressTracker.onClusterStatePausingSetForAllShardSnapshots();
            }

            String previousMasterNodeId = event.previousState().nodes().getMasterNodeId();
            String currentMasterNodeId = event.state().nodes().getMasterNodeId();
            if (currentMasterNodeId != null && currentMasterNodeId.equals(previousMasterNodeId) == false) {
                // Clear request deduplicator since we need to send all requests that were potentially not handled by the previous
                // master again
                remoteFailedRequestDeduplicator.clear();
                for (List<SnapshotsInProgress.Entry> snapshots : currentSnapshots.entriesByRepo()) {
                    syncShardStatsOnNewMaster(snapshots);
                }
            }

        } catch (Exception e) {
            assert false : new AssertionError(e);
            logger.warn("failed to update snapshot state", e);
        }
    }

    /**
     * Determines whether we want to track this kind of shutdown for snapshot pausing progress.
     * We want tracking is shutdown metadata is set, and not type RESTART.
     * Note that the Shutdown API is idempotent and the type of shutdown may change to / from RESTART to / from some other type of interest.
     *
     * @return true if snapshots will be paused during this type of local node shutdown.
     */
    private static boolean isPausingProgressTrackedShutdown(@Nullable SingleNodeShutdownMetadata localNodeShutdownMetadata) {
        return localNodeShutdownMetadata != null && localNodeShutdownMetadata.getType() != SingleNodeShutdownMetadata.Type.RESTART;
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        // abort any snapshots occurring on the soon-to-be closed shard
        synchronized (shardSnapshots) {
            for (Map.Entry<Snapshot, Map<ShardId, IndexShardSnapshotStatus>> snapshotShards : shardSnapshots.entrySet()) {
                final var indexShardSnapshotStatus = snapshotShards.getValue().get(shardId);
                if (indexShardSnapshotStatus != null) {
                    logger.debug(
                        "[{}] shard closing, abort snapshotting for snapshot [{}]",
                        shardId,
                        snapshotShards.getKey().getSnapshotId()
                    );
                    indexShardSnapshotStatus.abortIfNotCompleted("shard is closing, aborting", notifyOnAbortTaskRunner::enqueueTask);
                }
            }
        }
    }

    private void logIndexShardSnapshotStatuses(Logger callerLogger) {
        for (var snapshot : shardSnapshots.entrySet()) {  // iterate the snapshots
            for (var shardSnapshot : snapshot.getValue().entrySet()) {  // iterate the individual shard snapshots
                callerLogger.info(
                    Strings.format(
                        "SnapshotId %s, ShardId %s, shard snapshot status: %s",
                        snapshot.getKey().getSnapshotId(),
                        shardSnapshot.getKey(),
                        shardSnapshot.getValue()
                    )
                );
            }
        }
    }

    /**
     * Returns status of shards that are snapshotted on the node and belong to the given snapshot
     * <p>
     * This method is executed on data node
     * </p>
     *
     * @param snapshot  snapshot
     * @return map of shard id to snapshot status
     */
    public Map<ShardId, IndexShardSnapshotStatus.Copy> currentSnapshotShards(Snapshot snapshot) {
        synchronized (shardSnapshots) {
            final var current = shardSnapshots.get(snapshot);
            if (current == null) {
                return null;
            }

            final Map<ShardId, IndexShardSnapshotStatus.Copy> result = Maps.newMapWithExpectedSize(current.size());
            for (final var entry : current.entrySet()) {
                result.put(entry.getKey(), entry.getValue().asCopy());
            }
            return result;
        }
    }

    /**
     * Cancels any snapshots that have been removed from the given list of SnapshotsInProgress.
     */
    private void cancelRemoved(SnapshotsInProgress snapshotsInProgress) {
        // First, remove snapshots that are no longer there
        Iterator<Map.Entry<Snapshot, Map<ShardId, IndexShardSnapshotStatus>>> it = shardSnapshots.entrySet().iterator();
        while (it.hasNext()) {
            final Map.Entry<Snapshot, Map<ShardId, IndexShardSnapshotStatus>> entry = it.next();
            final Snapshot snapshot = entry.getKey();
            if (snapshotsInProgress.snapshot(snapshot) == null) {
                // abort any running snapshots of shards for the removed entry;
                // this could happen if for some reason the cluster state update for aborting
                // running shards is missed, then the snapshot is removed is a subsequent cluster
                // state update, which is being processed here
                it.remove();
                for (IndexShardSnapshotStatus snapshotStatus : entry.getValue().values()) {
                    snapshotStatus.abortIfNotCompleted(
                        "snapshot has been removed in cluster state, aborting",
                        notifyOnAbortTaskRunner::enqueueTask
                    );
                }
            }
        }
    }

    /**
     * Starts new snapshots and pauses or aborts active shard snapshot based on the updated {@link SnapshotsInProgress} entry.
     */
    private void handleUpdatedSnapshotsInProgressEntry(String localNodeId, boolean removingLocalNode, SnapshotsInProgress.Entry entry) {
        if (entry.isClone()) {
            // This is a snapshot clone, it will be executed on the current master
            return;
        }

        switch (entry.state()) {
            case STARTED -> {
                if (entry.hasShardsInInitState() == false) {
                    // Snapshot is running but has no running shards yet, nothing to do
                    return;
                }

                if (removingLocalNode) {
                    pauseShardSnapshotsForNodeRemoval(localNodeId, entry);
                } else {
                    startNewShardSnapshots(localNodeId, entry);
                }
            }
            case ABORTED -> {
                // Abort all running shards for this snapshot
                final Snapshot snapshot = entry.snapshot();
                Map<ShardId, IndexShardSnapshotStatus> snapshotShards = shardSnapshots.getOrDefault(snapshot, emptyMap());
                for (Map.Entry<RepositoryShardId, ShardSnapshotStatus> shard : entry.shardSnapshotStatusByRepoShardId().entrySet()) {
                    final ShardId sid = entry.shardId(shard.getKey());
                    final IndexShardSnapshotStatus snapshotStatus = snapshotShards.get(sid);
                    if (snapshotStatus == null) {
                        // due to CS batching we might have missed the INIT state and straight went into ABORTED
                        // notify master that abort has completed by moving to FAILED
                        if (shard.getValue().state() == ShardState.ABORTED && localNodeId.equals(shard.getValue().nodeId())) {
                            notifyUnsuccessfulSnapshotShard(
                                snapshot,
                                sid,
                                ShardState.FAILED,
                                shard.getValue().reason(),
                                shard.getValue().generation(),
                                // Shard snapshot never began, so there is no status object to update.
                                (outcomeInfoString) -> {}
                            );
                        }
                    } else {
                        snapshotStatus.abortIfNotCompleted("snapshot has been aborted", notifyOnAbortTaskRunner::enqueueTask);
                    }
                }
            }
            // otherwise snapshot is not running, nothing to do
        }
    }

    private void startNewShardSnapshots(String localNodeId, SnapshotsInProgress.Entry entry) {
        Map<ShardId, ShardGeneration> shardsToStart = null;
        final Snapshot snapshot = entry.snapshot();
        final var runningShardsForSnapshot = shardSnapshots.getOrDefault(snapshot, emptyMap());
        for (var scheduledShard : entry.shards().entrySet()) {
            // Add all new shards to start processing on
            final var shardId = scheduledShard.getKey();
            final var shardSnapshotStatus = scheduledShard.getValue();
            if (shardSnapshotStatus.state() == ShardState.INIT && localNodeId.equals(shardSnapshotStatus.nodeId())) {
                final var runningShard = runningShardsForSnapshot.get(shardId);
                if (runningShard == null || runningShard.isPaused()) {
                    logger.trace("[{}] adding [{}] shard to the queue", shardId, runningShard == null ? "new" : "paused");
                    if (shardsToStart == null) {
                        shardsToStart = new HashMap<>();
                    }
                    shardsToStart.put(shardId, shardSnapshotStatus.generation());
                }
            }
        }
        if (shardsToStart == null) {
            return;
        }
        assert shardsToStart.isEmpty() == false;

        final var newSnapshotShards = shardSnapshots.computeIfAbsent(snapshot, s -> new HashMap<>());

        for (final Map.Entry<ShardId, ShardGeneration> shardEntry : shardsToStart.entrySet()) {
            final ShardId shardId = shardEntry.getKey();
            final IndexShardSnapshotStatus snapshotStatus = IndexShardSnapshotStatus.newInitializing(shardEntry.getValue());
            newSnapshotShards.put(shardId, snapshotStatus);
            final IndexId indexId = entry.indices().get(shardId.getIndexName());
            assert indexId != null;
            assert SnapshotsService.useShardGenerations(entry.version())
                || ShardGenerations.fixShardGeneration(snapshotStatus.generation()) == null
                : "Found non-null, non-numeric shard generation ["
                    + snapshotStatus.generation()
                    + "] for snapshot with old-format compatibility";
            final var shardSnapshotTask = newShardSnapshotTask(
                shardId,
                snapshot,
                indexId,
                snapshotStatus,
                entry.version(),
                entry.startTime()
            );
            startShardSnapshotTaskRunner.enqueueTask(new ActionListener<>() {
                @Override
                public void onResponse(Releasable releasable) {
                    try (releasable) {
                        shardSnapshotTask.run();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    final var wrapperException = new IllegalStateException(
                        "impossible failure starting shard snapshot for " + shardId + " in " + snapshot,
                        e
                    );
                    logger.error(wrapperException.getMessage(), wrapperException);
                    assert false : wrapperException; // impossible
                }
            });
            snapshotStatus.updateStatusDescription("shard snapshot enqueued to start");
        }

        // apply some backpressure by reserving one SNAPSHOT thread for the startup work
        startShardSnapshotTaskRunner.runSyncTasksEagerly(threadPool.executor(ThreadPool.Names.SNAPSHOT));
    }

    /**
     * Iterates all the shard snapshots for the given snapshot entry. Any shard snapshots assigned to this data node will be: immediately
     * paused, if not already running; or flagged for pausing, if running.
     */
    private void pauseShardSnapshotsForNodeRemoval(String localNodeId, SnapshotsInProgress.Entry masterEntryCopy) {
        final var localShardSnapshots = shardSnapshots.getOrDefault(masterEntryCopy.snapshot(), Map.of());

        for (final Map.Entry<ShardId, ShardSnapshotStatus> shardEntry : masterEntryCopy.shards().entrySet()) {
            final ShardId shardId = shardEntry.getKey();
            final ShardSnapshotStatus masterShardSnapshotStatusCopy = shardEntry.getValue();

            if (masterShardSnapshotStatusCopy.state() != ShardState.INIT) {
                // shard snapshot not currently scheduled by master
                continue;
            }

            if (localNodeId.equals(masterShardSnapshotStatusCopy.nodeId()) == false) {
                // shard snapshot scheduled on a different node
                continue;
            }

            final var localShardSnapshotStatus = localShardSnapshots.get(shardId);
            if (localShardSnapshotStatus == null) {
                // shard snapshot scheduled but not currently running, pause immediately without starting
                notifyUnsuccessfulSnapshotShard(
                    masterEntryCopy.snapshot(),
                    shardId,
                    ShardState.PAUSED_FOR_NODE_REMOVAL,
                    "paused",
                    masterShardSnapshotStatusCopy.generation(),
                    // Shard snapshot never began, so there is no status object to update
                    (outcomeInfoString) -> {}
                );
            } else {
                // shard snapshot currently running, mark for pause
                localShardSnapshotStatus.pauseIfNotCompleted(notifyOnAbortTaskRunner::enqueueTask);
            }
        }
    }

    private Runnable newShardSnapshotTask(
        final ShardId shardId,
        final Snapshot snapshot,
        final IndexId indexId,
        final IndexShardSnapshotStatus snapshotStatus,
        final IndexVersion entryVersion,
        final long entryStartTime
    ) {
        Consumer<String> postMasterNotificationAction = (outcomeInfoString) -> {
            snapshotStatus.updateStatusDescription("Data node shard snapshot finished. Remote master update outcome: " + outcomeInfoString);
        };

        // Listener that runs on completion of the shard snapshot: it will notify the master node of success or failure.
        ActionListener<ShardSnapshotResult> snapshotResultListener = new ActionListener<>() {
            @Override
            public void onResponse(ShardSnapshotResult shardSnapshotResult) {
                snapshotStatus.updateStatusDescription("snapshot succeeded: proceeding to notify master of success");
                final ShardGeneration newGeneration = shardSnapshotResult.getGeneration();
                assert newGeneration != null;
                assert newGeneration.equals(snapshotStatus.generation());
                if (logger.isTraceEnabled()) {
                    final IndexShardSnapshotStatus.Copy lastSnapshotStatus = snapshotStatus.asCopy();
                    logger.trace(
                        "[{}][{}] completed snapshot to [{}] with status [{}] at generation [{}]",
                        shardId,
                        snapshot,
                        snapshot.getRepository(),
                        lastSnapshotStatus,
                        snapshotStatus.generation()
                    );
                }

                notifySuccessfulSnapshotShard(snapshot, shardId, shardSnapshotResult, postMasterNotificationAction);
            }

            @Override
            public void onFailure(Exception e) {
                snapshotStatus.updateStatusDescription("failed with exception '" + e + ": proceeding to notify master of failure");
                final String failure;
                final Stage nextStage;
                if (e instanceof AbortedSnapshotException) {
                    nextStage = Stage.FAILURE;
                    failure = "aborted";
                    logger.debug(() -> format("[%s][%s] aborted shard snapshot", shardId, snapshot), e);
                } else if (e instanceof PausedSnapshotException) {
                    nextStage = Stage.PAUSED;
                    failure = "paused for removal of node holding primary";
                    logger.debug(() -> format("[%s][%s] pausing shard snapshot", shardId, snapshot), e);
                } else {
                    nextStage = Stage.FAILURE;
                    failure = summarizeFailure(e);
                    logger.warn(() -> format("[%s][%s] failed to snapshot shard", shardId, snapshot), e);
                }
                final var shardState = snapshotStatus.moveToUnsuccessful(nextStage, failure, threadPool.absoluteTimeInMillis());
                notifyUnsuccessfulSnapshotShard(
                    snapshot,
                    shardId,
                    shardState,
                    failure,
                    snapshotStatus.generation(),
                    postMasterNotificationAction
                );
            }
        };

        snapshotShutdownProgressTracker.incNumberOfShardSnapshotsInProgress(shardId, snapshot);
        var decTrackerRunsBeforeResultListener = ActionListener.runAfter(snapshotResultListener, () -> {
            snapshotShutdownProgressTracker.decNumberOfShardSnapshotsInProgress(shardId, snapshot, snapshotStatus);
        });

        // separate method to make sure this lambda doesn't capture any heavy local objects like a SnapshotsInProgress.Entry
        return () -> snapshot(shardId, snapshot, indexId, snapshotStatus, entryVersion, entryStartTime, decTrackerRunsBeforeResultListener);
    }

    // package private for testing
    static String summarizeFailure(Throwable t) {
        if (t.getCause() == null) {
            return t.getClass().getSimpleName() + "[" + t.getMessage() + "]";
        } else {
            StringBuilder sb = new StringBuilder();
            while (t != null) {
                sb.append(t.getClass().getSimpleName());
                if (t.getMessage() != null) {
                    sb.append("[");
                    sb.append(t.getMessage());
                    sb.append("]");
                }
                t = t.getCause();
                if (t != null) {
                    sb.append("; nested: ");
                }
            }
            return sb.toString();
        }
    }

    /**
     * Creates shard snapshot
     *
     * @param snapshot       snapshot
     * @param snapshotStatus snapshot status
     */
    private void snapshot(
        final ShardId shardId,
        final Snapshot snapshot,
        final IndexId indexId,
        final IndexShardSnapshotStatus snapshotStatus,
        IndexVersion version,
        final long entryStartTime,
        ActionListener<ShardSnapshotResult> resultListener
    ) {
        ActionListener.run(resultListener, listener -> {
            snapshotStatus.updateStatusDescription("has started");
            snapshotStatus.ensureNotAborted();
            final IndexShard indexShard = indicesService.indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
            if (indexShard.routingEntry().primary() == false) {
                throw new IndexShardSnapshotFailedException(shardId, "snapshot should be performed only on primary");
            }
            if (indexShard.routingEntry().relocating()) {
                // do not snapshot when in the process of relocation of primaries so we won't get conflicts
                throw new IndexShardSnapshotFailedException(shardId, "cannot snapshot while relocating");
            }

            final IndexShardState indexShardState = indexShard.state();
            if (indexShardState == IndexShardState.CREATED || indexShardState == IndexShardState.RECOVERING) {
                // shard has just been created, or still recovering
                throw new IndexShardSnapshotFailedException(shardId, "shard didn't fully recover yet");
            }

            final Repository repository = repositoriesService.repository(snapshot.getRepository());
            SnapshotIndexCommit snapshotIndexCommit = null;
            try {
                snapshotStatus.updateStatusDescription("acquiring commit reference from IndexShard: triggers a shard flush");
                snapshotIndexCommit = new SnapshotIndexCommit(indexShard.acquireIndexCommitForSnapshot());
                snapshotStatus.updateStatusDescription("commit reference acquired, proceeding with snapshot");
                final var shardStateId = getShardStateId(indexShard, snapshotIndexCommit.indexCommit()); // not aborted so indexCommit() ok
                snapshotStatus.addAbortListener(makeAbortListener(indexShard.shardId(), snapshot, snapshotIndexCommit));
                snapshotStatus.ensureNotAborted();
                repository.snapshotShard(
                    new SnapshotShardContext(
                        indexShard.store(),
                        indexShard.mapperService(),
                        snapshot.getSnapshotId(),
                        indexId,
                        snapshotIndexCommit,
                        shardStateId,
                        snapshotStatus,
                        version,
                        entryStartTime,
                        listener
                    )
                );
                snapshotIndexCommit = null; // success
            } finally {
                if (snapshotIndexCommit != null) {
                    snapshotIndexCommit.closingBefore(new ActionListener<Void>() {
                        @Override
                        public void onResponse(Void unused) {}

                        @Override
                        public void onFailure(Exception e) {
                            // we're already failing exceptionally, and prefer to propagate the original exception instead of this one
                            logger.warn(Strings.format("exception closing commit for [%s] in [%s]", indexShard.shardId(), snapshot), e);
                        }
                    }).onResponse(null);
                }
            }
        });
    }

    private static ActionListener<IndexShardSnapshotStatus.AbortStatus> makeAbortListener(
        ShardId shardId,
        Snapshot snapshot,
        SnapshotIndexCommit snapshotIndexCommit
    ) {
        return new ActionListener<>() {
            @Override
            public void onResponse(IndexShardSnapshotStatus.AbortStatus abortStatus) {
                if (abortStatus == IndexShardSnapshotStatus.AbortStatus.ABORTED) {
                    assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC, ThreadPool.Names.SNAPSHOT);
                    snapshotIndexCommit.onAbort();
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(() -> Strings.format("unexpected failure in %s", description()), e);
                assert false : e;
            }

            @Override
            public String toString() {
                return description();
            }

            private String description() {
                return Strings.format("abort listener for [%s] in [%s]", shardId, snapshot);
            }
        };
    }

    /**
     * Generates an identifier from the current state of a shard that can be used to detect whether a shard's contents
     * have changed between two snapshots.
     * A shard is assumed to have unchanged contents if its global- and local checkpoint are equal, its maximum
     * sequence number has not changed and its history- and force-merge-uuid have not changed.
     * The method returns {@code null} if global and local checkpoint are different for a shard since no safe unique
     * shard state id can be used in this case because of the possibility of a primary failover leading to different
     * shard content for the same sequence number on a subsequent snapshot.
     *
     * @param indexShard          Shard
     * @param snapshotIndexCommit IndexCommit for shard
     * @return shard state id or {@code null} if none can be used
     */
    @Nullable
    public static String getShardStateId(IndexShard indexShard, IndexCommit snapshotIndexCommit) throws IOException {
        final Map<String, String> userCommitData = snapshotIndexCommit.getUserData();
        final SequenceNumbers.CommitInfo seqNumInfo = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(userCommitData.entrySet());
        final long maxSeqNo = seqNumInfo.maxSeqNo();
        if (maxSeqNo != seqNumInfo.localCheckpoint() || maxSeqNo != indexShard.getLastSyncedGlobalCheckpoint()) {
            return null;
        }
        return userCommitData.get(Engine.HISTORY_UUID_KEY)
            + "-"
            + userCommitData.getOrDefault(Engine.FORCE_MERGE_UUID_KEY, "na")
            + "-"
            + maxSeqNo;
    }

    /**
     * Checks if any shards were processed that the new master doesn't know about
     */
    private void syncShardStatsOnNewMaster(List<SnapshotsInProgress.Entry> entries) {
        for (SnapshotsInProgress.Entry snapshot : entries) {
            if (snapshot.state() == SnapshotsInProgress.State.STARTED || snapshot.state() == SnapshotsInProgress.State.ABORTED) {
                final Map<ShardId, IndexShardSnapshotStatus> localShards;
                synchronized (shardSnapshots) {
                    final var currentLocalShards = shardSnapshots.get(snapshot.snapshot());
                    if (currentLocalShards == null) {
                        continue;
                    }
                    localShards = Map.copyOf(currentLocalShards);
                }
                Map<ShardId, ShardSnapshotStatus> masterShards = snapshot.shards();
                for (Map.Entry<ShardId, IndexShardSnapshotStatus> localShard : localShards.entrySet()) {
                    ShardId shardId = localShard.getKey();
                    ShardSnapshotStatus masterShard = masterShards.get(shardId);
                    if (masterShard != null && masterShard.state().completed() == false) {
                        final IndexShardSnapshotStatus.Copy indexShardSnapshotStatus = localShard.getValue().asCopy();
                        final Stage stage = indexShardSnapshotStatus.getStage();
                        final String statusDescription = indexShardSnapshotStatus.getStatusDescription();
                        final int maxStatusAppend = 1000;
                        // Master knows about the shard and thinks it has not completed
                        if (stage == Stage.DONE) {
                            // but we think the shard is done - we need to make new master know that the shard is done
                            logger.debug(
                                "[{}] new master thinks the shard [{}] is not completed but the shard is done locally, "
                                    + "updating status on the master",
                                snapshot.snapshot(),
                                shardId
                            );
                            notifySuccessfulSnapshotShard(
                                snapshot.snapshot(),
                                shardId,
                                localShard.getValue().getShardSnapshotResult(),
                                (outcomeInfoString) -> localShard.getValue()
                                    .updateStatusDescription(
                                        Strings.format(
                                            """
                                                Data node already successfully finished shard snapshot, but a new master needed to be
                                                notified. New remote master notification outcome: [%s]. The prior shard snapshot status
                                                description was [%s]
                                                """,
                                            outcomeInfoString,
                                            statusDescription.length() < maxStatusAppend
                                                ? statusDescription
                                                : statusDescription.substring(0, maxStatusAppend)
                                        )
                                    )
                            );
                        } else if (stage == Stage.FAILURE) {
                            // but we think the shard failed - we need to make new master know that the shard failed
                            logger.debug(
                                "[{}] new master thinks the shard [{}] is not completed but the shard failed locally, "
                                    + "updating status on master",
                                snapshot.snapshot(),
                                shardId
                            );
                            notifyUnsuccessfulSnapshotShard(
                                snapshot.snapshot(),
                                shardId,
                                ShardState.FAILED,
                                indexShardSnapshotStatus.getFailure(),
                                localShard.getValue().generation(),
                                // Update the original statusDescription with the latest remote master call outcome, but include the old
                                // response. This will allow us to see when/whether the information reached the previous and current master.
                                (outcomeInfoString) -> localShard.getValue()
                                    .updateStatusDescription(
                                        Strings.format(
                                            """
                                                Data node already failed shard snapshot, but a new master needed to be notified. New remote
                                                master notification outcome: [%s]. The prior shard snapshot status description was [%s]
                                                """,
                                            outcomeInfoString,
                                            statusDescription.length() < maxStatusAppend
                                                ? statusDescription
                                                : statusDescription.substring(0, maxStatusAppend)
                                        )
                                    )
                            );
                        } else if (stage == Stage.PAUSED) {
                            // but we think the shard has paused - we need to make new master know that
                            logger.debug("""
                                new master thinks that shard [{}] snapshot [{}], with shard generation [{}], is still running, but the \
                                shard snapshot is paused locally, updating status on master
                                """, shardId, snapshot.snapshot(), localShard.getValue().generation());
                            notifyUnsuccessfulSnapshotShard(
                                snapshot.snapshot(),
                                shardId,
                                ShardState.PAUSED_FOR_NODE_REMOVAL,
                                indexShardSnapshotStatus.getFailure(),
                                localShard.getValue().generation(),
                                (outcomeInfoString) -> localShard.getValue()
                                    .updateStatusDescription(
                                        Strings.format(
                                            """
                                                Data node already paused shard snapshot, but a new master needed to be notified. New remote
                                                master notification outcome: [%s]. The prior shard snapshot status description was [%s]
                                                """,
                                            outcomeInfoString,
                                            statusDescription.length() < maxStatusAppend
                                                ? statusDescription
                                                : statusDescription.substring(0, maxStatusAppend)
                                        )
                                    )
                            );
                        }
                    }
                }

            }
        }
    }

    /**
     * Notify the master node that the given shard snapshot completed successfully.
     */
    private void notifySuccessfulSnapshotShard(
        final Snapshot snapshot,
        final ShardId shardId,
        ShardSnapshotResult shardSnapshotResult,
        Consumer<String> postMasterNotificationAction
    ) {
        assert shardSnapshotResult != null;
        assert shardSnapshotResult.getGeneration() != null;
        sendSnapshotShardUpdate(
            snapshot,
            shardId,
            ShardSnapshotStatus.success(clusterService.localNode().getId(), shardSnapshotResult),
            postMasterNotificationAction
        );
    }

    /**
     * Notify the master node that the given shard snapshot has completed but did not succeed
     */
    private void notifyUnsuccessfulSnapshotShard(
        final Snapshot snapshot,
        final ShardId shardId,
        final ShardState shardState,
        final String failure,
        final ShardGeneration generation,
        Consumer<String> postMasterNotificationAction
    ) {
        assert shardState == ShardState.FAILED || shardState == ShardState.PAUSED_FOR_NODE_REMOVAL : shardState;
        sendSnapshotShardUpdate(
            snapshot,
            shardId,
            new ShardSnapshotStatus(clusterService.localNode().getId(), shardState, generation, failure),
            postMasterNotificationAction
        );
        if (shardState == ShardState.PAUSED_FOR_NODE_REMOVAL) {
            logger.debug(
                "Pausing shard [{}] snapshot [{}], with shard generation [{}], because this node is marked for removal",
                shardId,
                snapshot,
                generation
            );
        }
    }

    /** Updates the shard snapshot status by sending a {@link UpdateIndexShardSnapshotStatusRequest} to the master node */
    private void sendSnapshotShardUpdate(
        final Snapshot snapshot,
        final ShardId shardId,
        final ShardSnapshotStatus status,
        Consumer<String> postMasterNotificationAction
    ) {
        snapshotShutdownProgressTracker.trackRequestSentToMaster(snapshot, shardId);
        ActionListener<Void> updateResultListener = new ActionListener<>() {
            @Override
            public void onResponse(Void aVoid) {
                snapshotShutdownProgressTracker.releaseRequestSentToMaster(snapshot, shardId);
                postMasterNotificationAction.accept(
                    Strings.format("successfully sent shard snapshot state [%s] update to the master node", status.state())
                );
                logger.trace("[{}][{}] updated snapshot state to [{}]", shardId, snapshot, status);
                if (status.state().completed()) {
                    shardStatusConsistencyChecker.ensureShardComplete(snapshot, shardId);
                } else {
                    assert status.state() == ShardState.PAUSED_FOR_NODE_REMOVAL : status;
                }
            }

            @Override
            public void onFailure(Exception e) {
                snapshotShutdownProgressTracker.releaseRequestSentToMaster(snapshot, shardId);
                postMasterNotificationAction.accept(
                    Strings.format("exception trying to send shard snapshot state [%s] update to the master node [%s]", status.state(), e)
                );
                logger.warn(() -> format("[%s][%s] failed to update snapshot state to [%s]", shardId, snapshot, status), e);
            }
        };

        remoteFailedRequestDeduplicator.executeOnce(
            new UpdateIndexShardSnapshotStatusRequest(snapshot, shardId, status),
            updateResultListener,
            (req, reqListener) -> transportService.sendRequest(
                transportService.getLocalNode(),
                SnapshotsService.UPDATE_SNAPSHOT_STATUS_ACTION_NAME,
                req,
                new ActionListenerResponseHandler<>(
                    reqListener.map(res -> null),
                    in -> ActionResponse.Empty.INSTANCE,
                    TransportResponseHandler.TRANSPORT_WORKER
                )
            )
        );
    }

    private final ShardStatusConsistencyChecker shardStatusConsistencyChecker = new ShardStatusConsistencyChecker();

    /**
     * After receiving an ack from the master confirming that it marked a shard snapshot as complete, checks the local cluster state to
     * confirm that it was updated to reflect this and logs a message if not. Not 100% watertight because this node might be lagging behind
     * and hasn't received the cluster state yet, or else it might have been removed from the cluster, but both of these things will also
     * be visible in the logs. See e.g. o.e.c.c.Publication#logIncompleteNodes and o.e.c.c.NodeLeftExecutor.
     */
    // visible for testing
    class ShardStatusConsistencyChecker {

        // dedicated logger so we can separate these logs from other SnapshotShardsService DEBUG logs
        private static final Logger CONSISTENCY_CHECKER_LOGGER = LogManager.getLogger(ShardStatusConsistencyChecker.class);

        private record CheckTask(Snapshot snapshot, ShardId shardId) {}

        private final AtomicInteger queuedTaskCount = new AtomicInteger(0); // atomic to ensure max one thread processing the queue
        private final Queue<CheckTask> queue = new ConcurrentLinkedQueue<>();

        /**
         * Adds a {@link CheckTask} with the snapshot info to the queue. If the queue was previously empty, then {@link #runCheck()} will be
         * started on a GENERIC thread.
         */
        void ensureShardComplete(Snapshot snapshot, ShardId shardId) {
            if (CONSISTENCY_CHECKER_LOGGER.isDebugEnabled() == false) {
                return;
            }

            if (queuedTaskCount.get() > 1000) {
                // racy check, we only need an approximate limit
                return;
            }

            queue.add(new CheckTask(snapshot, shardId));
            if (queuedTaskCount.getAndIncrement() == 0) {
                threadPool.generic().execute(this::runCheck);
            } // else a runCheck is already running somewhere and will pick up the task we just added
        }

        /**
         * Empties the {@link #queue} and verifies that the cluster state shows a successful shard snapshot for each {@link CheckTask}.
         * A debug level message is logged if the cluster state does not show a completed shard snapshot: this could be a bug, or a race
         * with a cluster state update not being applied yet.
         */
        private void runCheck() {
            while (true) {
                final var taskCount = queuedTaskCount.get();
                final var clusterStateSnapshotsInProgress = SnapshotsInProgress.get(clusterService.state());
                final var shardsBySnapshot = new HashMap<Snapshot, Set<ShardId>>();
                for (int i = 0; i < taskCount; i++) {
                    final var task = queue.poll();
                    assert task != null;
                    shardsBySnapshot.computeIfAbsent(task.snapshot(), ignored -> new HashSet<>()).add(task.shardId());
                }
                for (final var snapshotShards : shardsBySnapshot.entrySet()) {
                    final var snapshot = snapshotShards.getKey();
                    final var clusterStateSnapshotEntry = clusterStateSnapshotsInProgress.snapshot(snapshot);
                    if (clusterStateSnapshotEntry != null) {
                        for (final var shardId : snapshotShards.getValue()) {
                            final var clusterStateShardStatus = clusterStateSnapshotEntry.shards().get(shardId);
                            if (clusterStateShardStatus == null) {
                                CONSISTENCY_CHECKER_LOGGER.debug(
                                    "shard [{}] in snapshot [{}] unexpectedly not found (should be impossible)",
                                    shardId,
                                    snapshot
                                );
                            } else if (clusterStateShardStatus.state().completed() == false) {
                                CONSISTENCY_CHECKER_LOGGER.debug(
                                    "shard [{}] in snapshot [{}] unexpectedly still in state [{}] after notifying master",
                                    shardId,
                                    snapshot,
                                    clusterStateShardStatus
                                );
                            } // else shard is marked complete as expected
                        }
                    } // else snapshot already completed & removed from cluster state
                }

                if (queuedTaskCount.addAndGet(-taskCount) == 0) {
                    return;
                } // else someone added some more tasks, so keep trying
            }
        }
    }
}
