/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ResultDeduplicator;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardState;
import org.elasticsearch.cluster.SnapshotsInProgress.State;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.internal.io.IOUtils;
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
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * This service runs on data nodes and controls currently running shard snapshots on these nodes. It is responsible for
 * starting and stopping shard level snapshots.
 * See package level documentation of {@link org.elasticsearch.snapshots} for details.
 */
public class SnapshotShardsService extends AbstractLifecycleComponent implements ClusterStateListener, IndexEventListener {
    private static final Logger logger = LogManager.getLogger(SnapshotShardsService.class);

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final RepositoriesService repositoriesService;

    private final TransportService transportService;

    private final ThreadPool threadPool;

    private final Map<Snapshot, Map<ShardId, IndexShardSnapshotStatus>> shardSnapshots = new HashMap<>();

    // A map of snapshots to the shardIds that we already reported to the master as failed
    private final ResultDeduplicator<UpdateIndexShardSnapshotStatusRequest, Void> remoteFailedRequestDeduplicator =
        new ResultDeduplicator<>();

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
        if (DiscoveryNode.canContainData(settings)) {
            // this is only useful on the nodes that can hold data
            clusterService.addListener(this);
        }
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
            SnapshotsInProgress previousSnapshots = event.previousState().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
            SnapshotsInProgress currentSnapshots = event.state().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
            if (previousSnapshots.equals(currentSnapshots) == false) {
                synchronized (shardSnapshots) {
                    cancelRemoved(currentSnapshots);
                    startNewSnapshots(currentSnapshots);
                }
            }

            String previousMasterNodeId = event.previousState().nodes().getMasterNodeId();
            String currentMasterNodeId = event.state().nodes().getMasterNodeId();
            if (currentMasterNodeId != null && currentMasterNodeId.equals(previousMasterNodeId) == false) {
                syncShardStatsOnNewMaster(event);
            }

        } catch (Exception e) {
            assert false : new AssertionError(e);
            logger.warn("failed to update snapshot state", e);
        }
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        // abort any snapshots occurring on the soon-to-be closed shard
        synchronized (shardSnapshots) {
            for (Map.Entry<Snapshot, Map<ShardId, IndexShardSnapshotStatus>> snapshotShards : shardSnapshots.entrySet()) {
                Map<ShardId, IndexShardSnapshotStatus> shards = snapshotShards.getValue();
                if (shards.containsKey(shardId)) {
                    logger.debug(
                        "[{}] shard closing, abort snapshotting for snapshot [{}]",
                        shardId,
                        snapshotShards.getKey().getSnapshotId()
                    );
                    shards.get(shardId).abortIfNotCompleted("shard is closing, aborting");
                }
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
    public Map<ShardId, IndexShardSnapshotStatus> currentSnapshotShards(Snapshot snapshot) {
        synchronized (shardSnapshots) {
            final Map<ShardId, IndexShardSnapshotStatus> current = shardSnapshots.get(snapshot);
            return current == null ? null : new HashMap<>(current);
        }
    }

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
                    snapshotStatus.abortIfNotCompleted("snapshot has been removed in cluster state, aborting");
                }
            }
        }
    }

    private void startNewSnapshots(SnapshotsInProgress snapshotsInProgress) {
        final String localNodeId = clusterService.localNode().getId();
        for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
            final State entryState = entry.state();
            if (entry.isClone()) {
                // This is a snapshot clone, it will be executed on the current master
                continue;
            }
            if (entryState == State.STARTED) {
                Map<ShardId, IndexShardSnapshotStatus> startedShards = null;
                final Snapshot snapshot = entry.snapshot();
                Map<ShardId, IndexShardSnapshotStatus> snapshotShards = shardSnapshots.getOrDefault(snapshot, emptyMap());
                for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shard : entry.shards()) {
                    // Add all new shards to start processing on
                    final ShardId shardId = shard.key;
                    final ShardSnapshotStatus shardSnapshotStatus = shard.value;
                    if (shardSnapshotStatus.state() == ShardState.INIT
                        && localNodeId.equals(shardSnapshotStatus.nodeId())
                        && snapshotShards.containsKey(shardId) == false) {
                        logger.trace("[{}] adding shard to the queue", shardId);
                        if (startedShards == null) {
                            startedShards = new HashMap<>();
                        }
                        startedShards.put(shardId, IndexShardSnapshotStatus.newInitializing(shardSnapshotStatus.generation()));
                    }
                }
                if (startedShards != null && startedShards.isEmpty() == false) {
                    shardSnapshots.computeIfAbsent(snapshot, s -> new HashMap<>()).putAll(startedShards);
                    startNewShards(entry, startedShards);
                }
            } else if (entryState == State.ABORTED) {
                // Abort all running shards for this snapshot
                final Snapshot snapshot = entry.snapshot();
                Map<ShardId, IndexShardSnapshotStatus> snapshotShards = shardSnapshots.getOrDefault(snapshot, emptyMap());
                for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shard : entry.shards()) {
                    final IndexShardSnapshotStatus snapshotStatus = snapshotShards.get(shard.key);
                    if (snapshotStatus == null) {
                        // due to CS batching we might have missed the INIT state and straight went into ABORTED
                        // notify master that abort has completed by moving to FAILED
                        if (shard.value.state() == ShardState.ABORTED && localNodeId.equals(shard.value.nodeId())) {
                            notifyFailedSnapshotShard(snapshot, shard.key, shard.value.reason(), shard.value.generation());
                        }
                    } else {
                        snapshotStatus.abortIfNotCompleted("snapshot has been aborted");
                    }
                }
            }
        }
    }

    private void startNewShards(SnapshotsInProgress.Entry entry, Map<ShardId, IndexShardSnapshotStatus> startedShards) {
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
            final Snapshot snapshot = entry.snapshot();
            for (final Map.Entry<ShardId, IndexShardSnapshotStatus> shardEntry : startedShards.entrySet()) {
                final ShardId shardId = shardEntry.getKey();
                final IndexShardSnapshotStatus snapshotStatus = shardEntry.getValue();
                final IndexId indexId = entry.indices().get(shardId.getIndexName());
                assert indexId != null;
                assert SnapshotsService.useShardGenerations(entry.version())
                    || ShardGenerations.fixShardGeneration(snapshotStatus.generation()) == null
                    : "Found non-null, non-numeric shard generation ["
                        + snapshotStatus.generation()
                        + "] for snapshot with old-format compatibility";
                snapshot(shardId, snapshot, indexId, entry.userMetadata(), snapshotStatus, entry.version(), new ActionListener<>() {
                    @Override
                    public void onResponse(ShardSnapshotResult shardSnapshotResult) {
                        final String newGeneration = shardSnapshotResult.getGeneration();
                        assert newGeneration != null;
                        assert newGeneration.equals(snapshotStatus.generation());
                        if (logger.isDebugEnabled()) {
                            final IndexShardSnapshotStatus.Copy lastSnapshotStatus = snapshotStatus.asCopy();
                            logger.debug(
                                "[{}][{}] completed snapshot to [{}] with status [{}] at generation [{}]",
                                shardId,
                                snapshot,
                                snapshot.getRepository(),
                                lastSnapshotStatus,
                                snapshotStatus.generation()
                            );
                        }
                        notifySuccessfulSnapshotShard(snapshot, shardId, shardSnapshotResult);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        final String failure;
                        if (e instanceof AbortedSnapshotException) {
                            failure = "aborted";
                            logger.debug(() -> new ParameterizedMessage("[{}][{}] aborted shard snapshot", shardId, snapshot), e);
                        } else {
                            failure = summarizeFailure(e);
                            logger.warn(() -> new ParameterizedMessage("[{}][{}] failed to snapshot shard", shardId, snapshot), e);
                        }
                        snapshotStatus.moveToFailed(threadPool.absoluteTimeInMillis(), failure);
                        notifyFailedSnapshotShard(snapshot, shardId, failure, snapshotStatus.generation());
                    }
                });
            }
        });
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
        final Map<String, Object> userMetadata,
        final IndexShardSnapshotStatus snapshotStatus,
        Version version,
        ActionListener<ShardSnapshotResult> listener
    ) {
        try {
            final IndexShard indexShard = indicesService.indexServiceSafe(shardId.getIndex()).getShardOrNull(shardId.id());
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
            Engine.IndexCommitRef snapshotRef = null;
            try {
                snapshotRef = indexShard.acquireIndexCommitForSnapshot();
                repository.snapshotShard(
                    new SnapshotShardContext(
                        indexShard.store(),
                        indexShard.mapperService(),
                        snapshot.getSnapshotId(),
                        indexId,
                        snapshotRef,
                        getShardStateId(indexShard, snapshotRef.getIndexCommit()),
                        snapshotStatus,
                        version,
                        userMetadata,
                        listener
                    )
                );
            } catch (Exception e) {
                IOUtils.close(snapshotRef);
                throw e;
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
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
    private static String getShardStateId(IndexShard indexShard, IndexCommit snapshotIndexCommit) throws IOException {
        final Map<String, String> userCommitData = snapshotIndexCommit.getUserData();
        final SequenceNumbers.CommitInfo seqNumInfo = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(userCommitData.entrySet());
        final long maxSeqNo = seqNumInfo.maxSeqNo;
        if (maxSeqNo != seqNumInfo.localCheckpoint || maxSeqNo != indexShard.getLastSyncedGlobalCheckpoint()) {
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
    private void syncShardStatsOnNewMaster(ClusterChangedEvent event) {
        SnapshotsInProgress snapshotsInProgress = event.state().custom(SnapshotsInProgress.TYPE);
        if (snapshotsInProgress == null) {
            return;
        }

        // Clear request deduplicator since we need to send all requests that were potentially not handled by the previous
        // master again
        remoteFailedRequestDeduplicator.clear();
        for (SnapshotsInProgress.Entry snapshot : snapshotsInProgress.entries()) {
            if (snapshot.state() == State.STARTED || snapshot.state() == State.ABORTED) {
                Map<ShardId, IndexShardSnapshotStatus> localShards = currentSnapshotShards(snapshot.snapshot());
                if (localShards != null) {
                    ImmutableOpenMap<ShardId, ShardSnapshotStatus> masterShards = snapshot.shards();
                    for (Map.Entry<ShardId, IndexShardSnapshotStatus> localShard : localShards.entrySet()) {
                        ShardId shardId = localShard.getKey();
                        ShardSnapshotStatus masterShard = masterShards.get(shardId);
                        if (masterShard != null && masterShard.state().completed() == false) {
                            final IndexShardSnapshotStatus.Copy indexShardSnapshotStatus = localShard.getValue().asCopy();
                            final Stage stage = indexShardSnapshotStatus.getStage();
                            // Master knows about the shard and thinks it has not completed
                            if (stage == Stage.DONE) {
                                // but we think the shard is done - we need to make new master know that the shard is done
                                logger.debug(
                                    "[{}] new master thinks the shard [{}] is not completed but the shard is done locally, "
                                        + "updating status on the master",
                                    snapshot.snapshot(),
                                    shardId
                                );
                                notifySuccessfulSnapshotShard(snapshot.snapshot(), shardId, localShard.getValue().getShardSnapshotResult());

                            } else if (stage == Stage.FAILURE) {
                                // but we think the shard failed - we need to make new master know that the shard failed
                                logger.debug(
                                    "[{}] new master thinks the shard [{}] is not completed but the shard failed locally, "
                                        + "updating status on master",
                                    snapshot.snapshot(),
                                    shardId
                                );
                                notifyFailedSnapshotShard(
                                    snapshot.snapshot(),
                                    shardId,
                                    indexShardSnapshotStatus.getFailure(),
                                    localShard.getValue().generation()
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    /** Notify the master node that the given shard has been successfully snapshotted **/
    private void notifySuccessfulSnapshotShard(final Snapshot snapshot, final ShardId shardId, ShardSnapshotResult shardSnapshotResult) {
        assert shardSnapshotResult != null;
        assert shardSnapshotResult.getGeneration() != null;
        sendSnapshotShardUpdate(snapshot, shardId, ShardSnapshotStatus.success(clusterService.localNode().getId(), shardSnapshotResult));
    }

    /** Notify the master node that the given shard failed to be snapshotted **/
    private void notifyFailedSnapshotShard(final Snapshot snapshot, final ShardId shardId, final String failure, final String generation) {
        sendSnapshotShardUpdate(
            snapshot,
            shardId,
            new ShardSnapshotStatus(clusterService.localNode().getId(), ShardState.FAILED, failure, generation)
        );
    }

    /** Updates the shard snapshot status by sending a {@link UpdateIndexShardSnapshotStatusRequest} to the master node */
    private void sendSnapshotShardUpdate(final Snapshot snapshot, final ShardId shardId, final ShardSnapshotStatus status) {
        remoteFailedRequestDeduplicator.executeOnce(
            new UpdateIndexShardSnapshotStatusRequest(snapshot, shardId, status),
            new ActionListener<>() {
                @Override
                public void onResponse(Void aVoid) {
                    logger.trace("[{}][{}] updated snapshot state to [{}]", shardId, snapshot, status);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn(
                        () -> new ParameterizedMessage("[{}][{}] failed to update snapshot state to [{}]", shardId, snapshot, status),
                        e
                    );
                }
            },
            (req, reqListener) -> transportService.sendRequest(
                transportService.getLocalNode(),
                SnapshotsService.UPDATE_SNAPSHOT_STATUS_ACTION_NAME,
                req,
                new TransportResponseHandler<ActionResponse.Empty>() {
                    @Override
                    public ActionResponse.Empty read(StreamInput in) {
                        return ActionResponse.Empty.INSTANCE;
                    }

                    @Override
                    public void handleResponse(ActionResponse.Empty response) {
                        reqListener.onResponse(null);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        reqListener.onFailure(exp);
                    }
                }
            )
        );
    }
}
