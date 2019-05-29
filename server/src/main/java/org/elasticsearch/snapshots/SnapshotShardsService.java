/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.snapshots;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus;
import org.elasticsearch.cluster.SnapshotsInProgress.State;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.SnapshotFailedEngineException;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus.Stage;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestDeduplicator;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.cluster.SnapshotsInProgress.completed;
import static org.elasticsearch.transport.EmptyTransportResponseHandler.INSTANCE_SAME;

/**
 * This service runs on data and master nodes and controls currently snapshotted shards on these nodes. It is responsible for
 * starting and stopping shard level snapshots
 */
public class SnapshotShardsService extends AbstractLifecycleComponent implements ClusterStateListener, IndexEventListener {
    private static final Logger logger = LogManager.getLogger(SnapshotShardsService.class);

    public static final String UPDATE_SNAPSHOT_STATUS_ACTION_NAME_V6 = "internal:cluster/snapshot/update_snapshot";
    public static final String UPDATE_SNAPSHOT_STATUS_ACTION_NAME = "internal:cluster/snapshot/update_snapshot_status";

    private final Settings settings;

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final SnapshotsService snapshotsService;

    private final TransportService transportService;

    private final ThreadPool threadPool;

    private final Map<Snapshot, Map<ShardId, IndexShardSnapshotStatus>> shardSnapshots = new HashMap<>();

    // A map of snapshots to the shardIds that we already reported to the master as failed
    private final TransportRequestDeduplicator<UpdateIndexShardSnapshotStatusRequest> remoteFailedRequestDeduplicator =
        new TransportRequestDeduplicator<>();

    private final SnapshotStateExecutor snapshotStateExecutor = new SnapshotStateExecutor();
    private final UpdateSnapshotStatusAction updateSnapshotStatusHandler;

    @Inject
    public SnapshotShardsService(Settings settings, ClusterService clusterService, SnapshotsService snapshotsService,
                                 ThreadPool threadPool, TransportService transportService, IndicesService indicesService,
                                 ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        this.settings = settings;
        this.indicesService = indicesService;
        this.snapshotsService = snapshotsService;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        if (DiscoveryNode.isDataNode(settings)) {
            // this is only useful on the nodes that can hold data
            clusterService.addListener(this);
        }

        // The constructor of UpdateSnapshotStatusAction will register itself to the TransportService.
        this.updateSnapshotStatusHandler = new UpdateSnapshotStatusAction(
            transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver);

        if (DiscoveryNode.isMasterNode(settings)) {
            // This needs to run only on nodes that can become masters
            transportService.registerRequestHandler(UPDATE_SNAPSHOT_STATUS_ACTION_NAME_V6, UpdateSnapshotStatusRequestV6::new,
                ThreadPool.Names.SAME, new UpdateSnapshotStateRequestHandlerV6());
        }
    }

    @Override
    protected void doStart() {
        assert this.updateSnapshotStatusHandler != null;
        assert transportService.getRequestHandler(UPDATE_SNAPSHOT_STATUS_ACTION_NAME) != null;
        if (DiscoveryNode.isMasterNode(settings)) {
            assert transportService.getRequestHandler(UPDATE_SNAPSHOT_STATUS_ACTION_NAME_V6) != null;
        }
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {
        clusterService.removeListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        try {
            SnapshotsInProgress previousSnapshots = event.previousState().custom(SnapshotsInProgress.TYPE);
            SnapshotsInProgress currentSnapshots = event.state().custom(SnapshotsInProgress.TYPE);
            if ((previousSnapshots == null && currentSnapshots != null)
                || (previousSnapshots != null && previousSnapshots.equals(currentSnapshots) == false)) {
                synchronized (shardSnapshots) {
                    processIndexShardSnapshots(currentSnapshots, event.state().nodes().getMasterNode());
                }
            }

            String previousMasterNodeId = event.previousState().nodes().getMasterNodeId();
            String currentMasterNodeId = event.state().nodes().getMasterNodeId();
            if (currentMasterNodeId != null && currentMasterNodeId.equals(previousMasterNodeId) == false) {
                syncShardStatsOnNewMaster(event);
            }

        } catch (Exception e) {
            logger.warn("Failed to update snapshot state ", e);
        }
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        // abort any snapshots occurring on the soon-to-be closed shard
        synchronized (shardSnapshots) {
            for (Map.Entry<Snapshot, Map<ShardId, IndexShardSnapshotStatus>> snapshotShards : shardSnapshots.entrySet()) {
                Map<ShardId, IndexShardSnapshotStatus> shards = snapshotShards.getValue();
                if (shards.containsKey(shardId)) {
                    logger.debug("[{}] shard closing, abort snapshotting for snapshot [{}]",
                        shardId, snapshotShards.getKey().getSnapshotId());
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

    /**
     * Checks if any new shards should be snapshotted on this node
     *
     * @param snapshotsInProgress Current snapshots in progress in cluster state
     */
    private void processIndexShardSnapshots(SnapshotsInProgress snapshotsInProgress, DiscoveryNode masterNode) {
        cancelRemoved(snapshotsInProgress);
        if (snapshotsInProgress != null) {
            startNewSnapshots(snapshotsInProgress, masterNode);
        }
    }

    private void cancelRemoved(@Nullable SnapshotsInProgress snapshotsInProgress) {
        // First, remove snapshots that are no longer there
        Iterator<Map.Entry<Snapshot, Map<ShardId, IndexShardSnapshotStatus>>> it = shardSnapshots.entrySet().iterator();
        while (it.hasNext()) {
            final Map.Entry<Snapshot, Map<ShardId, IndexShardSnapshotStatus>> entry = it.next();
            final Snapshot snapshot = entry.getKey();
            if (snapshotsInProgress == null || snapshotsInProgress.snapshot(snapshot) == null) {
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

    private void startNewSnapshots(SnapshotsInProgress snapshotsInProgress, DiscoveryNode masterNode) {
        // For now we will be mostly dealing with a single snapshot at a time but might have multiple simultaneously running
        // snapshots in the future
        // Now go through all snapshots and update existing or create missing
        final String localNodeId = clusterService.localNode().getId();
        for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
            final State entryState = entry.state();
            if (entryState == State.STARTED) {
                Map<ShardId, IndexShardSnapshotStatus> startedShards = null;
                final Snapshot snapshot = entry.snapshot();
                Map<ShardId, IndexShardSnapshotStatus> snapshotShards = shardSnapshots.getOrDefault(snapshot, emptyMap());
                for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shard : entry.shards()) {
                    // Add all new shards to start processing on
                    final ShardId shardId = shard.key;
                    final ShardSnapshotStatus shardSnapshotStatus = shard.value;
                    if (localNodeId.equals(shardSnapshotStatus.nodeId()) && shardSnapshotStatus.state() == State.INIT
                        && snapshotShards.containsKey(shardId) == false) {
                        logger.trace("[{}] - Adding shard to the queue", shardId);
                        if (startedShards == null) {
                             startedShards = new HashMap<>();
                        }
                        startedShards.put(shardId, IndexShardSnapshotStatus.newInitializing());
                    }
                }
                if (startedShards != null && startedShards.isEmpty() == false) {
                    shardSnapshots.computeIfAbsent(snapshot, s -> new HashMap<>()).putAll(startedShards);
                    startNewShards(entry, startedShards, masterNode);
                }
            } else if (entryState == State.ABORTED) {
                // Abort all running shards for this snapshot
                final Snapshot snapshot = entry.snapshot();
                Map<ShardId, IndexShardSnapshotStatus> snapshotShards = shardSnapshots.getOrDefault(snapshot, emptyMap());
                for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shard : entry.shards()) {
                    final IndexShardSnapshotStatus snapshotStatus = snapshotShards.get(shard.key);
                    if (snapshotStatus != null) {
                        final IndexShardSnapshotStatus.Copy lastSnapshotStatus =
                            snapshotStatus.abortIfNotCompleted("snapshot has been aborted");
                        final Stage stage = lastSnapshotStatus.getStage();
                        if (stage == Stage.FINALIZE) {
                            logger.debug("[{}] trying to cancel snapshot on shard [{}] that is finalizing, " +
                                "letting it finish", snapshot, shard.key);
                        } else if (stage == Stage.DONE) {
                            logger.debug("[{}] trying to cancel snapshot on the shard [{}] that is already done, " +
                                "updating status on the master", snapshot, shard.key);
                            notifySuccessfulSnapshotShard(snapshot, shard.key, masterNode);
                        } else if (stage == Stage.FAILURE) {
                            logger.debug("[{}] trying to cancel snapshot on the shard [{}] that has already failed, " +
                                "updating status on the master", snapshot, shard.key);
                            notifyFailedSnapshotShard(snapshot, shard.key, lastSnapshotStatus.getFailure(), masterNode);
                        }
                    } else {
                        // due to CS batching we might have missed the INIT state and straight went into ABORTED
                        // notify master that abort has completed by moving to FAILED
                        if (shard.value.state() == State.ABORTED) {
                            notifyFailedSnapshotShard(snapshot, shard.key, shard.value.reason(), masterNode);
                        }
                    }
                }
            }
        }
    }

    private void startNewShards(SnapshotsInProgress.Entry entry, Map<ShardId, IndexShardSnapshotStatus> startedShards,
                                DiscoveryNode masterNode) {
        final Snapshot snapshot = entry.snapshot();
        final Map<String, IndexId> indicesMap = entry.indices().stream().collect(Collectors.toMap(IndexId::getName, Function.identity()));
        final Executor executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);
        for (final Map.Entry<ShardId, IndexShardSnapshotStatus> shardEntry : startedShards.entrySet()) {
            final ShardId shardId = shardEntry.getKey();
            final IndexId indexId = indicesMap.get(shardId.getIndexName());
            assert indexId != null;
            executor.execute(new AbstractRunnable() {

                private final SetOnce<Exception> failure = new SetOnce<>();

                @Override
                public void doRun() {
                    final IndexShard indexShard =
                        indicesService.indexServiceSafe(shardId.getIndex()).getShardOrNull(shardId.id());
                    snapshot(indexShard, snapshot, indexId, shardEntry.getValue());
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn(() -> new ParameterizedMessage("[{}][{}] failed to snapshot shard", shardId, snapshot), e);
                    failure.set(e);
                }

                @Override
                public void onRejection(Exception e) {
                    failure.set(e);
                }

                @Override
                public void onAfter() {
                    final Exception exception = failure.get();
                    if (exception != null) {
                        notifyFailedSnapshotShard(snapshot, shardId, ExceptionsHelper.detailedMessage(exception), masterNode);
                    } else {
                        notifySuccessfulSnapshotShard(snapshot, shardId, masterNode);
                    }
                }
            });
        }
    }

    /**
     * Creates shard snapshot
     *
     * @param snapshot       snapshot
     * @param snapshotStatus snapshot status
     */
    private void snapshot(final IndexShard indexShard, final Snapshot snapshot, final IndexId indexId,
                          final IndexShardSnapshotStatus snapshotStatus) {
        final ShardId shardId = indexShard.shardId();
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

        final Repository repository = snapshotsService.getRepositoriesService().repository(snapshot.getRepository());
        try {
            // we flush first to make sure we get the latest writes snapshotted
            try (Engine.IndexCommitRef snapshotRef = indexShard.acquireLastIndexCommit(true)) {
                repository.snapshotShard(indexShard, indexShard.store(), snapshot.getSnapshotId(), indexId, snapshotRef.getIndexCommit(),
                    snapshotStatus);
                if (logger.isDebugEnabled()) {
                    final IndexShardSnapshotStatus.Copy lastSnapshotStatus = snapshotStatus.asCopy();
                    logger.debug("snapshot ({}) completed to {} with {}", snapshot, repository, lastSnapshotStatus);
                }
            }
        } catch (SnapshotFailedEngineException | IndexShardSnapshotFailedException e) {
            throw e;
        } catch (Exception e) {
            throw new IndexShardSnapshotFailedException(shardId, "Failed to snapshot", e);
        }
    }

    /**
     * Checks if any shards were processed that the new master doesn't know about
     */
    private void syncShardStatsOnNewMaster(ClusterChangedEvent event) {
        SnapshotsInProgress snapshotsInProgress = event.state().custom(SnapshotsInProgress.TYPE);
        if (snapshotsInProgress == null) {
            return;
        }
        final DiscoveryNode masterNode = event.state().nodes().getMasterNode();
        for (SnapshotsInProgress.Entry snapshot : snapshotsInProgress.entries()) {
            if (snapshot.state() == State.STARTED || snapshot.state() == State.ABORTED) {
                Map<ShardId, IndexShardSnapshotStatus> localShards = currentSnapshotShards(snapshot.snapshot());
                if (localShards != null) {
                    ImmutableOpenMap<ShardId, ShardSnapshotStatus> masterShards = snapshot.shards();
                    for(Map.Entry<ShardId, IndexShardSnapshotStatus> localShard : localShards.entrySet()) {
                        ShardId shardId = localShard.getKey();
                        ShardSnapshotStatus masterShard = masterShards.get(shardId);
                        if (masterShard != null && masterShard.state().completed() == false) {
                            final IndexShardSnapshotStatus.Copy indexShardSnapshotStatus = localShard.getValue().asCopy();
                            final Stage stage = indexShardSnapshotStatus.getStage();
                            // Master knows about the shard and thinks it has not completed
                            if (stage == Stage.DONE) {
                                // but we think the shard is done - we need to make new master know that the shard is done
                                logger.debug("[{}] new master thinks the shard [{}] is not completed but the shard is done locally, " +
                                             "updating status on the master", snapshot.snapshot(), shardId);
                                notifySuccessfulSnapshotShard(snapshot.snapshot(), shardId, masterNode);

                            } else if (stage == Stage.FAILURE) {
                                // but we think the shard failed - we need to make new master know that the shard failed
                                logger.debug("[{}] new master thinks the shard [{}] is not completed but the shard failed locally, " +
                                             "updating status on master", snapshot.snapshot(), shardId);
                                notifyFailedSnapshotShard(snapshot.snapshot(), shardId, indexShardSnapshotStatus.getFailure(), masterNode);
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Internal request that is used to send changes in snapshot status to master
     */
    public static class UpdateIndexShardSnapshotStatusRequest extends MasterNodeRequest<UpdateIndexShardSnapshotStatusRequest> {
        private Snapshot snapshot;
        private ShardId shardId;
        private ShardSnapshotStatus status;

        public UpdateIndexShardSnapshotStatusRequest() {

        }

        public UpdateIndexShardSnapshotStatusRequest(Snapshot snapshot, ShardId shardId, ShardSnapshotStatus status) {
            this.snapshot = snapshot;
            this.shardId = shardId;
            this.status = status;
            // By default, we keep trying to post snapshot status messages to avoid snapshot processes getting stuck.
            this.masterNodeTimeout = TimeValue.timeValueNanos(Long.MAX_VALUE);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            snapshot = new Snapshot(in);
            shardId = ShardId.readShardId(in);
            status = new ShardSnapshotStatus(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            snapshot.writeTo(out);
            shardId.writeTo(out);
            status.writeTo(out);
        }

        public Snapshot snapshot() {
            return snapshot;
        }

        public ShardId shardId() {
            return shardId;
        }

        public ShardSnapshotStatus status() {
            return status;
        }

        @Override
        public String toString() {
            return snapshot + ", shardId [" + shardId + "], status [" + status.state() + "]";
        }
    }

    /** Notify the master node that the given shard has been successfully snapshotted **/
    private void notifySuccessfulSnapshotShard(final Snapshot snapshot, final ShardId shardId, DiscoveryNode masterNode) {
        sendSnapshotShardUpdate(
            snapshot, shardId, new ShardSnapshotStatus(clusterService.localNode().getId(), State.SUCCESS), masterNode);
    }

    /** Notify the master node that the given shard failed to be snapshotted **/
    private void notifyFailedSnapshotShard(Snapshot snapshot, ShardId shardId, String failure, DiscoveryNode masterNode) {
        sendSnapshotShardUpdate(
            snapshot, shardId, new ShardSnapshotStatus(clusterService.localNode().getId(), State.FAILED, failure), masterNode);
    }

    /** Updates the shard snapshot status by sending a {@link UpdateIndexShardSnapshotStatusRequest} to the master node */
    void sendSnapshotShardUpdate(Snapshot snapshot, ShardId shardId, ShardSnapshotStatus status, DiscoveryNode masterNode) {
        try {
            if (masterNode.getVersion().onOrAfter(Version.V_6_1_0)) {
                remoteFailedRequestDeduplicator.executeOnce(
                    new UpdateIndexShardSnapshotStatusRequest(snapshot, shardId, status),
                    new ActionListener<Void>() {
                        @Override
                        public void onResponse(Void aVoid) {
                            logger.trace("[{}] [{}] updated snapshot state", snapshot, status);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.warn(
                                () -> new ParameterizedMessage("[{}] [{}] failed to update snapshot state", snapshot, status), e);
                        }
                    },
                    (req, reqListener) -> transportService.sendRequest(
                        transportService.getLocalNode(), UPDATE_SNAPSHOT_STATUS_ACTION_NAME, req,
                        new TransportResponseHandler<UpdateIndexShardSnapshotStatusResponse>() {
                            @Override
                            public UpdateIndexShardSnapshotStatusResponse read(StreamInput in) throws IOException {
                                final UpdateIndexShardSnapshotStatusResponse response = new UpdateIndexShardSnapshotStatusResponse();
                                response.readFrom(in);
                                return response;
                            }

                            @Override
                            public void handleResponse(UpdateIndexShardSnapshotStatusResponse response) {
                                reqListener.onResponse(null);
                            }

                            @Override
                            public void handleException(TransportException exp) {
                                reqListener.onFailure(exp);
                            }

                            @Override
                            public String executor() {
                                return ThreadPool.Names.SAME;
                            }
                        })
                );
            } else {
                transportService.sendRequest(masterNode, UPDATE_SNAPSHOT_STATUS_ACTION_NAME_V6,
                    new UpdateSnapshotStatusRequestV6(snapshot, shardId, status), INSTANCE_SAME);
            }
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("[{}] [{}] failed to update snapshot state", snapshot, status), e);
        }
    }

    /**
     * Updates the shard status on master node
     *
     * @param request update shard status request
     */
    private void innerUpdateSnapshotState(final UpdateIndexShardSnapshotStatusRequest request,
                                          ActionListener<UpdateIndexShardSnapshotStatusResponse> listener) {
        logger.trace("received updated snapshot restore state [{}]", request);
        clusterService.submitStateUpdateTask(
            "update snapshot state",
            request,
            ClusterStateTaskConfig.build(Priority.NORMAL),
            snapshotStateExecutor,
            new ClusterStateTaskListener() {
                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(new UpdateIndexShardSnapshotStatusResponse());
                }
            });
    }

    private class SnapshotStateExecutor implements ClusterStateTaskExecutor<UpdateIndexShardSnapshotStatusRequest> {

        @Override
        public ClusterTasksResult<UpdateIndexShardSnapshotStatusRequest>
                        execute(ClusterState currentState, List<UpdateIndexShardSnapshotStatusRequest> tasks) {
            final SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
            if (snapshots != null) {
                int changedCount = 0;
                final List<SnapshotsInProgress.Entry> entries = new ArrayList<>();
                for (SnapshotsInProgress.Entry entry : snapshots.entries()) {
                    ImmutableOpenMap.Builder<ShardId, ShardSnapshotStatus> shards = ImmutableOpenMap.builder();
                    boolean updated = false;

                    for (UpdateIndexShardSnapshotStatusRequest updateSnapshotState : tasks) {
                        if (entry.snapshot().equals(updateSnapshotState.snapshot())) {
                            logger.trace("[{}] Updating shard [{}] with status [{}]",
                                updateSnapshotState.snapshot(), updateSnapshotState.shardId(), updateSnapshotState.status().state());
                            if (updated == false) {
                                shards.putAll(entry.shards());
                                updated = true;
                            }
                            shards.put(updateSnapshotState.shardId(), updateSnapshotState.status());
                            changedCount++;
                        }
                    }

                    if (updated) {
                        if (completed(shards.values()) == false) {
                            entries.add(new SnapshotsInProgress.Entry(entry, shards.build()));
                        } else {
                            // Snapshot is finished - mark it as done
                            // TODO: Add PARTIAL_SUCCESS status?
                            SnapshotsInProgress.Entry updatedEntry = new SnapshotsInProgress.Entry(entry, State.SUCCESS, shards.build());
                            entries.add(updatedEntry);
                        }
                    } else {
                        entries.add(entry);
                    }
                }
                if (changedCount > 0) {
                    logger.trace("changed cluster state triggered by {} snapshot state updates", changedCount);
                    return ClusterTasksResult.<UpdateIndexShardSnapshotStatusRequest>builder().successes(tasks)
                        .build(ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE,
                            new SnapshotsInProgress(unmodifiableList(entries))).build());
                }
            }
            return ClusterTasksResult.<UpdateIndexShardSnapshotStatusRequest>builder().successes(tasks).build(currentState);
        }
    }

    static class UpdateIndexShardSnapshotStatusResponse extends ActionResponse {

    }

    private class UpdateSnapshotStatusAction
        extends TransportMasterNodeAction<UpdateIndexShardSnapshotStatusRequest, UpdateIndexShardSnapshotStatusResponse> {
            UpdateSnapshotStatusAction(TransportService transportService, ClusterService clusterService,
                ThreadPool threadPool, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
                    super(
                        settings, SnapshotShardsService.UPDATE_SNAPSHOT_STATUS_ACTION_NAME, transportService, clusterService, threadPool,
                        actionFilters, indexNameExpressionResolver, UpdateIndexShardSnapshotStatusRequest::new
                    );
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected UpdateIndexShardSnapshotStatusResponse newResponse() {
            return new UpdateIndexShardSnapshotStatusResponse();
        }

        @Override
        protected void masterOperation(UpdateIndexShardSnapshotStatusRequest request, ClusterState state,
                                       ActionListener<UpdateIndexShardSnapshotStatusResponse> listener) {
            innerUpdateSnapshotState(request, listener);
        }

        @Override
        protected ClusterBlockException checkBlock(UpdateIndexShardSnapshotStatusRequest request, ClusterState state) {
            return null;
        }
    }

    /**
     * A BWC version of {@link UpdateIndexShardSnapshotStatusRequest}
     */
    static class UpdateSnapshotStatusRequestV6 extends TransportRequest {
        private Snapshot snapshot;
        private ShardId shardId;
        private ShardSnapshotStatus status;

        UpdateSnapshotStatusRequestV6() {

        }

        UpdateSnapshotStatusRequestV6(Snapshot snapshot, ShardId shardId, ShardSnapshotStatus status) {
            this.snapshot = snapshot;
            this.shardId = shardId;
            this.status = status;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            snapshot = new Snapshot(in);
            shardId = ShardId.readShardId(in);
            status = new ShardSnapshotStatus(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            snapshot.writeTo(out);
            shardId.writeTo(out);
            status.writeTo(out);
        }

        Snapshot snapshot() {
            return snapshot;
        }

        ShardId shardId() {
            return shardId;
        }

        ShardSnapshotStatus status() {
            return status;
        }

        @Override
        public String toString() {
            return snapshot + ", shardId [" + shardId + "], status [" + status.state() + "]";
        }
    }

    /**
     * A BWC version of {@link UpdateSnapshotStatusAction}
     */
    class UpdateSnapshotStateRequestHandlerV6 implements TransportRequestHandler<UpdateSnapshotStatusRequestV6> {
        @Override
        public void messageReceived(UpdateSnapshotStatusRequestV6 requestV6, final TransportChannel channel) throws Exception {
            final UpdateIndexShardSnapshotStatusRequest request =
                new UpdateIndexShardSnapshotStatusRequest(requestV6.snapshot(), requestV6.shardId(), requestV6.status());
            innerUpdateSnapshotState(request, new ActionListener<UpdateIndexShardSnapshotStatusResponse>() {
                @Override
                public void onResponse(UpdateIndexShardSnapshotStatusResponse updateSnapshotStatusResponse) {

                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn("Failed to update snapshot status", e);
                }
            });
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

}
