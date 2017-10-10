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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus;
import org.elasticsearch.cluster.SnapshotsInProgress.State;
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
import org.elasticsearch.common.unit.ByteSizeValue;
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
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.cluster.SnapshotsInProgress.completed;

/**
 * This service runs on data and master nodes and controls currently snapshotted shards on these nodes. It is responsible for
 * starting and stopping shard level snapshots
 */
public class SnapshotShardsService extends AbstractLifecycleComponent implements ClusterStateApplier, IndexEventListener {

    public static final String UPDATE_SNAPSHOT_ACTION_NAME = "internal:cluster/snapshot/update_snapshot";

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final SnapshotsService snapshotsService;

    private final TransportService transportService;

    private final ThreadPool threadPool;

    private final Lock shutdownLock = new ReentrantLock();

    private final Condition shutdownCondition = shutdownLock.newCondition();

    private volatile Map<Snapshot, SnapshotShards> shardSnapshots = emptyMap();

    private final SnapshotStateExecutor snapshotStateExecutor = new SnapshotStateExecutor();

    @Inject
    public SnapshotShardsService(Settings settings, ClusterService clusterService, SnapshotsService snapshotsService, ThreadPool threadPool,
                                 TransportService transportService, IndicesService indicesService) {
        super(settings);
        this.indicesService = indicesService;
        this.snapshotsService = snapshotsService;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        if (DiscoveryNode.isDataNode(settings)) {
            // this is only useful on the nodes that can hold data
            // addLowPriorityApplier to make sure that Repository will be created before snapshot
            clusterService.addLowPriorityApplier(this);
        }

        if (DiscoveryNode.isMasterNode(settings)) {
            // This needs to run only on nodes that can become masters
            transportService.registerRequestHandler(UPDATE_SNAPSHOT_ACTION_NAME, UpdateIndexShardSnapshotStatusRequest::new, ThreadPool.Names.SAME, new UpdateSnapshotStateRequestHandler());
        }

    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {
        shutdownLock.lock();
        try {
            while(!shardSnapshots.isEmpty() && shutdownCondition.await(5, TimeUnit.SECONDS)) {
                // Wait for at most 5 second for locally running snapshots to finish
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } finally {
            shutdownLock.unlock();
        }

    }

    @Override
    protected void doClose() {
        clusterService.removeApplier(this);
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        try {
            SnapshotsInProgress prev = event.previousState().custom(SnapshotsInProgress.TYPE);
            SnapshotsInProgress curr = event.state().custom(SnapshotsInProgress.TYPE);

            if ((prev == null && curr != null) || (prev != null && prev.equals(curr) == false)) {
                processIndexShardSnapshots(event);
            }
            String masterNodeId = event.state().nodes().getMasterNodeId();
            if (masterNodeId != null && masterNodeId.equals(event.previousState().nodes().getMasterNodeId()) == false) {
                syncShardStatsOnNewMaster(event);
            }

        } catch (Exception e) {
            logger.warn("Failed to update snapshot state ", e);
        }
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        // abort any snapshots occurring on the soon-to-be closed shard
        Map<Snapshot, SnapshotShards> snapshotShardsMap = shardSnapshots;
        for (Map.Entry<Snapshot, SnapshotShards> snapshotShards : snapshotShardsMap.entrySet()) {
            Map<ShardId, IndexShardSnapshotStatus> shards = snapshotShards.getValue().shards;
            if (shards.containsKey(shardId)) {
                logger.debug("[{}] shard closing, abort snapshotting for snapshot [{}]", shardId, snapshotShards.getKey().getSnapshotId());
                shards.get(shardId).abort();
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
        SnapshotShards snapshotShards = shardSnapshots.get(snapshot);
        if (snapshotShards == null) {
            return null;
        } else {
            return snapshotShards.shards;
        }
    }

    /**
     * Checks if any new shards should be snapshotted on this node
     *
     * @param event cluster state changed event
     */
    private void processIndexShardSnapshots(ClusterChangedEvent event) {
        SnapshotsInProgress snapshotsInProgress = event.state().custom(SnapshotsInProgress.TYPE);
        Map<Snapshot, SnapshotShards> survivors = new HashMap<>();
        // First, remove snapshots that are no longer there
        for (Map.Entry<Snapshot, SnapshotShards> entry : shardSnapshots.entrySet()) {
            final Snapshot snapshot = entry.getKey();
            if (snapshotsInProgress != null && snapshotsInProgress.snapshot(snapshot) != null) {
                survivors.put(entry.getKey(), entry.getValue());
            } else {
                // abort any running snapshots of shards for the removed entry;
                // this could happen if for some reason the cluster state update for aborting
                // running shards is missed, then the snapshot is removed is a subsequent cluster
                // state update, which is being processed here
                for (IndexShardSnapshotStatus snapshotStatus : entry.getValue().shards.values()) {
                    if (snapshotStatus.stage() == Stage.INIT || snapshotStatus.stage() == Stage.STARTED) {
                        snapshotStatus.abort();
                    }
                }
            }
        }

        // For now we will be mostly dealing with a single snapshot at a time but might have multiple simultaneously running
        // snapshots in the future
        Map<Snapshot, Map<ShardId, IndexShardSnapshotStatus>> newSnapshots = new HashMap<>();
        // Now go through all snapshots and update existing or create missing
        final String localNodeId = event.state().nodes().getLocalNodeId();
        final DiscoveryNode masterNode = event.state().nodes().getMasterNode();
        final Map<Snapshot, Map<String, IndexId>> snapshotIndices = new HashMap<>();
        if (snapshotsInProgress != null) {
            for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
                snapshotIndices.put(entry.snapshot(),
                                    entry.indices().stream().collect(Collectors.toMap(IndexId::getName, Function.identity())));
                if (entry.state() == State.STARTED) {
                    Map<ShardId, IndexShardSnapshotStatus> startedShards = new HashMap<>();
                    SnapshotShards snapshotShards = shardSnapshots.get(entry.snapshot());
                    for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shard : entry.shards()) {
                        // Add all new shards to start processing on
                        if (localNodeId.equals(shard.value.nodeId())) {
                            if (shard.value.state() == State.INIT && (snapshotShards == null || !snapshotShards.shards.containsKey(shard.key))) {
                                logger.trace("[{}] - Adding shard to the queue", shard.key);
                                startedShards.put(shard.key, new IndexShardSnapshotStatus());
                            }
                        }
                    }
                    if (!startedShards.isEmpty()) {
                        newSnapshots.put(entry.snapshot(), startedShards);
                        if (snapshotShards != null) {
                            // We already saw this snapshot but we need to add more started shards
                            Map<ShardId, IndexShardSnapshotStatus> shards = new HashMap<>();
                            // Put all shards that were already running on this node
                            shards.putAll(snapshotShards.shards);
                            // Put all newly started shards
                            shards.putAll(startedShards);
                            survivors.put(entry.snapshot(), new SnapshotShards(unmodifiableMap(shards)));
                        } else {
                            // Brand new snapshot that we haven't seen before
                            survivors.put(entry.snapshot(), new SnapshotShards(unmodifiableMap(startedShards)));
                        }
                    }
                } else if (entry.state() == State.ABORTED) {
                    // Abort all running shards for this snapshot
                    SnapshotShards snapshotShards = shardSnapshots.get(entry.snapshot());
                    if (snapshotShards != null) {
                        for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shard : entry.shards()) {
                            IndexShardSnapshotStatus snapshotStatus = snapshotShards.shards.get(shard.key);
                            if (snapshotStatus != null) {
                                switch (snapshotStatus.stage()) {
                                    case INIT:
                                    case STARTED:
                                        snapshotStatus.abort();
                                        break;
                                    case FINALIZE:
                                        logger.debug("[{}] trying to cancel snapshot on shard [{}] that is finalizing, letting it finish", entry.snapshot(), shard.key);
                                        break;
                                    case DONE:
                                        logger.debug("[{}] trying to cancel snapshot on the shard [{}] that is already done, updating status on the master", entry.snapshot(), shard.key);
                                        updateIndexShardSnapshotStatus(entry.snapshot(), shard.key,
                                                new ShardSnapshotStatus(localNodeId, State.SUCCESS), masterNode);
                                        break;
                                    case FAILURE:
                                        logger.debug("[{}] trying to cancel snapshot on the shard [{}] that has already failed, updating status on the master", entry.snapshot(), shard.key);
                                        updateIndexShardSnapshotStatus(entry.snapshot(), shard.key,
                                            new ShardSnapshotStatus(localNodeId, State.FAILED, snapshotStatus.failure()), masterNode);
                                        break;
                                    default:
                                        throw new IllegalStateException("Unknown snapshot shard stage " + snapshotStatus.stage());
                                }
                            }
                        }
                    }
                }
            }
        }

        // Update the list of snapshots that we saw and tried to started
        // If startup of these shards fails later, we don't want to try starting these shards again
        shutdownLock.lock();
        try {
            shardSnapshots = unmodifiableMap(survivors);
            if (shardSnapshots.isEmpty()) {
                // Notify all waiting threads that no more snapshots
                shutdownCondition.signalAll();
            }
        } finally {
            shutdownLock.unlock();
        }

        // We have new shards to starts
        if (newSnapshots.isEmpty() == false) {
            Executor executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);
            for (final Map.Entry<Snapshot, Map<ShardId, IndexShardSnapshotStatus>> entry : newSnapshots.entrySet()) {
                Map<String, IndexId> indicesMap = snapshotIndices.get(entry.getKey());
                assert indicesMap != null;
                for (final Map.Entry<ShardId, IndexShardSnapshotStatus> shardEntry : entry.getValue().entrySet()) {
                    final ShardId shardId = shardEntry.getKey();
                    try {
                        final IndexShard indexShard = indicesService.indexServiceSafe(shardId.getIndex()).getShardOrNull(shardId.id());
                        final IndexId indexId = indicesMap.get(shardId.getIndexName());
                        assert indexId != null;
                        executor.execute(new AbstractRunnable() {
                            @Override
                            public void doRun() {
                                snapshot(indexShard, entry.getKey(), indexId, shardEntry.getValue());
                                updateIndexShardSnapshotStatus(entry.getKey(), shardId,
                                    new ShardSnapshotStatus(localNodeId, State.SUCCESS), masterNode);
                            }

                            @Override
                            public void onFailure(Exception e) {
                                logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}] [{}] failed to create snapshot", shardId, entry.getKey()), e);
                                updateIndexShardSnapshotStatus(entry.getKey(), shardId,
                                    new ShardSnapshotStatus(localNodeId, State.FAILED, ExceptionsHelper.detailedMessage(e)), masterNode);
                            }

                        });
                    } catch (Exception e) {
                        updateIndexShardSnapshotStatus(entry.getKey(), shardId,
                            new ShardSnapshotStatus(localNodeId, State.FAILED, ExceptionsHelper.detailedMessage(e)), masterNode);
                    }
                }
            }
        }
    }

    /**
     * Creates shard snapshot
     *
     * @param snapshot       snapshot
     * @param snapshotStatus snapshot status
     */
    private void snapshot(final IndexShard indexShard, final Snapshot snapshot, final IndexId indexId, final IndexShardSnapshotStatus snapshotStatus) {
        Repository repository = snapshotsService.getRepositoriesService().repository(snapshot.getRepository());
        ShardId shardId = indexShard.shardId();
        if (!indexShard.routingEntry().primary()) {
            throw new IndexShardSnapshotFailedException(shardId, "snapshot should be performed only on primary");
        }
        if (indexShard.routingEntry().relocating()) {
            // do not snapshot when in the process of relocation of primaries so we won't get conflicts
            throw new IndexShardSnapshotFailedException(shardId, "cannot snapshot while relocating");
        }
        if (indexShard.state() == IndexShardState.CREATED || indexShard.state() == IndexShardState.RECOVERING) {
            // shard has just been created, or still recovering
            throw new IndexShardSnapshotFailedException(shardId, "shard didn't fully recover yet");
        }

        try {
            // we flush first to make sure we get the latest writes snapshotted
            try (Engine.IndexCommitRef snapshotRef = indexShard.acquireIndexCommit(true)) {
                repository.snapshotShard(indexShard, snapshot.getSnapshotId(), indexId, snapshotRef.getIndexCommit(), snapshotStatus);
                if (logger.isDebugEnabled()) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("    index    : version [").append(snapshotStatus.indexVersion()).append("], number_of_files [").append(snapshotStatus.numberOfFiles()).append("] with total_size [").append(new ByteSizeValue(snapshotStatus.totalSize())).append("]\n");
                    logger.debug("snapshot ({}) completed to {}, took [{}]\n{}", snapshot, repository,
                        TimeValue.timeValueMillis(snapshotStatus.time()), sb);
                }
            }
        } catch (SnapshotFailedEngineException e) {
            throw e;
        } catch (IndexShardSnapshotFailedException e) {
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
        final String localNodeId = event.state().nodes().getLocalNodeId();
        final DiscoveryNode masterNode = event.state().nodes().getMasterNode();
        for (SnapshotsInProgress.Entry snapshot : snapshotsInProgress.entries()) {
            if (snapshot.state() == State.STARTED || snapshot.state() == State.ABORTED) {
                Map<ShardId, IndexShardSnapshotStatus> localShards = currentSnapshotShards(snapshot.snapshot());
                if (localShards != null) {
                    ImmutableOpenMap<ShardId, ShardSnapshotStatus> masterShards = snapshot.shards();
                    for(Map.Entry<ShardId, IndexShardSnapshotStatus> localShard : localShards.entrySet()) {
                        ShardId shardId = localShard.getKey();
                        IndexShardSnapshotStatus localShardStatus = localShard.getValue();
                        ShardSnapshotStatus masterShard = masterShards.get(shardId);
                        if (masterShard != null && masterShard.state().completed() == false) {
                            // Master knows about the shard and thinks it has not completed
                            if (localShardStatus.stage() == Stage.DONE) {
                                // but we think the shard is done - we need to make new master know that the shard is done
                                logger.debug("[{}] new master thinks the shard [{}] is not completed but the shard is done locally, updating status on the master", snapshot.snapshot(), shardId);
                                updateIndexShardSnapshotStatus(snapshot.snapshot(), shardId,
                                        new ShardSnapshotStatus(localNodeId, State.SUCCESS), masterNode);
                            } else if (localShard.getValue().stage() == Stage.FAILURE) {
                                // but we think the shard failed - we need to make new master know that the shard failed
                                logger.debug("[{}] new master thinks the shard [{}] is not completed but the shard failed locally, updating status on master", snapshot.snapshot(), shardId);
                                updateIndexShardSnapshotStatus(snapshot.snapshot(), shardId,
                                        new ShardSnapshotStatus(localNodeId, State.FAILED, localShardStatus.failure()), masterNode);

                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Stores the list of shards that has to be snapshotted on this node
     */
    private static class SnapshotShards {
        private final Map<ShardId, IndexShardSnapshotStatus> shards;

        private SnapshotShards(Map<ShardId, IndexShardSnapshotStatus> shards) {
            this.shards = shards;
        }
    }


    /**
     * Internal request that is used to send changes in snapshot status to master
     */
    public static class UpdateIndexShardSnapshotStatusRequest extends TransportRequest {
        private Snapshot snapshot;
        private ShardId shardId;
        private ShardSnapshotStatus status;

        public UpdateIndexShardSnapshotStatusRequest() {

        }

        public UpdateIndexShardSnapshotStatusRequest(Snapshot snapshot, ShardId shardId, ShardSnapshotStatus status) {
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

    /**
     * Updates the shard status
     */
    public void updateIndexShardSnapshotStatus(Snapshot snapshot, ShardId shardId, ShardSnapshotStatus status, DiscoveryNode master) {
        UpdateIndexShardSnapshotStatusRequest request = new UpdateIndexShardSnapshotStatusRequest(snapshot, shardId, status);
        try {
            transportService.sendRequest(master, UPDATE_SNAPSHOT_ACTION_NAME, request, EmptyTransportResponseHandler.INSTANCE_SAME);
        } catch (Exception e) {
            logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}] [{}] failed to update snapshot state", request.snapshot(), request.status()), e);
        }
    }

    /**
     * Updates the shard status on master node
     *
     * @param request update shard status request
     */
    private void innerUpdateSnapshotState(final UpdateIndexShardSnapshotStatusRequest request) {
        logger.trace("received updated snapshot restore state [{}]", request);
        clusterService.submitStateUpdateTask(
            "update snapshot state",
            request,
            ClusterStateTaskConfig.build(Priority.NORMAL),
            snapshotStateExecutor,
            (source, e) -> logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}][{}] failed to update snapshot status to [{}]",
                request.snapshot(), request.shardId(), request.status()), e));
    }

    class SnapshotStateExecutor implements ClusterStateTaskExecutor<UpdateIndexShardSnapshotStatusRequest> {

        @Override
        public ClusterTasksResult<UpdateIndexShardSnapshotStatusRequest> execute(ClusterState currentState, List<UpdateIndexShardSnapshotStatusRequest> tasks) throws Exception {
            final SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
            if (snapshots != null) {
                int changedCount = 0;
                final List<SnapshotsInProgress.Entry> entries = new ArrayList<>();
                for (SnapshotsInProgress.Entry entry : snapshots.entries()) {
                    ImmutableOpenMap.Builder<ShardId, ShardSnapshotStatus> shards = ImmutableOpenMap.builder();
                    boolean updated = false;

                    for (UpdateIndexShardSnapshotStatusRequest updateSnapshotState : tasks) {
                        if (entry.snapshot().equals(updateSnapshotState.snapshot())) {
                            logger.trace("[{}] Updating shard [{}] with status [{}]", updateSnapshotState.snapshot(), updateSnapshotState.shardId(), updateSnapshotState.status().state());
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
                            // Finalize snapshot in the repository
                            snapshotsService.endSnapshot(updatedEntry);
                            logger.info("snapshot [{}] is done", updatedEntry.snapshot());
                        }
                    } else {
                        entries.add(entry);
                    }
                }
                if (changedCount > 0) {
                    logger.trace("changed cluster state triggered by {} snapshot state updates", changedCount);

                    final SnapshotsInProgress updatedSnapshots = new SnapshotsInProgress(entries.toArray(new SnapshotsInProgress.Entry[entries.size()]));
                    return ClusterTasksResult.<UpdateIndexShardSnapshotStatusRequest>builder().successes(tasks).build(
                        ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE, updatedSnapshots).build());
                }
            }
            return ClusterTasksResult.<UpdateIndexShardSnapshotStatusRequest>builder().successes(tasks).build(currentState);
        }
    }

    /**
     * Transport request handler that is used to send changes in snapshot status to master
     */
    class UpdateSnapshotStateRequestHandler implements TransportRequestHandler<UpdateIndexShardSnapshotStatusRequest> {
        @Override
        public void messageReceived(UpdateIndexShardSnapshotStatusRequest request, final TransportChannel channel) throws Exception {
            innerUpdateSnapshotState(request);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

}
