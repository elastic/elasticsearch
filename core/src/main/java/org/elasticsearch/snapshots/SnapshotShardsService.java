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
import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.engine.SnapshotFailedEngineException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardRepository;
import org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.indices.IndicesService;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.cluster.SnapshotsInProgress.completed;

/**
 * This service runs on data and master nodes and controls currently snapshotted shards on these nodes. It is responsible for
 * starting and stopping shard level snapshots
 */
public class SnapshotShardsService extends AbstractLifecycleComponent<SnapshotShardsService> implements ClusterStateListener {

    public static final String UPDATE_SNAPSHOT_ACTION_NAME = "internal:cluster/snapshot/update_snapshot";

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final SnapshotsService snapshotsService;

    private final TransportService transportService;

    private final ThreadPool threadPool;

    private final Lock shutdownLock = new ReentrantLock();

    private final Condition shutdownCondition = shutdownLock.newCondition();

    private volatile Map<Snapshot, SnapshotShards> shardSnapshots = emptyMap();

    private final BlockingQueue<UpdateIndexShardSnapshotStatusRequest> updatedSnapshotStateQueue = ConcurrentCollections.newBlockingQueue();


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
            // addLast to make sure that Repository will be created before snapshot
            clusterService.addLast(this);
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
        clusterService.remove(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        try {
            SnapshotsInProgress prev = event.previousState().custom(SnapshotsInProgress.TYPE);
            SnapshotsInProgress curr = event.state().custom(SnapshotsInProgress.TYPE);

            if (prev == null) {
                if (curr != null) {
                    processIndexShardSnapshots(event);
                }
            } else if (prev.equals(curr) == false) {
                    processIndexShardSnapshots(event);
            }
            String masterNodeId = event.state().nodes().getMasterNodeId();
            if (masterNodeId != null && masterNodeId.equals(event.previousState().nodes().getMasterNodeId()) == false) {
                syncShardStatsOnNewMaster(event);
            }

        } catch (Throwable t) {
            logger.warn("Failed to update snapshot state ", t);
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
            }
        }

        // For now we will be mostly dealing with a single snapshot at a time but might have multiple simultaneously running
        // snapshots in the future
        Map<Snapshot, Map<ShardId, IndexShardSnapshotStatus>> newSnapshots = new HashMap<>();
        // Now go through all snapshots and update existing or create missing
        final String localNodeId = clusterService.localNode().getId();
        if (snapshotsInProgress != null) {
            for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
                if (entry.state() == SnapshotsInProgress.State.STARTED) {
                    Map<ShardId, IndexShardSnapshotStatus> startedShards = new HashMap<>();
                    SnapshotShards snapshotShards = shardSnapshots.get(entry.snapshot());
                    for (ObjectObjectCursor<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shard : entry.shards()) {
                        // Add all new shards to start processing on
                        if (localNodeId.equals(shard.value.nodeId())) {
                            if (shard.value.state() == SnapshotsInProgress.State.INIT && (snapshotShards == null || !snapshotShards.shards.containsKey(shard.key))) {
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
                } else if (entry.state() == SnapshotsInProgress.State.ABORTED) {
                    // Abort all running shards for this snapshot
                    SnapshotShards snapshotShards = shardSnapshots.get(entry.snapshot());
                    if (snapshotShards != null) {
                        for (ObjectObjectCursor<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shard : entry.shards()) {
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
                                                new SnapshotsInProgress.ShardSnapshotStatus(event.state().nodes().getLocalNodeId(), SnapshotsInProgress.State.SUCCESS));
                                        break;
                                    case FAILURE:
                                        logger.debug("[{}] trying to cancel snapshot on the shard [{}] that has already failed, updating status on the master", entry.snapshot(), shard.key);
                                        updateIndexShardSnapshotStatus(entry.snapshot(), shard.key,
                                                new SnapshotsInProgress.ShardSnapshotStatus(event.state().nodes().getLocalNodeId(), SnapshotsInProgress.State.FAILED, snapshotStatus.failure()));
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
                for (final Map.Entry<ShardId, IndexShardSnapshotStatus> shardEntry : entry.getValue().entrySet()) {
                    final ShardId shardId = shardEntry.getKey();
                    try {
                        final IndexShard indexShard = indicesService.indexServiceSafe(shardId.getIndex()).getShardOrNull(shardId.id());
                        executor.execute(new AbstractRunnable() {
                            @Override
                            public void doRun() {
                                snapshot(indexShard, entry.getKey(), shardEntry.getValue());
                                updateIndexShardSnapshotStatus(entry.getKey(), shardId, new SnapshotsInProgress.ShardSnapshotStatus(localNodeId, SnapshotsInProgress.State.SUCCESS));
                            }

                            @Override
                            public void onFailure(Throwable t) {
                                logger.warn("[{}] [{}] failed to create snapshot", t, shardId, entry.getKey());
                                updateIndexShardSnapshotStatus(entry.getKey(), shardId, new SnapshotsInProgress.ShardSnapshotStatus(localNodeId, SnapshotsInProgress.State.FAILED, ExceptionsHelper.detailedMessage(t)));
                            }

                        });
                    } catch (Throwable t) {
                        updateIndexShardSnapshotStatus(entry.getKey(), shardId, new SnapshotsInProgress.ShardSnapshotStatus(localNodeId, SnapshotsInProgress.State.FAILED, ExceptionsHelper.detailedMessage(t)));
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
    private void snapshot(final IndexShard indexShard, final Snapshot snapshot, final IndexShardSnapshotStatus snapshotStatus) {
        IndexShardRepository indexShardRepository = snapshotsService.getRepositoriesService().indexShardRepository(snapshot.getRepository());
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
            IndexCommit snapshotIndexCommit = indexShard.snapshotIndex(true);
            try {
                indexShardRepository.snapshot(snapshot.getSnapshotId(), shardId, snapshotIndexCommit, snapshotStatus);
                if (logger.isDebugEnabled()) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("    index    : version [").append(snapshotStatus.indexVersion()).append("], number_of_files [").append(snapshotStatus.numberOfFiles()).append("] with total_size [").append(new ByteSizeValue(snapshotStatus.totalSize())).append("]\n");
                    logger.debug("snapshot ({}) completed to {}, took [{}]\n{}", snapshot, indexShardRepository,
                        TimeValue.timeValueMillis(snapshotStatus.time()), sb);
                }
            } finally {
                indexShard.releaseSnapshot(snapshotIndexCommit);
            }
        } catch (SnapshotFailedEngineException e) {
            throw e;
        } catch (IndexShardSnapshotFailedException e) {
            throw e;
        } catch (Throwable e) {
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
        for (SnapshotsInProgress.Entry snapshot : snapshotsInProgress.entries()) {
            if (snapshot.state() == SnapshotsInProgress.State.STARTED || snapshot.state() == SnapshotsInProgress.State.ABORTED) {
                Map<ShardId, IndexShardSnapshotStatus> localShards = currentSnapshotShards(snapshot.snapshot());
                if (localShards != null) {
                    ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> masterShards = snapshot.shards();
                    for(Map.Entry<ShardId, IndexShardSnapshotStatus> localShard : localShards.entrySet()) {
                        ShardId shardId = localShard.getKey();
                        IndexShardSnapshotStatus localShardStatus = localShard.getValue();
                        SnapshotsInProgress.ShardSnapshotStatus masterShard = masterShards.get(shardId);
                        if (masterShard != null && masterShard.state().completed() == false) {
                            // Master knows about the shard and thinks it has not completed
                            if (localShardStatus.stage() == IndexShardSnapshotStatus.Stage.DONE) {
                                // but we think the shard is done - we need to make new master know that the shard is done
                                logger.debug("[{}] new master thinks the shard [{}] is not completed but the shard is done locally, updating status on the master", snapshot.snapshot(), shardId);
                                updateIndexShardSnapshotStatus(snapshot.snapshot(), shardId,
                                        new SnapshotsInProgress.ShardSnapshotStatus(event.state().nodes().getLocalNodeId(), SnapshotsInProgress.State.SUCCESS));
                            } else if (localShard.getValue().stage() == IndexShardSnapshotStatus.Stage.FAILURE) {
                                // but we think the shard failed - we need to make new master know that the shard failed
                                logger.debug("[{}] new master thinks the shard [{}] is not completed but the shard failed locally, updating status on master", snapshot.snapshot(), shardId);
                                updateIndexShardSnapshotStatus(snapshot.snapshot(), shardId,
                                        new SnapshotsInProgress.ShardSnapshotStatus(event.state().nodes().getLocalNodeId(), SnapshotsInProgress.State.FAILED, localShardStatus.failure()));

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
        private SnapshotsInProgress.ShardSnapshotStatus status;

        private volatile boolean processed; // state field, no need to serialize

        public UpdateIndexShardSnapshotStatusRequest() {

        }

        public UpdateIndexShardSnapshotStatusRequest(Snapshot snapshot, ShardId shardId, SnapshotsInProgress.ShardSnapshotStatus status) {
            this.snapshot = snapshot;
            this.shardId = shardId;
            this.status = status;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            snapshot = new Snapshot(in);
            shardId = ShardId.readShardId(in);
            status = SnapshotsInProgress.ShardSnapshotStatus.readShardSnapshotStatus(in);
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

        public SnapshotsInProgress.ShardSnapshotStatus status() {
            return status;
        }

        @Override
        public String toString() {
            return "" + snapshot + ", shardId [" + shardId + "], status [" + status.state() + "]";
        }

        public void markAsProcessed() {
            processed = true;
        }

        public boolean isProcessed() {
            return processed;
        }
    }

    /**
     * Updates the shard status
     */
    public void updateIndexShardSnapshotStatus(Snapshot snapshot, ShardId shardId, SnapshotsInProgress.ShardSnapshotStatus status) {
        UpdateIndexShardSnapshotStatusRequest request = new UpdateIndexShardSnapshotStatusRequest(snapshot, shardId, status);
        try {
            if (clusterService.state().nodes().isLocalNodeElectedMaster()) {
                innerUpdateSnapshotState(request);
            } else {
                transportService.sendRequest(clusterService.state().nodes().getMasterNode(),
                        UPDATE_SNAPSHOT_ACTION_NAME, request, EmptyTransportResponseHandler.INSTANCE_SAME);
            }
        } catch (Throwable t) {
            logger.warn("[{}] [{}] failed to update snapshot state", t, request.snapshot(), request.status());
        }
    }

    /**
     * Updates the shard status on master node
     *
     * @param request update shard status request
     */
    private void innerUpdateSnapshotState(final UpdateIndexShardSnapshotStatusRequest request) {
        logger.trace("received updated snapshot restore state [{}]", request);
        updatedSnapshotStateQueue.add(request);

        clusterService.submitStateUpdateTask("update snapshot state", new ClusterStateUpdateTask() {
            private final List<UpdateIndexShardSnapshotStatusRequest> drainedRequests = new ArrayList<>();

            @Override
            public ClusterState execute(ClusterState currentState) {
                // The request was already processed as a part of an early batch - skipping
                if (request.isProcessed()) {
                    return currentState;
                }

                updatedSnapshotStateQueue.drainTo(drainedRequests);

                final int batchSize = drainedRequests.size();

                // nothing to process (a previous event has processed it already)
                if (batchSize == 0) {
                    return currentState;
                }

                final SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                if (snapshots != null) {
                    int changedCount = 0;
                    final List<SnapshotsInProgress.Entry> entries = new ArrayList<>();
                    for (SnapshotsInProgress.Entry entry : snapshots.entries()) {
                        ImmutableOpenMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards = ImmutableOpenMap.builder();
                        boolean updated = false;

                        for (int i = 0; i < batchSize; i++) {
                            final UpdateIndexShardSnapshotStatusRequest updateSnapshotState = drainedRequests.get(i);
                            updateSnapshotState.markAsProcessed();

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
                                SnapshotsInProgress.Entry updatedEntry = new SnapshotsInProgress.Entry(entry, SnapshotsInProgress.State.SUCCESS, shards.build());
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
                        return ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE, updatedSnapshots).build();
                    }
                }
                return currentState;
            }

            @Override
            public void onFailure(String source, Throwable t) {
                for (UpdateIndexShardSnapshotStatusRequest request : drainedRequests) {
                    logger.warn("[{}][{}] failed to update snapshot status to [{}]", t, request.snapshot(), request.shardId(), request.status());
                }
            }
        });
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
