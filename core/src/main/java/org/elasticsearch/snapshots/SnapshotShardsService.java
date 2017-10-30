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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus;
import org.elasticsearch.cluster.SnapshotsInProgress.State;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
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

import java.util.HashMap;
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

/**
 * This service runs on data and master nodes and controls currently snapshotted shards on these nodes. It is responsible for
 * starting and stopping shard level snapshots
 */
public class SnapshotShardsService extends AbstractLifecycleComponent implements ClusterStateListener, IndexEventListener {

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final SnapshotsService snapshotsService;

    private final ThreadPool threadPool;

    private final Lock shutdownLock = new ReentrantLock();

    private final Condition shutdownCondition = shutdownLock.newCondition();

    private volatile Map<Snapshot, SnapshotShards> shardSnapshots = emptyMap();
    private final Client client;

    @Inject
    public SnapshotShardsService(Settings settings, ClusterService clusterService, SnapshotsService snapshotsService,
                                 ThreadPool threadPool, IndicesService indicesService, Client client) {
        super(settings);
        this.indicesService = indicesService;
        this.snapshotsService = snapshotsService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.client = client;
        if (DiscoveryNode.isDataNode(settings)) {
            // this is only useful on the nodes that can hold data.
            clusterService.addListener(this);
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
        clusterService.removeListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
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
                                                new ShardSnapshotStatus(localNodeId, State.SUCCESS));
                                        break;
                                    case FAILURE:
                                        logger.debug("[{}] trying to cancel snapshot on the shard [{}] that has already failed, updating status on the master", entry.snapshot(), shard.key);
                                        updateIndexShardSnapshotStatus(entry.snapshot(), shard.key,
                                            new ShardSnapshotStatus(localNodeId, State.FAILED, snapshotStatus.failure()));
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
                                    new ShardSnapshotStatus(localNodeId, State.SUCCESS));
                            }

                            @Override
                            public void onFailure(Exception e) {
                                logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}] [{}] failed to create snapshot", shardId, entry.getKey()), e);
                                updateIndexShardSnapshotStatus(entry.getKey(), shardId,
                                    new ShardSnapshotStatus(localNodeId, State.FAILED, ExceptionsHelper.detailedMessage(e)));
                            }

                        });
                    } catch (Exception e) {
                        updateIndexShardSnapshotStatus(entry.getKey(), shardId,
                            new ShardSnapshotStatus(localNodeId, State.FAILED, ExceptionsHelper.detailedMessage(e)));
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
        final String localNodeId = event.state().nodes().getLocalNodeId();
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
                                        new ShardSnapshotStatus(localNodeId, State.SUCCESS));
                            } else if (localShard.getValue().stage() == Stage.FAILURE) {
                                // but we think the shard failed - we need to make new master know that the shard failed
                                logger.debug("[{}] new master thinks the shard [{}] is not completed but the shard failed locally, updating status on master", snapshot.snapshot(), shardId);
                                updateIndexShardSnapshotStatus(snapshot.snapshot(), shardId,
                                        new ShardSnapshotStatus(localNodeId, State.FAILED, localShardStatus.failure()));

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

    void updateIndexShardSnapshotStatus(Snapshot snapshot, ShardId shardId, ShardSnapshotStatus status) {
        final UpdateSnapshotStatusRequest request = new UpdateSnapshotStatusRequest(snapshot, shardId, status);
        client.admin().cluster().execute(UpdateSnapshotStatusAction.INSTANCE, request, new ActionListener<UpdateSnapshotStatusResponse>() {
            @Override
            public void onResponse(UpdateSnapshotStatusResponse updateSnapshotStatusResponse) {
                logger.debug((Supplier<?>) () -> new ParameterizedMessage("snapshot state is updated on the master [{}]", request));
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to send/update a snapshot state [{}]", request), e);
            }
        });
    }
}
