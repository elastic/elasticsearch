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

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.snapshots.SnapshotsShardService.completed;

/**
 * This service runs on the master node, monitors changes in the cluster and updates currently running snapshot shards accordingly.
 */
public class SnapshotsShardWatcherService extends AbstractLifecycleComponent<SnapshotsShardWatcherService> implements ClusterStateListener {


    private final ClusterService clusterService;

    private final SnapshotManager snapshotManager;

    @Inject
    public SnapshotsShardWatcherService(Settings settings, ClusterService clusterService, SnapshotManager snapshotManager) {
        super(settings);
        this.clusterService = clusterService;
        this.snapshotManager = snapshotManager;
        if (DiscoveryNode.masterNode(settings)) {
            // addLast to make sure that Repository will be created before snapshot
            clusterService.addLast(this);
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        try {
            if (event.localNodeMaster()) {
                if (event.nodesRemoved()) {
                    processSnapshotsOnRemovedNodes(event);
                }
                if (event.routingTableChanged()) {
                    processStartedShards(event);
                }
            }
        } catch (Throwable t) {
            logger.warn("Failed to update snapshot state ", t);
        }
    }

    /**
     * Cleans up shard snapshots that were running on removed nodes
     *
     * @param event cluster changed event
     */
    private void processSnapshotsOnRemovedNodes(ClusterChangedEvent event) {
        if (removedNodesCleanupNeeded(event)) {
            // Check if we just became the master
            final boolean newMaster = !event.previousState().nodes().localNodeMaster();
            clusterService.submitStateUpdateTask("update snapshot state after node removal", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    DiscoveryNodes nodes = currentState.nodes();
                    SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                    if (snapshots == null) {
                        return currentState;
                    }
                    boolean changed = false;
                    ArrayList<SnapshotsInProgress.Entry> entries = newArrayList();
                    for (final SnapshotsInProgress.Entry snapshot : snapshots.entries()) {
                        SnapshotsInProgress.Entry updatedSnapshot = snapshot;
                        boolean snapshotChanged = false;
                        if (snapshot.state() == SnapshotsInProgress.State.STARTED || snapshot.state() == SnapshotsInProgress.State.ABORTED) {
                            ImmutableMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards = ImmutableMap.builder();
                            for (ImmutableMap.Entry<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shardEntry : snapshot.shards().entrySet()) {
                                SnapshotsInProgress.ShardSnapshotStatus shardStatus = shardEntry.getValue();
                                if (!shardStatus.state().completed() && shardStatus.nodeId() != null) {
                                    if (nodes.nodeExists(shardStatus.nodeId())) {
                                        shards.put(shardEntry);
                                    } else {
                                        // TODO: Restart snapshot on another node?
                                        snapshotChanged = true;
                                        logger.warn("failing snapshot of shard [{}] on closed node [{}]", shardEntry.getKey(), shardStatus.nodeId());
                                        shards.put(shardEntry.getKey(), new SnapshotsInProgress.ShardSnapshotStatus(shardStatus.nodeId(), SnapshotsInProgress.State.FAILED, "node shutdown"));
                                    }
                                }
                            }
                            if (snapshotChanged) {
                                changed = true;
                                ImmutableMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shardsMap = shards.build();
                                if (!snapshot.state().completed() && completed(shardsMap.values())) {
                                    updatedSnapshot = new SnapshotsInProgress.Entry(snapshot, SnapshotsInProgress.State.SUCCESS, shardsMap);
                                    snapshotManager.endSnapshot(updatedSnapshot);
                                } else {
                                    updatedSnapshot = new SnapshotsInProgress.Entry(snapshot, snapshot.state(), shardsMap);
                                }
                            }
                            entries.add(updatedSnapshot);
                        } else if (snapshot.state() == SnapshotsInProgress.State.INIT && newMaster) {
                            // Clean up the snapshot that failed to start from the old master
                            snapshotManager.deleteSnapshot(snapshot.snapshotId(), new SnapshotsService.DeleteSnapshotListener() {
                                @Override
                                public void onResponse() {
                                    logger.debug("cleaned up abandoned snapshot {} in INIT state", snapshot.snapshotId());
                                }

                                @Override
                                public void onFailure(Throwable t) {
                                    logger.warn("failed to clean up abandoned snapshot {} in INIT state", snapshot.snapshotId());
                                }
                            });
                        } else if (snapshot.state() == SnapshotsInProgress.State.SUCCESS && newMaster) {
                            // Finalize the snapshot
                            snapshotManager.endSnapshot(snapshot);
                        }
                    }
                    if (changed) {
                        snapshots = new SnapshotsInProgress(entries.toArray(new SnapshotsInProgress.Entry[entries.size()]));
                        return ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE, snapshots).build();
                    }
                    return currentState;
                }

                @Override
                public void onFailure(String source, Throwable t) {
                    logger.warn("failed to update snapshot state after node removal");
                }
            });
        }
    }

    private void processStartedShards(ClusterChangedEvent event) {
        if (waitingShardsStartedOrUnassigned(event)) {
            clusterService.submitStateUpdateTask("update snapshot state after shards started", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    RoutingTable routingTable = currentState.routingTable();
                    SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                    if (snapshots != null) {
                        boolean changed = false;
                        ArrayList<SnapshotsInProgress.Entry> entries = newArrayList();
                        for (final SnapshotsInProgress.Entry snapshot : snapshots.entries()) {
                            SnapshotsInProgress.Entry updatedSnapshot = snapshot;
                            if (snapshot.state() == SnapshotsInProgress.State.STARTED) {
                                ImmutableMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards = processWaitingShards(snapshot.shards(), routingTable);
                                if (shards != null) {
                                    changed = true;
                                    if (!snapshot.state().completed() && completed(shards.values())) {
                                        updatedSnapshot = new SnapshotsInProgress.Entry(snapshot, SnapshotsInProgress.State.SUCCESS, shards);
                                        snapshotManager.endSnapshot(updatedSnapshot);
                                    } else {
                                        updatedSnapshot = new SnapshotsInProgress.Entry(snapshot, shards);
                                    }
                                }
                                entries.add(updatedSnapshot);
                            }
                        }
                        if (changed) {
                            snapshots = new SnapshotsInProgress(entries.toArray(new SnapshotsInProgress.Entry[entries.size()]));
                            return ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE, snapshots).build();
                        }
                    }
                    return currentState;
                }

                @Override
                public void onFailure(String source, Throwable t) {
                    logger.warn("failed to update snapshot state after shards started from [{}] ", t, source);
                }
            });
        }
    }

    private ImmutableMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> processWaitingShards(ImmutableMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> snapshotShards, RoutingTable routingTable) {
        boolean snapshotChanged = false;
        ImmutableMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards = ImmutableMap.builder();
        for (ImmutableMap.Entry<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shardEntry : snapshotShards.entrySet()) {
            SnapshotsInProgress.ShardSnapshotStatus shardStatus = shardEntry.getValue();
            if (shardStatus.state() == SnapshotsInProgress.State.WAITING) {
                ShardId shardId = shardEntry.getKey();
                IndexRoutingTable indexShardRoutingTable = routingTable.index(shardId.getIndex());
                if (indexShardRoutingTable != null) {
                    IndexShardRoutingTable shardRouting = indexShardRoutingTable.shard(shardId.id());
                    if (shardRouting != null && shardRouting.primaryShard() != null) {
                        if (shardRouting.primaryShard().started()) {
                            // Shard that we were waiting for has started on a node, let's process it
                            snapshotChanged = true;
                            logger.trace("starting shard that we were waiting for [{}] on node [{}]", shardEntry.getKey(), shardStatus.nodeId());
                            shards.put(shardEntry.getKey(), new SnapshotsInProgress.ShardSnapshotStatus(shardRouting.primaryShard().currentNodeId()));
                            continue;
                        } else if (shardRouting.primaryShard().initializing() || shardRouting.primaryShard().relocating()) {
                            // Shard that we were waiting for hasn't started yet or still relocating - will continue to wait
                            shards.put(shardEntry);
                            continue;
                        }
                    }
                }
                // Shard that we were waiting for went into unassigned state or disappeared - giving up
                snapshotChanged = true;
                logger.warn("failing snapshot of shard [{}] on unassigned shard [{}]", shardEntry.getKey(), shardStatus.nodeId());
                shards.put(shardEntry.getKey(), new SnapshotsInProgress.ShardSnapshotStatus(shardStatus.nodeId(), SnapshotsInProgress.State.FAILED, "shard is unassigned"));
            } else {
                shards.put(shardEntry);
            }
        }
        if (snapshotChanged) {
            return shards.build();
        } else {
            return null;
        }
    }

    private boolean waitingShardsStartedOrUnassigned(ClusterChangedEvent event) {
        SnapshotsInProgress curr = event.state().custom(SnapshotsInProgress.TYPE);
        if (curr != null) {
            for (SnapshotsInProgress.Entry entry : curr.entries()) {
                if (entry.state() == SnapshotsInProgress.State.STARTED && !entry.waitingIndices().isEmpty()) {
                    for (String index : entry.waitingIndices().keySet()) {
                        if (event.indexRoutingTableChanged(index)) {
                            IndexRoutingTable indexShardRoutingTable = event.state().getRoutingTable().index(index);
                            for (ShardId shardId : entry.waitingIndices().get(index)) {
                                ShardRouting shardRouting = indexShardRoutingTable.shard(shardId.id()).primaryShard();
                                if (shardRouting != null && (shardRouting.started() || shardRouting.unassigned())) {
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    private boolean removedNodesCleanupNeeded(ClusterChangedEvent event) {
        // Check if we just became the master
        boolean newMaster = !event.previousState().nodes().localNodeMaster();
        SnapshotsInProgress snapshotsInProgress = event.state().custom(SnapshotsInProgress.TYPE);
        if (snapshotsInProgress == null) {
            return false;
        }
        for (SnapshotsInProgress.Entry snapshot : snapshotsInProgress.entries()) {
            if (newMaster && (snapshot.state() == SnapshotsInProgress.State.SUCCESS || snapshot.state() == SnapshotsInProgress.State.INIT)) {
                // We just replaced old master and snapshots in intermediate states needs to be cleaned
                return true;
            }
            for (DiscoveryNode node : event.nodesDelta().removedNodes()) {
                for (SnapshotsInProgress.ShardSnapshotStatus shardStatus : snapshot.shards().values()) {
                    if (!shardStatus.state().completed() && node.getId().equals(shardStatus.nodeId())) {
                        // At least one shard was running on the removed node - we need to fail it
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() {
        clusterService.remove(this);
    }
}
