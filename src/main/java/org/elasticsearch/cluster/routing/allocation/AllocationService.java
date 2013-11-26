/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.cluster.routing.allocation;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocators;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;

/**
 * This service manages the node allocation of a cluster. For this reason the
 * {@link AllocationService} keeps {@link AllocationDeciders} to choose nodes
 * for shard allocation. This class also manages new nodes joining the cluster
 * and rerouting of shards.
 */
public class AllocationService extends AbstractComponent {

    private final AllocationDeciders allocationDeciders;
    private final ClusterInfoService clusterInfoService;
    private final ShardsAllocators shardsAllocators;

    public AllocationService() {
        this(ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    public AllocationService(Settings settings) {
        this(settings,
                new AllocationDeciders(settings, new NodeSettingsService(ImmutableSettings.Builder.EMPTY_SETTINGS)),
                new ShardsAllocators(settings), ClusterInfoService.EMPTY);
    }

    @Inject
    public AllocationService(Settings settings, AllocationDeciders allocationDeciders, ShardsAllocators shardsAllocators, ClusterInfoService clusterInfoService) {
        super(settings);
        this.allocationDeciders = allocationDeciders;
        this.shardsAllocators = shardsAllocators;
        this.clusterInfoService = clusterInfoService;
    }

    /**
     * Applies the started shards. Note, shards can be called several times within this method.
     * <p/>
     * <p>If the same instance of the routing table is returned, then no change has been made.</p>
     */
    public RoutingAllocation.Result applyStartedShards(ClusterState clusterState, List<? extends ShardRouting> startedShards) {
        return applyStartedShards(clusterState, startedShards, true);
    }

    public RoutingAllocation.Result applyStartedShards(ClusterState clusterState, List<? extends ShardRouting> startedShards, boolean withReroute) {
        RoutingNodes routingNodes = clusterState.routingNodes();
        // shuffle the unassigned nodes, just so we won't have things like poison failed shards
        Collections.shuffle(routingNodes.unassigned());
        StartedRerouteAllocation allocation = new StartedRerouteAllocation(allocationDeciders, routingNodes, clusterState.nodes(), startedShards, clusterInfoService.getClusterInfo());
        boolean changed = applyStartedShards(routingNodes, startedShards);
        if (!changed) {
            return new RoutingAllocation.Result(false, clusterState.routingTable(), allocation.explanation());
        }
        shardsAllocators.applyStartedShards(allocation);
        if (withReroute) {
            reroute(allocation);
        }
        return new RoutingAllocation.Result(true, new RoutingTable.Builder().updateNodes(routingNodes).build().validateRaiseException(clusterState.metaData()), allocation.explanation());
    }

    public RoutingAllocation.Result applyFailedShard(ClusterState clusterState, ShardRouting failedShard) {
        return applyFailedShards(clusterState, ImmutableList.of(failedShard));
    }

    /**
     * Applies the failed shards. Note, shards can be called several times within this method.
     * <p/>
     * <p>If the same instance of the routing table is returned, then no change has been made.</p>
     */
    public RoutingAllocation.Result applyFailedShards(ClusterState clusterState, List<ShardRouting> failedShards) {
        RoutingNodes routingNodes = clusterState.routingNodes();
        // shuffle the unassigned nodes, just so we won't have things like poison failed shards
        Collections.shuffle(routingNodes.unassigned());
        FailedRerouteAllocation allocation = new FailedRerouteAllocation(allocationDeciders, routingNodes, clusterState.nodes(), failedShards, clusterInfoService.getClusterInfo());
        boolean changed = false;
        for (ShardRouting failedShard : failedShards) {
            changed |= applyFailedShard(allocation, failedShard, true);
        }
        if (!changed) {
            return new RoutingAllocation.Result(false, clusterState.routingTable(), allocation.explanation());
        }
        shardsAllocators.applyFailedShards(allocation);
        reroute(allocation);
        return new RoutingAllocation.Result(true, new RoutingTable.Builder().updateNodes(routingNodes).build().validateRaiseException(clusterState.metaData()), allocation.explanation());
    }

    public RoutingAllocation.Result reroute(ClusterState clusterState, AllocationCommands commands) throws ElasticSearchException {
        RoutingNodes routingNodes = clusterState.routingNodes();
        // we don't shuffle the unassigned shards here, to try and get as close as possible to
        // a consistent result of the effect the commands have on the routing
        // this allows systems to dry run the commands, see the resulting cluster state, and act on it
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, routingNodes, clusterState.nodes(), clusterInfoService.getClusterInfo());
        // we ignore disable allocation, because commands are explicit
        allocation.ignoreDisable(true);
        commands.execute(allocation);
        // we revert the ignore disable flag, since when rerouting, we want the original setting to take place
        allocation.ignoreDisable(false);
        // the assumption is that commands will move / act on shards (or fail through exceptions)
        // so, there will always be shard "movements", so no need to check on reroute
        reroute(allocation);
        return new RoutingAllocation.Result(true, new RoutingTable.Builder().updateNodes(routingNodes).build().validateRaiseException(clusterState.metaData()), allocation.explanation());
    }

    /**
     * Reroutes the routing table based on the live nodes.
     * <p/>
     * <p>If the same instance of the routing table is returned, then no change has been made.
     */
    public RoutingAllocation.Result reroute(ClusterState clusterState) {
        RoutingNodes routingNodes = clusterState.routingNodes();
        // shuffle the unassigned nodes, just so we won't have things like poison failed shards
        Collections.shuffle(routingNodes.unassigned());
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, routingNodes, clusterState.nodes(), clusterInfoService.getClusterInfo());
        if (!reroute(allocation)) {
            return new RoutingAllocation.Result(false, clusterState.routingTable(), allocation.explanation());
        }
        return new RoutingAllocation.Result(true, new RoutingTable.Builder().updateNodes(routingNodes).build().validateRaiseException(clusterState.metaData()), allocation.explanation());
    }

    /**
     * Only handles reroute but *without* any reassignment of unassigned shards or rebalancing. Does
     * make sure to handle removed nodes, but only moved the shards to UNASSIGNED, does not reassign
     * them.
     */
    public RoutingAllocation.Result rerouteWithNoReassign(ClusterState clusterState) {
        RoutingNodes routingNodes = clusterState.routingNodes();
        // shuffle the unassigned nodes, just so we won't have things like poison failed shards
        Collections.shuffle(routingNodes.unassigned());
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, routingNodes, clusterState.nodes(), clusterInfoService.getClusterInfo());
        boolean changed = false;
        // first, clear from the shards any node id they used to belong to that is now dead
        changed |= deassociateDeadNodes(allocation);

        // create a sorted list of from nodes with least number of shards to the maximum ones
        applyNewNodes(allocation);

        // elect primaries *before* allocating unassigned, so backups of primaries that failed
        // will be moved to primary state and not wait for primaries to be allocated and recovered (*from gateway*)
        changed |= electPrimariesAndUnassignDanglingReplicas(allocation);

        if (!changed) {
            return new RoutingAllocation.Result(false, clusterState.routingTable(), allocation.explanation());
        }
        return new RoutingAllocation.Result(true, new RoutingTable.Builder().updateNodes(routingNodes).build().validateRaiseException(clusterState.metaData()), allocation.explanation());
    }

    private boolean reroute(RoutingAllocation allocation) {
        boolean changed = false;
        // first, clear from the shards any node id they used to belong to that is now dead
        changed |= deassociateDeadNodes(allocation);

        // create a sorted list of from nodes with least number of shards to the maximum ones
        applyNewNodes(allocation);

        // elect primaries *before* allocating unassigned, so backups of primaries that failed
        // will be moved to primary state and not wait for primaries to be allocated and recovered (*from gateway*)
        changed |= electPrimariesAndUnassignDanglingReplicas(allocation);

        // now allocate all the unassigned to available nodes
        if (allocation.routingNodes().hasUnassigned()) {
            changed |= shardsAllocators.allocateUnassigned(allocation);
            // elect primaries again, in case this is needed with unassigned allocation
            changed |= electPrimariesAndUnassignDanglingReplicas(allocation);
        }

        // move shards that no longer can be allocated
        changed |= moveShards(allocation);

        // rebalance
        changed |= shardsAllocators.rebalance(allocation);

        return changed;
    }

    private boolean moveShards(RoutingAllocation allocation) {
        boolean changed = false;

        // create a copy of the shards interleaving between nodes, and check if they can remain
        List<MutableShardRouting> shards = new ArrayList<MutableShardRouting>();
        int index = 0;
        boolean found = true;
        while (found) {
            found = false;
            for (RoutingNode routingNode : allocation.routingNodes()) {
                if (index >= routingNode.shards().size()) {
                    continue;
                }
                found = true;
                shards.add(routingNode.shards().get(index));
            }
            index++;
        }
        for (int i = 0; i < shards.size(); i++) {
            MutableShardRouting shardRouting = shards.get(i);
            // we can only move started shards...
            if (!shardRouting.started()) {
                continue;
            }
            RoutingNode routingNode = allocation.routingNodes().node(shardRouting.currentNodeId());
            Decision decision = allocation.deciders().canRemain(shardRouting, routingNode, allocation);
            if (decision.type() == Decision.Type.NO) {
                logger.debug("[{}][{}] allocated on [{}], but can no longer be allocated on it, moving...", shardRouting.index(), shardRouting.id(), routingNode.node());
                boolean moved = shardsAllocators.move(shardRouting, routingNode, allocation);
                if (!moved) {
                    logger.debug("[{}][{}] can't move", shardRouting.index(), shardRouting.id());
                } else {
                    changed = true;
                }
            }
        }
        return changed;
    }

    private boolean electPrimariesAndUnassignDanglingReplicas(RoutingAllocation allocation) {
        boolean changed = false;
        RoutingNodes routingNodes = allocation.routingNodes();
        for (MutableShardRouting shardEntry : routingNodes.unassigned()) {
            if (shardEntry.primary() && !shardEntry.assignedToNode()) {
                boolean elected = false;
                // primary and not assigned, go over and find a replica that is assigned and active (since it might be relocating)
                for (RoutingNode routingNode : routingNodes.nodesToShards().values()) {

                    for (MutableShardRouting shardEntry2 : routingNode.shards()) {
                        if (shardEntry.shardId().equals(shardEntry2.shardId()) && shardEntry2.active()) {
                            assert shardEntry2.assignedToNode();
                            assert !shardEntry2.primary();

                            changed = true;
                            shardEntry.moveFromPrimary();
                            shardEntry2.moveToPrimary();

                            if (shardEntry2.relocatingNodeId() != null) {
                                // its also relocating, make sure to move the other routing to primary
                                RoutingNode node = routingNodes.node(shardEntry2.relocatingNodeId());
                                if (node != null) {
                                    for (MutableShardRouting shardRouting : node) {
                                        if (shardRouting.shardId().equals(shardEntry2.shardId()) && !shardRouting.primary()) {
                                            shardRouting.moveToPrimary();
                                            break;
                                        }
                                    }
                                }
                            }

                            elected = true;
                            break;
                        }
                    }

                    if (elected) {
                        break;
                    }
                }
            }
        }

        // go over and remove dangling replicas that are initializing, but we couldn't elect primary ones...
        List<ShardRouting> shardsToFail = null;
        for (MutableShardRouting shardEntry : routingNodes.unassigned()) {
            if (shardEntry.primary() && !shardEntry.assignedToNode()) {
                for (RoutingNode routingNode : routingNodes.nodesToShards().values()) {
                    for (MutableShardRouting shardEntry2 : routingNode.shards()) {
                        if (shardEntry.shardId().equals(shardEntry2.shardId()) && !shardEntry2.active()) {
                            changed = true;
                            if (shardsToFail == null) {
                                shardsToFail = new ArrayList<ShardRouting>();
                            }
                            shardsToFail.add(shardEntry2);
                        }
                    }
                }
            }
        }
        if (shardsToFail != null) {
            for (ShardRouting shardToFail : shardsToFail) {
                applyFailedShard(allocation, shardToFail, false);
            }
        }
        return changed;
    }

    /**
     * Applies the new nodes to the routing nodes and returns them (just the
     * new nodes);
     */
    private void applyNewNodes(RoutingAllocation allocation) {
        for (ObjectCursor<DiscoveryNode> cursor : allocation.nodes().dataNodes().values()) {
            DiscoveryNode node = cursor.value;
            if (!allocation.routingNodes().nodesToShards().containsKey(node.id())) {
                RoutingNode routingNode = new RoutingNode(node.id(), node);
                allocation.routingNodes().nodesToShards().put(node.id(), routingNode);
            }
        }
    }

    private boolean deassociateDeadNodes(RoutingAllocation allocation) {
        boolean changed = false;
        for (Iterator<RoutingNode> it = allocation.routingNodes().nodesToShards().values().iterator(); it.hasNext(); ) {
            RoutingNode node = it.next();
            if (allocation.nodes().dataNodes().containsKey(node.nodeId())) {
                // its a live node, continue
                continue;
            }
            changed = true;
            // now, go over all the shards routing on the node, and fail them
            for (MutableShardRouting shardRouting : new ArrayList<MutableShardRouting>(node.shards())) {
                applyFailedShard(allocation, shardRouting, false);
            }
            // its a dead node, remove it, note, its important to remove it *after* we apply failed shard
            // since it relies on the fact that the RoutingNode exists in the list of nodes
            it.remove();
        }
        return changed;
    }

    private boolean applyStartedShards(RoutingNodes routingNodes, Iterable<? extends ShardRouting> startedShardEntries) {
        boolean dirty = false;
        // apply shards might be called several times with the same shard, ignore it
        for (ShardRouting startedShard : startedShardEntries) {
            assert startedShard.state() == INITIALIZING;

            // retrieve the relocating node id before calling moveToStarted().
            String relocatingNodeId = null;

            RoutingNode currentRoutingNode = routingNodes.nodesToShards().get(startedShard.currentNodeId());
            if (currentRoutingNode != null) {
                for (MutableShardRouting shard : currentRoutingNode) {
                    if (shard.shardId().equals(startedShard.shardId())) {
                        relocatingNodeId = shard.relocatingNodeId();
                        if (!shard.started()) {
                            dirty = true;
                            shard.moveToStarted();
                        }
                        break;
                    }
                }
            }

            // startedShard is the current state of the shard (post relocation for example)
            // this means that after relocation, the state will be started and the currentNodeId will be
            // the node we relocated to

            if (relocatingNodeId == null) {
                continue;
            }

            RoutingNode sourceRoutingNode = routingNodes.nodesToShards().get(relocatingNodeId);
            if (sourceRoutingNode != null) {
                Iterator<MutableShardRouting> shardsIter = sourceRoutingNode.iterator();
                while (shardsIter.hasNext()) {
                    MutableShardRouting shard = shardsIter.next();
                    if (shard.shardId().equals(startedShard.shardId())) {
                        if (shard.relocating()) {
                            dirty = true;
                            shardsIter.remove();
                            break;
                        }
                    }
                }
            }
        }
        return dirty;
    }

    /**
     * Applies the relevant logic to handle a failed shard. Returns <tt>true</tt> if changes happened that
     * require relocation.
     */
    private boolean applyFailedShard(RoutingAllocation allocation, ShardRouting failedShard, boolean addToIgnoreList) {
        // create a copy of the failed shard, since we assume we can change possible references to it without
        // changing the state of failed shard
        failedShard = new ImmutableShardRouting(failedShard);

        IndexRoutingTable indexRoutingTable = allocation.routingTable().index(failedShard.index());
        if (indexRoutingTable == null) {
            return false;
        }

        if (failedShard.relocatingNodeId() != null) {
            // the shard is relocating, either in initializing (recovery from another node) or relocating (moving to another node)
            if (failedShard.state() == INITIALIZING) {
                // the shard is initializing and recovering from another node
                boolean dirty = false;
                // first, we need to cancel the current node that is being initialized
                RoutingNode initializingNode = allocation.routingNodes().node(failedShard.currentNodeId());
                if (initializingNode != null) {
                    for (Iterator<MutableShardRouting> it = initializingNode.iterator(); it.hasNext(); ) {
                        MutableShardRouting shardRouting = it.next();
                        if (shardRouting.equals(failedShard)) {
                            dirty = true;
                            it.remove();
                            shardRouting.deassignNode();

                            if (addToIgnoreList) {
                                // make sure we ignore this shard on the relevant node
                                allocation.addIgnoreShardForNode(failedShard.shardId(), failedShard.currentNodeId());
                            }

                            break;
                        }
                    }
                }
                if (dirty) {
                    // now, find the node that we are relocating *from*, and cancel its relocation
                    RoutingNode relocatingFromNode = allocation.routingNodes().node(failedShard.relocatingNodeId());
                    if (relocatingFromNode != null) {
                        for (Iterator<MutableShardRouting> it = relocatingFromNode.iterator(); it.hasNext(); ) {
                            MutableShardRouting shardRouting = it.next();
                            if (shardRouting.shardId().equals(failedShard.shardId()) && shardRouting.state() == RELOCATING) {
                                dirty = true;
                                shardRouting.cancelRelocation();
                                break;
                            }
                        }
                    }
                }
                return dirty;
            } else if (failedShard.state() == RELOCATING) {
                boolean dirty = false;
                // the shard is relocating, meaning its the source the shard is relocating from
                // first, we need to cancel the current relocation from the current node
                // now, find the node that we are recovering from, cancel the relocation, remove it from the node
                // and add it to the unassigned shards list...
                RoutingNode relocatingFromNode = allocation.routingNodes().node(failedShard.currentNodeId());
                if (relocatingFromNode != null) {
                    for (Iterator<MutableShardRouting> it = relocatingFromNode.iterator(); it.hasNext(); ) {
                        MutableShardRouting shardRouting = it.next();
                        if (shardRouting.equals(failedShard)) {
                            dirty = true;
                            shardRouting.cancelRelocation();
                            it.remove();
                            if (addToIgnoreList) {
                                // make sure we ignore this shard on the relevant node
                                allocation.addIgnoreShardForNode(failedShard.shardId(), failedShard.currentNodeId());
                            }

                            allocation.routingNodes().unassigned().add(new MutableShardRouting(failedShard.index(), failedShard.id(),
                                    null, failedShard.primary(), ShardRoutingState.UNASSIGNED, failedShard.version() + 1));
                            break;
                        }
                    }
                }
                if (dirty) {
                    // next, we need to find the target initializing shard that is recovering from, and remove it...
                    RoutingNode initializingNode = allocation.routingNodes().node(failedShard.relocatingNodeId());
                    if (initializingNode != null) {
                        for (Iterator<MutableShardRouting> it = initializingNode.iterator(); it.hasNext(); ) {
                            MutableShardRouting shardRouting = it.next();
                            if (shardRouting.shardId().equals(failedShard.shardId()) && shardRouting.state() == INITIALIZING) {
                                dirty = true;
                                shardRouting.deassignNode();
                                it.remove();
                            }
                        }
                    }
                }
                return dirty;
            } else {
                throw new ElasticSearchIllegalStateException("illegal state for a failed shard, relocating node id is set, but state does not match: " + failedShard);
            }
        } else {
            // the shard is not relocating, its either started, or initializing, just cancel it and move on...
            boolean dirty = false;
            RoutingNode node = allocation.routingNodes().node(failedShard.currentNodeId());
            if (node != null) {
                for (Iterator<MutableShardRouting> it = node.iterator(); it.hasNext(); ) {
                    MutableShardRouting shardRouting = it.next();
                    if (shardRouting.equals(failedShard)) {
                        dirty = true;
                        if (addToIgnoreList) {
                            // make sure we ignore this shard on the relevant node
                            allocation.addIgnoreShardForNode(failedShard.shardId(), failedShard.currentNodeId());
                        }

                        it.remove();

                        // move all the shards matching the failed shard to the end of the unassigned list
                        // so we give a chance for other allocations and won't create poison failed allocations
                        // that can keep other shards from being allocated (because of limits applied on how many
                        // shards we can start per node)
                        List<MutableShardRouting> shardsToMove = Lists.newArrayList();
                        for (Iterator<MutableShardRouting> unassignedIt = allocation.routingNodes().unassigned().iterator(); unassignedIt.hasNext(); ) {
                            MutableShardRouting unassignedShardRouting = unassignedIt.next();
                            if (unassignedShardRouting.shardId().equals(failedShard.shardId())) {
                                unassignedIt.remove();
                                shardsToMove.add(unassignedShardRouting);
                            }
                        }
                        if (!shardsToMove.isEmpty()) {
                            allocation.routingNodes().unassigned().addAll(shardsToMove);
                        }

                        allocation.routingNodes().unassigned().add(new MutableShardRouting(failedShard.index(), failedShard.id(),
                                null, failedShard.primary(), ShardRoutingState.UNASSIGNED, failedShard.version() + 1));

                        break;
                    }
                }
            }
            return dirty;
        }
    }
}
