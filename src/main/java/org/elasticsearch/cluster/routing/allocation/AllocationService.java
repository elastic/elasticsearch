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

package org.elasticsearch.cluster.routing.allocation;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocators;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
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
        routingNodes.unassigned().shuffle();
        StartedRerouteAllocation allocation = new StartedRerouteAllocation(allocationDeciders, routingNodes, clusterState.nodes(), startedShards, clusterInfoService.getClusterInfo());
        boolean changed = applyStartedShards(routingNodes, startedShards);
        if (!changed) {
            return new RoutingAllocation.Result(false, clusterState.routingTable());
        }
        shardsAllocators.applyStartedShards(allocation);
        if (withReroute) {
            reroute(allocation);
        }
        return new RoutingAllocation.Result(true, new RoutingTable.Builder().updateNodes(routingNodes).build().validateRaiseException(clusterState.metaData()));
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
        routingNodes.unassigned().shuffle();
        FailedRerouteAllocation allocation = new FailedRerouteAllocation(allocationDeciders, routingNodes, clusterState.nodes(), failedShards, clusterInfoService.getClusterInfo());
        boolean changed = false;
        for (ShardRouting failedShard : failedShards) {
            changed |= applyFailedShard(allocation, failedShard, true);
        }
        if (!changed) {
            return new RoutingAllocation.Result(false, clusterState.routingTable());
        }
        shardsAllocators.applyFailedShards(allocation);
        reroute(allocation);
        return new RoutingAllocation.Result(true, new RoutingTable.Builder().updateNodes(routingNodes).build().validateRaiseException(clusterState.metaData()));
    }

    public RoutingAllocation.Result reroute(ClusterState clusterState, AllocationCommands commands) {
        return reroute(clusterState, commands, false);
    }

    public RoutingAllocation.Result reroute(ClusterState clusterState, AllocationCommands commands, boolean explain) throws ElasticsearchException {
        RoutingNodes routingNodes = clusterState.routingNodes();
        // we don't shuffle the unassigned shards here, to try and get as close as possible to
        // a consistent result of the effect the commands have on the routing
        // this allows systems to dry run the commands, see the resulting cluster state, and act on it
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, routingNodes, clusterState.nodes(), clusterInfoService.getClusterInfo());
        // don't short circuit deciders, we want a full explanation
        allocation.debugDecision(true);
        // we ignore disable allocation, because commands are explicit
        allocation.ignoreDisable(true);
        RoutingExplanations explanations = commands.execute(allocation, explain);
        // we revert the ignore disable flag, since when rerouting, we want the original setting to take place
        allocation.ignoreDisable(false);
        // the assumption is that commands will move / act on shards (or fail through exceptions)
        // so, there will always be shard "movements", so no need to check on reroute
        reroute(allocation);
        return new RoutingAllocation.Result(true, new RoutingTable.Builder().updateNodes(routingNodes).build().validateRaiseException(clusterState.metaData()), explanations);
    }

    /**
     * Reroutes the routing table based on the live nodes.
     * <p/>
     * <p>If the same instance of the routing table is returned, then no change has been made.
     */
    public RoutingAllocation.Result reroute(ClusterState clusterState) {
        return reroute(clusterState, false);
    }

    /**
     * Reroutes the routing table based on the live nodes.
     * <p/>
     * <p>If the same instance of the routing table is returned, then no change has been made.
     */
    public RoutingAllocation.Result reroute(ClusterState clusterState, boolean debug) {
        RoutingNodes routingNodes = clusterState.routingNodes();
        // shuffle the unassigned nodes, just so we won't have things like poison failed shards
        routingNodes.unassigned().shuffle();
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, routingNodes, clusterState.nodes(), clusterInfoService.getClusterInfo());
        allocation.debugDecision(debug);
        if (!reroute(allocation)) {
            return new RoutingAllocation.Result(false, clusterState.routingTable());
        }
        return new RoutingAllocation.Result(true, new RoutingTable.Builder().updateNodes(routingNodes).build().validateRaiseException(clusterState.metaData()));
    }

    /**
     * Only handles reroute but *without* any reassignment of unassigned shards or rebalancing. Does
     * make sure to handle removed nodes, but only moved the shards to UNASSIGNED, does not reassign
     * them.
     */
    public RoutingAllocation.Result rerouteWithNoReassign(ClusterState clusterState) {
        return rerouteWithNoReassign(clusterState, false);
    }

    /**
     * Only handles reroute but *without* any reassignment of unassigned shards or rebalancing. Does
     * make sure to handle removed nodes, but only moved the shards to UNASSIGNED, does not reassign
     * them.
     */
    public RoutingAllocation.Result rerouteWithNoReassign(ClusterState clusterState, boolean debug) {
        RoutingNodes routingNodes = clusterState.routingNodes();
        // shuffle the unassigned nodes, just so we won't have things like poison failed shards
        routingNodes.unassigned().shuffle();
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, routingNodes, clusterState.nodes(), clusterInfoService.getClusterInfo());
        allocation.debugDecision(debug);
        boolean changed = false;
        // first, clear from the shards any node id they used to belong to that is now dead
        changed |= deassociateDeadNodes(allocation);

        // create a sorted list of from nodes with least number of shards to the maximum ones
        applyNewNodes(allocation);

        // elect primaries *before* allocating unassigned, so backups of primaries that failed
        // will be moved to primary state and not wait for primaries to be allocated and recovered (*from gateway*)
        changed |= electPrimariesAndUnassignedDanglingReplicas(allocation);

        if (!changed) {
            return new RoutingAllocation.Result(false, clusterState.routingTable());
        }
        return new RoutingAllocation.Result(true, new RoutingTable.Builder().updateNodes(routingNodes).build().validateRaiseException(clusterState.metaData()));
    }

    private boolean reroute(RoutingAllocation allocation) {
        boolean changed = false;
        // first, clear from the shards any node id they used to belong to that is now dead
        changed |= deassociateDeadNodes(allocation);

        // create a sorted list of from nodes with least number of shards to the maximum ones
        applyNewNodes(allocation);

        // elect primaries *before* allocating unassigned, so backups of primaries that failed
        // will be moved to primary state and not wait for primaries to be allocated and recovered (*from gateway*)
        changed |= electPrimariesAndUnassignedDanglingReplicas(allocation);

        // now allocate all the unassigned to available nodes
        if (allocation.routingNodes().hasUnassigned()) {
            changed |= shardsAllocators.allocateUnassigned(allocation);
            // elect primaries again, in case this is needed with unassigned allocation
            changed |= electPrimariesAndUnassignedDanglingReplicas(allocation);
        }

        // move shards that no longer can be allocated
        changed |= moveShards(allocation);

        // rebalance
        changed |= shardsAllocators.rebalance(allocation);
        assert RoutingNodes.assertShardStats(allocation.routingNodes());
        return changed;
    }

    private boolean moveShards(RoutingAllocation allocation) {
        boolean changed = false;

        // create a copy of the shards interleaving between nodes, and check if they can remain
        List<MutableShardRouting> shards = new ArrayList<>();
        int index = 0;
        boolean found = true;
        final RoutingNodes routingNodes = allocation.routingNodes();
        while (found) {
            found = false;
            for (RoutingNode routingNode : routingNodes) {
                if (index >= routingNode.size()) {
                    continue;
                }
                found = true;
                shards.add(routingNode.get(index));
            }
            index++;
        }
        for (int i = 0; i < shards.size(); i++) {
            MutableShardRouting shardRouting = shards.get(i);
            // we can only move started shards...
            if (!shardRouting.started()) {
                continue;
            }
            final RoutingNode routingNode = routingNodes.node(shardRouting.currentNodeId());
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

    private boolean electPrimariesAndUnassignedDanglingReplicas(RoutingAllocation allocation) {
        boolean changed = false;
        RoutingNodes routingNodes = allocation.routingNodes();
        if (!routingNodes.hasUnassignedPrimaries()) {
            // move out if we don't have unassigned primaries
            return changed;
        }

        // go over and remove dangling replicas that are initializing for primary shards
        List<ShardRouting> shardsToFail = Lists.newArrayList();
        for (MutableShardRouting shardEntry : routingNodes.unassigned()) {
            if (shardEntry.primary()) {
                for (MutableShardRouting routing : routingNodes.assignedShards(shardEntry)) {
                    if (!routing.primary() && routing.initializing()) {
                        shardsToFail.add(routing);
                    }
                }

            }
        }
        for (ShardRouting shardToFail : shardsToFail) {
           changed |= applyFailedShard(allocation, shardToFail, false);
        }

        // now, go over and elect a new primary if possible, not, from this code block on, if one is elected,
        // routingNodes.hasUnassignedPrimaries() will potentially be false

        for (MutableShardRouting shardEntry : routingNodes.unassigned()) {
            if (shardEntry.primary()) {
                MutableShardRouting candidate = allocation.routingNodes().activeReplica(shardEntry);
                if (candidate != null) {
                    IndexMetaData index = allocation.metaData().index(candidate.index());
                    routingNodes.swapPrimaryFlag(shardEntry, candidate);
                    if (candidate.relocatingNodeId() != null) {
                        changed = true;
                        // its also relocating, make sure to move the other routing to primary
                        RoutingNode node = routingNodes.node(candidate.relocatingNodeId());
                        if (node != null) {
                            for (MutableShardRouting shardRouting : node) {
                                if (shardRouting.shardId().equals(candidate.shardId()) && !shardRouting.primary()) {
                                    routingNodes.swapPrimaryFlag(shardRouting);
                                    break;
                                }
                            }
                        }
                    }
                    if (IndexMetaData.isIndexUsingShadowReplicas(index.settings())) {
                        routingNodes.reinitShadowPrimary(candidate);
                        changed = true;
                    }
                }
            }
        }

        return changed;
    }

    /**
     * Applies the new nodes to the routing nodes and returns them (just the
     * new nodes);
     */
    private void applyNewNodes(RoutingAllocation allocation) {
        final RoutingNodes routingNodes = allocation.routingNodes();
        for (ObjectCursor<DiscoveryNode> cursor : allocation.nodes().dataNodes().values()) {
            DiscoveryNode node = cursor.value;
            if (!routingNodes.isKnown(node)) {
                routingNodes.addNode(node);
            }
        }
    }

    private boolean deassociateDeadNodes(RoutingAllocation allocation) {
        boolean changed = false;
        for (RoutingNodes.RoutingNodesIterator it = allocation.routingNodes().nodes(); it.hasNext(); ) {
            RoutingNode node = it.next();
            if (allocation.nodes().dataNodes().containsKey(node.nodeId())) {
                // its a live node, continue
                continue;
            }
            changed = true;
            // now, go over all the shards routing on the node, and fail them
            for (MutableShardRouting shardRouting : node.copyShards()) {
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

            // retrieve the relocating node id before calling startedShard().
            String relocatingNodeId = null;

            RoutingNodes.RoutingNodeIterator currentRoutingNode = routingNodes.routingNodeIter(startedShard.currentNodeId());
            if (currentRoutingNode != null) {
                for (MutableShardRouting shard : currentRoutingNode) {
                    if (shard.shardId().equals(startedShard.shardId())) {
                        relocatingNodeId = shard.relocatingNodeId();
                        if (!shard.started()) {
                            dirty = true;
                            routingNodes.started(shard);
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

            RoutingNodes.RoutingNodeIterator sourceRoutingNode = routingNodes.routingNodeIter(relocatingNodeId);
            if (sourceRoutingNode != null) {
                while (sourceRoutingNode.hasNext()) {
                    MutableShardRouting shard = sourceRoutingNode.next();
                    if (shard.shardId().equals(startedShard.shardId())) {
                        if (shard.relocating()) {
                            dirty = true;
                            sourceRoutingNode.remove();
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

        RoutingNodes routingNodes = allocation.routingNodes();
        boolean dirty = false;
        if (failedShard.relocatingNodeId() != null) {
            // the shard is relocating, either in initializing (recovery from another node) or relocating (moving to another node)
            if (failedShard.state() == INITIALIZING) {
                // the shard is initializing and recovering from another node
                // first, we need to cancel the current node that is being initialized
                RoutingNodes.RoutingNodeIterator initializingNode = routingNodes.routingNodeIter(failedShard.currentNodeId());
                if (initializingNode != null) {
                    while (initializingNode.hasNext()) {
                        MutableShardRouting shardRouting = initializingNode.next();
                        if (shardRouting.equals(failedShard)) {
                            dirty = true;
                            initializingNode.remove();
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
                    RoutingNode relocatingFromNode = routingNodes.node(failedShard.relocatingNodeId());
                    if (relocatingFromNode != null) {
                        for (MutableShardRouting shardRouting : relocatingFromNode) {
                            if (shardRouting.shardId().equals(failedShard.shardId()) && shardRouting.relocating()) {
                                dirty = true;
                                routingNodes.cancelRelocation(shardRouting);
                                break;
                            }
                        }
                    }
                } else {
                    logger.debug("failed shard {} not found in routingNodes, ignoring it", failedShard);
                }
                return dirty;
            } else if (failedShard.state() == RELOCATING) {
                // the shard is relocating, meaning its the source the shard is relocating from
                // first, we need to cancel the current relocation from the current node
                // now, find the node that we are recovering from, cancel the relocation, remove it from the node
                // and add it to the unassigned shards list...
                RoutingNodes.RoutingNodeIterator relocatingFromNode = routingNodes.routingNodeIter(failedShard.currentNodeId());
                if (relocatingFromNode != null) {
                    while (relocatingFromNode.hasNext()) {
                        MutableShardRouting shardRouting = relocatingFromNode.next();
                        if (shardRouting.equals(failedShard)) {
                            dirty = true;
                            relocatingFromNode.remove();
                            if (addToIgnoreList) {
                                // make sure we ignore this shard on the relevant node
                                allocation.addIgnoreShardForNode(failedShard.shardId(), failedShard.currentNodeId());
                            }

                            routingNodes.unassigned().add(new MutableShardRouting(failedShard.index(), failedShard.id(),
                                    null, failedShard.primary(), ShardRoutingState.UNASSIGNED, failedShard.version() + 1));
                            break;
                        }
                    }
                }
                if (dirty) {
                    // next, we need to find the target initializing shard that is recovering from, and remove it...
                    RoutingNodes.RoutingNodeIterator initializingNode = routingNodes.routingNodeIter(failedShard.relocatingNodeId());
                    if (initializingNode != null) {
                        while (initializingNode.hasNext()) {
                            MutableShardRouting shardRouting = initializingNode.next();
                            if (shardRouting.shardId().equals(failedShard.shardId()) && shardRouting.state() == INITIALIZING) {
                                dirty = true;
                                initializingNode.remove();
                            }
                        }
                    }
                } else {
                    logger.debug("failed shard {} not found in routingNodes, ignoring it", failedShard);
                }
            } else {
                throw new ElasticsearchIllegalStateException("illegal state for a failed shard, relocating node id is set, but state does not match: " + failedShard);
            }
        } else {
            // the shard is not relocating, its either started, or initializing, just cancel it and move on...
            RoutingNodes.RoutingNodeIterator node = routingNodes.routingNodeIter(failedShard.currentNodeId());
            if (node != null) {
                while (node.hasNext()) {
                    MutableShardRouting shardRouting = node.next();
                    if (shardRouting.equals(failedShard)) {
                        dirty = true;
                        if (addToIgnoreList) {
                            // make sure we ignore this shard on the relevant node
                            allocation.addIgnoreShardForNode(failedShard.shardId(), failedShard.currentNodeId());
                        }
                        node.remove();
                        // move all the shards matching the failed shard to the end of the unassigned list
                        // so we give a chance for other allocations and won't create poison failed allocations
                        // that can keep other shards from being allocated (because of limits applied on how many
                        // shards we can start per node)
                        List<MutableShardRouting> shardsToMove = Lists.newArrayList();
                        for (Iterator<MutableShardRouting> unassignedIt = routingNodes.unassigned().iterator(); unassignedIt.hasNext(); ) {
                            MutableShardRouting unassignedShardRouting = unassignedIt.next();
                            if (unassignedShardRouting.shardId().equals(failedShard.shardId())) {
                                unassignedIt.remove();
                                shardsToMove.add(unassignedShardRouting);
                            }
                        }
                        if (!shardsToMove.isEmpty()) {
                            routingNodes.unassigned().addAll(shardsToMove);
                        }

                        routingNodes.unassigned().add(new MutableShardRouting(failedShard.index(), failedShard.id(), null,
                                null, failedShard.restoreSource(), failedShard.primary(), ShardRoutingState.UNASSIGNED, failedShard.version() + 1));

                        break;
                    }
                }
            }
            if (!dirty) {
                logger.debug("failed shard {} not found in routingNodes, ignoring it", failedShard);
            }
        }
        return dirty;
    }
}
