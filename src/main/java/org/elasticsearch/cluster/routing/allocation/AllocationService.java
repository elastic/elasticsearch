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

import com.google.common.collect.Lists;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocators;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.*;

import static com.google.common.collect.Sets.newHashSet;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;

/**
 *
 */
public class AllocationService extends AbstractComponent {

    private final AllocationDeciders allocationDeciders;

    private final ShardsAllocators shardsAllocators;

    public AllocationService() {
        this(ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    public AllocationService(Settings settings) {
        this(settings,
                new AllocationDeciders(settings, new NodeSettingsService(ImmutableSettings.Builder.EMPTY_SETTINGS)),
                new ShardsAllocators(settings)
        );
    }

    @Inject
    public AllocationService(Settings settings, AllocationDeciders allocationDeciders, ShardsAllocators shardsAllocators) {
        super(settings);
        this.allocationDeciders = allocationDeciders;
        this.shardsAllocators = shardsAllocators;
    }

    /**
     * Applies the started shards. Note, shards can be called several times within this method.
     * <p/>
     * <p>If the same instance of the routing table is returned, then no change has been made.
     */
    public RoutingAllocation.Result applyStartedShards(ClusterState clusterState, List<? extends ShardRouting> startedShards) {
        RoutingNodes routingNodes = clusterState.routingNodes();
        // shuffle the unassigned nodes, just so we won't have things like poison failed shards
        Collections.shuffle(routingNodes.unassigned());
        StartedRerouteAllocation allocation = new StartedRerouteAllocation(allocationDeciders, routingNodes, clusterState.nodes(), startedShards);
        boolean changed = applyStartedShards(routingNodes, startedShards);
        if (!changed) {
            return new RoutingAllocation.Result(false, clusterState.routingTable(), allocation.explanation());
        }
        shardsAllocators.applyStartedShards(allocation);
        reroute(allocation);
        return new RoutingAllocation.Result(true, new RoutingTable.Builder().updateNodes(routingNodes).build().validateRaiseException(clusterState.metaData()), allocation.explanation());
    }

    /**
     * Applies the failed shards. Note, shards can be called several times within this method.
     * <p/>
     * <p>If the same instance of the routing table is returned, then no change has been made.
     */
    public RoutingAllocation.Result applyFailedShard(ClusterState clusterState, ShardRouting failedShard) {
        RoutingNodes routingNodes = clusterState.routingNodes();
        // shuffle the unassigned nodes, just so we won't have things like poison failed shards
        Collections.shuffle(routingNodes.unassigned());
        FailedRerouteAllocation allocation = new FailedRerouteAllocation(allocationDeciders, routingNodes, clusterState.nodes(), failedShard);
        boolean changed = applyFailedShard(allocation);
        if (!changed) {
            return new RoutingAllocation.Result(false, clusterState.routingTable(), allocation.explanation());
        }
        shardsAllocators.applyFailedShards(allocation);
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
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, routingNodes, clusterState.nodes());
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
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, routingNodes, clusterState.nodes());
        Iterable<DiscoveryNode> dataNodes = allocation.nodes().dataNodes().values();
        boolean changed = false;
        // first, clear from the shards any node id they used to belong to that is now dead
        changed |= deassociateDeadNodes(allocation.routingNodes(), dataNodes);

        // create a sorted list of from nodes with least number of shards to the maximum ones
        applyNewNodes(allocation.routingNodes(), dataNodes);

        // elect primaries *before* allocating unassigned, so backups of primaries that failed
        // will be moved to primary state and not wait for primaries to be allocated and recovered (*from gateway*)
        changed |= electPrimaries(allocation.routingNodes());

        if (!changed) {
            return new RoutingAllocation.Result(false, clusterState.routingTable(), allocation.explanation());
        }
        return new RoutingAllocation.Result(true, new RoutingTable.Builder().updateNodes(routingNodes).build().validateRaiseException(clusterState.metaData()), allocation.explanation());
    }

    private boolean reroute(RoutingAllocation allocation) {
        Iterable<DiscoveryNode> dataNodes = allocation.nodes().dataNodes().values();

        boolean changed = false;
        // first, clear from the shards any node id they used to belong to that is now dead
        changed |= deassociateDeadNodes(allocation.routingNodes(), dataNodes);

        // create a sorted list of from nodes with least number of shards to the maximum ones
        applyNewNodes(allocation.routingNodes(), dataNodes);

        // elect primaries *before* allocating unassigned, so backups of primaries that failed
        // will be moved to primary state and not wait for primaries to be allocated and recovered (*from gateway*)
        changed |= electPrimaries(allocation.routingNodes());

        // now allocate all the unassigned to available nodes
        if (allocation.routingNodes().hasUnassigned()) {
            changed |= shardsAllocators.allocateUnassigned(allocation);
            // elect primaries again, in case this is needed with unassigned allocation
            changed |= electPrimaries(allocation.routingNodes());
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
            if (!allocation.deciders().canRemain(shardRouting, routingNode, allocation)) {
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

    private boolean electPrimaries(RoutingNodes routingNodes) {
        boolean changed = false;
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
        return changed;
    }

    /**
     * Applies the new nodes to the routing nodes and returns them (just the
     * new nodes);
     *
     * @param liveNodes currently live nodes.
     */
    private void applyNewNodes(RoutingNodes routingNodes, Iterable<DiscoveryNode> liveNodes) {
        for (DiscoveryNode node : liveNodes) {
            if (!routingNodes.nodesToShards().containsKey(node.id())) {
                RoutingNode routingNode = new RoutingNode(node.id(), node);
                routingNodes.nodesToShards().put(node.id(), routingNode);
            }
        }
    }

    private boolean deassociateDeadNodes(RoutingNodes routingNodes, Iterable<DiscoveryNode> liveNodes) {
        boolean changed = false;
        Set<String> liveNodeIds = newHashSet();
        for (DiscoveryNode liveNode : liveNodes) {
            liveNodeIds.add(liveNode.id());
        }
        Set<String> nodeIdsToRemove = newHashSet();
        for (RoutingNode routingNode : routingNodes) {
            for (Iterator<MutableShardRouting> shardsIterator = routingNode.shards().iterator(); shardsIterator.hasNext(); ) {
                MutableShardRouting shardRoutingEntry = shardsIterator.next();
                if (!shardRoutingEntry.assignedToNode()) {
                    throw new ElasticSearchIllegalStateException(shardRoutingEntry.shardId() + " is not assigned to a node, but listed on as existing on node [" + routingNode.nodeId() + "]");
                }
                // we store the relocation state here since when we call de-assign node
                // later on, we will loose this state
                boolean relocating = shardRoutingEntry.relocating();
                String relocatingNodeId = shardRoutingEntry.relocatingNodeId();
                // is this the destination shard that we are relocating an existing shard to?
                // we know this since it has a relocating node id (the node we relocate from) and our state is INITIALIZING (and not RELOCATING)
                boolean isRelocationDestinationShard = relocatingNodeId != null && shardRoutingEntry.initializing();

                boolean remove = false;
                boolean currentNodeIsDead = false;
                if (!liveNodeIds.contains(shardRoutingEntry.currentNodeId())) {
                    changed = true;
                    nodeIdsToRemove.add(shardRoutingEntry.currentNodeId());

                    if (!isRelocationDestinationShard) {
                        routingNodes.unassigned().add(shardRoutingEntry);
                    }

                    shardRoutingEntry.deassignNode();
                    currentNodeIsDead = true;
                    remove = true;
                }

                // move source shard back to active state and cancel relocation mode.
                if (relocating && !liveNodeIds.contains(relocatingNodeId)) {
                    nodeIdsToRemove.add(relocatingNodeId);
                    if (!currentNodeIsDead) {
                        changed = true;
                        shardRoutingEntry.cancelRelocation();
                    }
                }

                if (isRelocationDestinationShard && !liveNodeIds.contains(relocatingNodeId)) {
                    changed = true;
                    remove = true;
                }

                if (remove) {
                    shardsIterator.remove();
                }
            }
        }
        for (String nodeIdToRemove : nodeIdsToRemove) {
            routingNodes.nodesToShards().remove(nodeIdToRemove);
        }

        // now, go over shards that are initializing and recovering from primary shards that are now down...
        for (RoutingNode routingNode : routingNodes) {
            for (Iterator<MutableShardRouting> shardsIterator = routingNode.shards().iterator(); shardsIterator.hasNext(); ) {
                MutableShardRouting shardRoutingEntry = shardsIterator.next();
                if (!shardRoutingEntry.assignedToNode()) {
                    throw new ElasticSearchIllegalStateException(shardRoutingEntry.shardId() + " is not assigned to a node, but listed on as existing on node [" + routingNode.nodeId() + "]");
                }
                // we always recover from primaries, so we care about replicas that are not primaries
                if (shardRoutingEntry.primary()) {
                    continue;
                }
                // if its not initializing, then its not recovering from the primary
                if (!shardRoutingEntry.initializing()) {
                    continue;
                }
                // its initializing because its relocating from another node (its replica recovering from another replica)
                if (shardRoutingEntry.relocatingNodeId() != null) {
                    continue;
                }
                for (MutableShardRouting unassignedShardRouting : routingNodes.unassigned()) {
                    // double check on the unassignedShardRouting.primary(), but it has to be a primary... (well, we double checked actually before...)
                    if (unassignedShardRouting.shardId().equals(shardRoutingEntry.shardId()) && unassignedShardRouting.primary()) {
                        // remove it...
                        routingNodes.unassigned().add(shardRoutingEntry);
                        shardRoutingEntry.deassignNode();
                        shardsIterator.remove();
                        break;
                    }
                }
            }
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

            if (relocatingNodeId == null)
                continue;

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
    private boolean applyFailedShard(FailedRerouteAllocation allocation) {
        IndexRoutingTable indexRoutingTable = allocation.routingTable().index(allocation.failedShard().index());
        if (indexRoutingTable == null) {
            return false;
        }

        ShardRouting failedShard = allocation.failedShard();

        boolean shardDirty = false;
        boolean inRelocation = failedShard.relocatingNodeId() != null;
        if (inRelocation) {
            RoutingNode routingNode = allocation.routingNodes().nodesToShards().get(failedShard.currentNodeId());
            if (routingNode != null) {
                Iterator<MutableShardRouting> shards = routingNode.iterator();
                while (shards.hasNext()) {
                    MutableShardRouting shard = shards.next();
                    if (shard.shardId().equals(failedShard.shardId())) {
                        shardDirty = true;
                        shard.deassignNode();
                        shards.remove();
                        break;
                    }
                }
            }
        }

        String nodeId = inRelocation ? failedShard.relocatingNodeId() : failedShard.currentNodeId();
        RoutingNode currentRoutingNode = allocation.routingNodes().nodesToShards().get(nodeId);

        if (currentRoutingNode == null) {
            // already failed (might be called several times for the same shard)
            return false;
        }

        Iterator<MutableShardRouting> shards = currentRoutingNode.iterator();
        while (shards.hasNext()) {
            MutableShardRouting shard = shards.next();
            if (shard.shardId().equals(failedShard.shardId())) {
                shardDirty = true;
                if (!inRelocation) {
                    shard.deassignNode();
                    shards.remove();
                } else {
                    shard.cancelRelocation();
                }
                break;
            }
        }

        if (!shardDirty) {
            return false;
        }

        // make sure we ignore this shard on the relevant node
        allocation.addIgnoreShardForNode(failedShard.shardId(), failedShard.currentNodeId());

        // if in relocation no need to find a new target, just cancel the relocation.
        if (inRelocation) {
            return true; // lets true, so we reroute in this case
        }

        // move all the shards matching the failed shard to the end of the unassigned list
        // so we give a chance for other allocations and won't create poison failed allocations
        // that can keep other shards from being allocated (because of limits applied on how many
        // shards we can start per node)
        List<MutableShardRouting> shardsToMove = Lists.newArrayList();
        for (Iterator<MutableShardRouting> it = allocation.routingNodes().unassigned().iterator(); it.hasNext(); ) {
            MutableShardRouting shardRouting = it.next();
            if (shardRouting.shardId().equals(failedShard.shardId())) {
                it.remove();
                shardsToMove.add(shardRouting);
            }
        }
        if (!shardsToMove.isEmpty()) {
            allocation.routingNodes().unassigned().addAll(shardsToMove);
        }

        // add the failed shard to the unassigned shards
        allocation.routingNodes().unassigned().add(new MutableShardRouting(failedShard.index(), failedShard.id(),
                null, failedShard.primary(), ShardRoutingState.UNASSIGNED, failedShard.version() + 1));

        return true;
    }
}
