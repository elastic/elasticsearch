/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.cluster.routing.ShardRoutingState.*;
import static org.elasticsearch.common.collect.Sets.*;

/**
 * @author kimchy (shay.banon)
 */
public class ShardsAllocation extends AbstractComponent {

    private final NodeAllocations nodeAllocations;

    public ShardsAllocation() {
        this(ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    public ShardsAllocation(Settings settings) {
        this(settings, new NodeAllocations(settings));
    }

    @Inject public ShardsAllocation(Settings settings, NodeAllocations nodeAllocations) {
        super(settings);
        this.nodeAllocations = nodeAllocations;
    }

    /**
     * Applies the started shards. Note, shards can be called several times within this method.
     *
     * <p>If the same instance of the routing table is returned, then no change has been made.
     */
    public RoutingAllocation.Result applyStartedShards(ClusterState clusterState, List<? extends ShardRouting> startedShards) {
        RoutingNodes routingNodes = clusterState.routingNodes();
        StartedRerouteAllocation allocation = new StartedRerouteAllocation(routingNodes, clusterState.nodes(), startedShards);
        nodeAllocations.applyStartedShards(nodeAllocations, allocation);
        boolean changed = applyStartedShards(routingNodes, startedShards);
        if (!changed) {
            return new RoutingAllocation.Result(false, clusterState.routingTable(), allocation.explanation());
        }
        reroute(allocation);
        return new RoutingAllocation.Result(true, new RoutingTable.Builder().updateNodes(routingNodes).build().validateRaiseException(clusterState.metaData()), allocation.explanation());
    }

    /**
     * Applies the failed shards. Note, shards can be called several times within this method.
     *
     * <p>If the same instance of the routing table is returned, then no change has been made.
     */
    public RoutingAllocation.Result applyFailedShard(ClusterState clusterState, ShardRouting failedShard) {
        RoutingNodes routingNodes = clusterState.routingNodes();
        FailedRerouteAllocation allocation = new FailedRerouteAllocation(routingNodes, clusterState.nodes(), failedShard);
        boolean changed = applyFailedShard(allocation);
        if (!changed) {
            return new RoutingAllocation.Result(false, clusterState.routingTable(), allocation.explanation());
        }
        nodeAllocations.applyFailedShards(nodeAllocations, allocation);
        reroute(allocation);
        return new RoutingAllocation.Result(true, new RoutingTable.Builder().updateNodes(routingNodes).build().validateRaiseException(clusterState.metaData()), allocation.explanation());
    }

    /**
     * Reroutes the routing table based on the live nodes.
     *
     * <p>If the same instance of the routing table is returned, then no change has been made.
     */
    public RoutingAllocation.Result reroute(ClusterState clusterState) {
        RoutingNodes routingNodes = clusterState.routingNodes();
        RoutingAllocation allocation = new RoutingAllocation(routingNodes, clusterState.nodes());
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
        RoutingAllocation allocation = new RoutingAllocation(routingNodes, clusterState.nodes());
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
            changed |= nodeAllocations.allocateUnassigned(nodeAllocations, allocation);
            changed |= allocateUnassigned(allocation);
            // elect primaries again, in case this is needed with unassigned allocation
            changed |= electPrimaries(allocation.routingNodes());
        }

        // rebalance
        changed |= rebalance(allocation);

        return changed;
    }

    private boolean rebalance(RoutingAllocation allocation) {
        boolean changed = false;
        List<RoutingNode> sortedNodesLeastToHigh = allocation.routingNodes().sortedNodesLeastToHigh();
        if (sortedNodesLeastToHigh.isEmpty()) {
            return false;
        }
        int lowIndex = 0;
        int highIndex = sortedNodesLeastToHigh.size() - 1;
        boolean relocationPerformed;
        do {
            relocationPerformed = false;
            while (lowIndex != highIndex) {
                RoutingNode lowRoutingNode = sortedNodesLeastToHigh.get(lowIndex);
                RoutingNode highRoutingNode = sortedNodesLeastToHigh.get(highIndex);
                int averageNumOfShards = allocation.routingNodes().requiredAverageNumberOfShardsPerNode();

                // only active shards can be removed so must count only active ones.
                if (highRoutingNode.numberOfOwningShards() <= averageNumOfShards) {
                    highIndex--;
                    continue;
                }

                if (lowRoutingNode.shards().size() >= averageNumOfShards) {
                    lowIndex++;
                    continue;
                }

                boolean relocated = false;
                List<MutableShardRouting> startedShards = highRoutingNode.shardsWithState(STARTED);
                for (MutableShardRouting startedShard : startedShards) {
                    if (!nodeAllocations.canRebalance(startedShard, allocation)) {
                        continue;
                    }

                    if (nodeAllocations.canAllocate(startedShard, lowRoutingNode, allocation).allocate()) {
                        changed = true;
                        lowRoutingNode.add(new MutableShardRouting(startedShard.index(), startedShard.id(),
                                lowRoutingNode.nodeId(), startedShard.currentNodeId(),
                                startedShard.primary(), INITIALIZING));

                        startedShard.relocate(lowRoutingNode.nodeId());
                        relocated = true;
                        relocationPerformed = true;
                        break;
                    }
                }

                if (!relocated) {
                    highIndex--;
                }
            }
        } while (relocationPerformed);
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

    private boolean allocateUnassigned(RoutingAllocation allocation) {
        boolean changed = false;
        RoutingNodes routingNodes = allocation.routingNodes();


        List<RoutingNode> nodes = routingNodes.sortedNodesLeastToHigh();

        Iterator<MutableShardRouting> unassignedIterator = routingNodes.unassigned().iterator();
        int lastNode = 0;

        while (unassignedIterator.hasNext()) {
            MutableShardRouting shard = unassignedIterator.next();
            // do the allocation, finding the least "busy" node
            for (int i = 0; i < nodes.size(); i++) {
                RoutingNode node = nodes.get(lastNode);
                lastNode++;
                if (lastNode == nodes.size()) {
                    lastNode = 0;
                }

                if (nodeAllocations.canAllocate(shard, node, allocation).allocate()) {
                    int numberOfShardsToAllocate = routingNodes.requiredAverageNumberOfShardsPerNode() - node.shards().size();
                    if (numberOfShardsToAllocate <= 0) {
                        continue;
                    }

                    changed = true;
                    node.add(shard);
                    unassignedIterator.remove();
                    break;
                }
            }
        }

        // allocate all the unassigned shards above the average per node.
        for (Iterator<MutableShardRouting> it = routingNodes.unassigned().iterator(); it.hasNext();) {
            MutableShardRouting shard = it.next();
            // go over the nodes and try and allocate the remaining ones
            for (RoutingNode routingNode : routingNodes.sortedNodesLeastToHigh()) {
                if (nodeAllocations.canAllocate(shard, routingNode, allocation).allocate()) {
                    changed = true;
                    routingNode.add(shard);
                    it.remove();
                    break;
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
                RoutingNode routingNode = new RoutingNode(node.id());
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
            for (Iterator<MutableShardRouting> shardsIterator = routingNode.shards().iterator(); shardsIterator.hasNext();) {
                MutableShardRouting shardRoutingEntry = shardsIterator.next();
                if (shardRoutingEntry.assignedToNode()) {
                    // we store the relocation state here since when we call de-assign node
                    // later on, we will loose this state
                    boolean relocating = shardRoutingEntry.relocating();
                    String relocatingNodeId = shardRoutingEntry.relocatingNodeId();
                    // is this the destination shard that we are relocating an existing shard to?
                    // we know this since it has a relocating node id (the node we relocate from) and our state is INITIALIZING (and not RELOCATING)
                    boolean isRelocationDestinationShard = relocatingNodeId != null && shardRoutingEntry.initializing();

                    boolean currentNodeIsDead = false;
                    if (!liveNodeIds.contains(shardRoutingEntry.currentNodeId())) {
                        changed = true;
                        nodeIdsToRemove.add(shardRoutingEntry.currentNodeId());

                        if (!isRelocationDestinationShard) {
                            routingNodes.unassigned().add(shardRoutingEntry);
                        }

                        shardRoutingEntry.deassignNode();
                        currentNodeIsDead = true;
                        shardsIterator.remove();
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
                        shardsIterator.remove();
                    }
                }
            }
        }
        for (String nodeIdToRemove : nodeIdsToRemove) {
            routingNodes.nodesToShards().remove(nodeIdToRemove);
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

        // add the failed shard to the unassigned shards
        allocation.routingNodes().unassigned().add(new MutableShardRouting(failedShard.index(), failedShard.id(),
                null, failedShard.primary(), ShardRoutingState.UNASSIGNED));

        return true;
    }
}
