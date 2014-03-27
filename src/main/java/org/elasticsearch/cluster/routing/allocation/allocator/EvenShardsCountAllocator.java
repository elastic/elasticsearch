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

package org.elasticsearch.cluster.routing.allocation.allocator;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.StartedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;

/**
 * A {@link ShardsAllocator} that tries to balance shards across nodes in the
 * cluster such that each node holds approximatly the same number of shards. The
 * allocations algorithm operates on a cluster ie. is index-agnostic. While the
 * number of shards per node might be balanced across the cluster a single node
 * can hold mulitple shards from a single index such that the shard of an index
 * are not necessarily balanced across nodes. Yet, due to high-level
 * {@link AllocationDecider decisions} multiple instances of the same shard
 * won't be allocated on the same node.
 * <p>
 * During {@link #rebalance(RoutingAllocation) re-balancing} the allocator takes
 * shards from the <tt>most busy</tt> nodes and tries to relocate the shards to
 * the least busy node until the number of shards per node are equal for all
 * nodes in the cluster or until no shards can be relocated anymore.
 * </p>
 */
public class EvenShardsCountAllocator extends AbstractComponent implements ShardsAllocator {

    @Inject
    public EvenShardsCountAllocator(Settings settings) {
        super(settings);
    }

    @Override
    public void applyStartedShards(StartedRerouteAllocation allocation) {
    }

    @Override
    public void applyFailedShards(FailedRerouteAllocation allocation) {
    }

    @Override
    public boolean allocateUnassigned(RoutingAllocation allocation) {
        boolean changed = false;
        RoutingNodes routingNodes = allocation.routingNodes();
        /* 
         * 1. order nodes by the number of shards allocated on them least one first (this takes relocation into account)
         *    ie. if a shard is relocating the target nodes shard count is incremented.
         * 2. iterate over the unassigned shards
         *    2a. find the least busy node in the cluster that allows allocation for the current unassigned shard
         *    2b. if a node is found add the shard to the node and remove it from the unassigned shards
         * 3. iterate over the remaining unassigned shards and try to allocate them on next possible node
         */
        // order nodes by number of shards (asc) 
        RoutingNode[] nodes = sortedNodesLeastToHigh(allocation);

        Iterator<MutableShardRouting> unassignedIterator = routingNodes.unassigned().iterator();
        int lastNode = 0;

        while (unassignedIterator.hasNext()) {
            MutableShardRouting shard = unassignedIterator.next();
            // do the allocation, finding the least "busy" node
            for (int i = 0; i < nodes.length; i++) {
                RoutingNode node = nodes[lastNode];
                lastNode++;
                if (lastNode == nodes.length) {
                    lastNode = 0;
                }

                Decision decision = allocation.deciders().canAllocate(shard, node, allocation);
                if (decision.type() == Decision.Type.YES) {
                    int numberOfShardsToAllocate = routingNodes.requiredAverageNumberOfShardsPerNode() - node.size();
                    if (numberOfShardsToAllocate <= 0) {
                        continue;
                    }

                    changed = true;
                    allocation.routingNodes().assign(shard, node.nodeId());
                    unassignedIterator.remove();
                    break;
                }
            }
        }

        // allocate all the unassigned shards above the average per node.
        for (Iterator<MutableShardRouting> it = routingNodes.unassigned().iterator(); it.hasNext(); ) {
            MutableShardRouting shard = it.next();
            // go over the nodes and try and allocate the remaining ones
            for (RoutingNode routingNode : sortedNodesLeastToHigh(allocation)) {
                Decision decision = allocation.deciders().canAllocate(shard, routingNode, allocation);
                if (decision.type() == Decision.Type.YES) {
                    changed = true;
                    allocation.routingNodes().assign(shard, routingNode.nodeId());
                    it.remove();
                    break;
                }
            }
        }
        return changed;
    }

    @Override
    public boolean rebalance(RoutingAllocation allocation) {
        // take shards form busy nodes and move them to less busy nodes
        boolean changed = false;
        RoutingNode[] sortedNodesLeastToHigh = sortedNodesLeastToHigh(allocation);
        if (sortedNodesLeastToHigh.length == 0) {
            return false;
        }
        int lowIndex = 0;
        int highIndex = sortedNodesLeastToHigh.length - 1;
        boolean relocationPerformed;
        do {
            relocationPerformed = false;
            while (lowIndex != highIndex) {
                RoutingNode lowRoutingNode = sortedNodesLeastToHigh[lowIndex];
                RoutingNode highRoutingNode = sortedNodesLeastToHigh[highIndex];
                int averageNumOfShards = allocation.routingNodes().requiredAverageNumberOfShardsPerNode();

                // only active shards can be removed so must count only active ones.
                if (highRoutingNode.numberOfOwningShards() <= averageNumOfShards) {
                    highIndex--;
                    continue;
                }

                if (lowRoutingNode.size() >= averageNumOfShards) {
                    lowIndex++;
                    continue;
                }

                // Take a started shard from a "busy" node and move it to less busy node and go on 
                boolean relocated = false;
                List<MutableShardRouting> startedShards = highRoutingNode.shardsWithState(STARTED);
                for (MutableShardRouting startedShard : startedShards) {
                    Decision rebalanceDecision = allocation.deciders().canRebalance(startedShard, allocation);
                    if (rebalanceDecision.type() == Decision.Type.NO) {
                        continue;
                    }

                    Decision allocateDecision = allocation.deciders().canAllocate(startedShard, lowRoutingNode, allocation);
                    if (allocateDecision.type() == Decision.Type.YES) {
                        changed = true;
                        allocation.routingNodes().assign(new MutableShardRouting(startedShard.index(), startedShard.id(),
                                lowRoutingNode.nodeId(), startedShard.currentNodeId(), startedShard.restoreSource(),
                                startedShard.primary(), INITIALIZING, startedShard.version() + 1), lowRoutingNode.nodeId());

                        allocation.routingNodes().relocate(startedShard, lowRoutingNode.nodeId());
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

    @Override
    public boolean move(MutableShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (!shardRouting.started()) {
            return false;
        }
        boolean changed = false;
        RoutingNode[] sortedNodesLeastToHigh = sortedNodesLeastToHigh(allocation);
        if (sortedNodesLeastToHigh.length == 0) {
            return false;
        }

        for (RoutingNode nodeToCheck : sortedNodesLeastToHigh) {
            // check if its the node we are moving from, no sense to check on it
            if (nodeToCheck.nodeId().equals(node.nodeId())) {
                continue;
            }
            Decision decision = allocation.deciders().canAllocate(shardRouting, nodeToCheck, allocation);
            if (decision.type() == Decision.Type.YES) {
                allocation.routingNodes().assign(new MutableShardRouting(shardRouting.index(), shardRouting.id(),
                        nodeToCheck.nodeId(), shardRouting.currentNodeId(), shardRouting.restoreSource(),
                        shardRouting.primary(), INITIALIZING, shardRouting.version() + 1), nodeToCheck.nodeId());

                allocation.routingNodes().relocate(shardRouting, nodeToCheck.nodeId());
                changed = true;
                break;
            }
        }

        return changed;
    }

    private RoutingNode[] sortedNodesLeastToHigh(RoutingAllocation allocation) {
        // create count per node id, taking into account relocations
        final ObjectIntOpenHashMap<String> nodeCounts = new ObjectIntOpenHashMap<>();
        for (RoutingNode node : allocation.routingNodes()) {
            for (int i = 0; i < node.size(); i++) {
                ShardRouting shardRouting = node.get(i);
                String nodeId = shardRouting.relocating() ? shardRouting.relocatingNodeId() : shardRouting.currentNodeId();
                nodeCounts.addTo(nodeId, 1);
            }
        }
        RoutingNode[] nodes = allocation.routingNodes().toArray();
        Arrays.sort(nodes, new Comparator<RoutingNode>() {
            @Override
            public int compare(RoutingNode o1, RoutingNode o2) {
                return nodeCounts.get(o1.nodeId()) - nodeCounts.get(o2.nodeId());
            }
        });
        return nodes;
    }
}
