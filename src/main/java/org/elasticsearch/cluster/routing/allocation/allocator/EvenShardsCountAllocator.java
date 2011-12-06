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

package org.elasticsearch.cluster.routing.allocation.allocator;

import gnu.trove.map.hash.TObjectIntHashMap;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.StartedRerouteAllocation;
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

                if (allocation.deciders().canAllocate(shard, node, allocation).allocate()) {
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
        for (Iterator<MutableShardRouting> it = routingNodes.unassigned().iterator(); it.hasNext(); ) {
            MutableShardRouting shard = it.next();
            // go over the nodes and try and allocate the remaining ones
            for (RoutingNode routingNode : sortedNodesLeastToHigh(allocation)) {
                if (allocation.deciders().canAllocate(shard, routingNode, allocation).allocate()) {
                    changed = true;
                    routingNode.add(shard);
                    it.remove();
                    break;
                }
            }
        }
        return changed;
    }

    @Override
    public boolean rebalance(RoutingAllocation allocation) {
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

                if (lowRoutingNode.shards().size() >= averageNumOfShards) {
                    lowIndex++;
                    continue;
                }

                boolean relocated = false;
                List<MutableShardRouting> startedShards = highRoutingNode.shardsWithState(STARTED);
                for (MutableShardRouting startedShard : startedShards) {
                    if (!allocation.deciders().canRebalance(startedShard, allocation)) {
                        continue;
                    }

                    if (allocation.deciders().canAllocate(startedShard, lowRoutingNode, allocation).allocate()) {
                        changed = true;
                        lowRoutingNode.add(new MutableShardRouting(startedShard.index(), startedShard.id(),
                                lowRoutingNode.nodeId(), startedShard.currentNodeId(),
                                startedShard.primary(), INITIALIZING, startedShard.version() + 1));

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

    @Override
    public boolean move(MutableShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        assert shardRouting.started();
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
            if (allocation.deciders().canAllocate(shardRouting, nodeToCheck, allocation).allocate()) {
                nodeToCheck.add(new MutableShardRouting(shardRouting.index(), shardRouting.id(),
                        nodeToCheck.nodeId(), shardRouting.currentNodeId(),
                        shardRouting.primary(), INITIALIZING, shardRouting.version() + 1));

                shardRouting.relocate(nodeToCheck.nodeId());
                changed = true;
                break;
            }
        }

        return changed;
    }

    private RoutingNode[] sortedNodesLeastToHigh(RoutingAllocation allocation) {
        // create count per node id, taking into account relocations
        final TObjectIntHashMap<String> nodeCounts = new TObjectIntHashMap<String>();
        for (RoutingNode node : allocation.routingNodes()) {
            for (int i = 0; i < node.shards().size(); i++) {
                ShardRouting shardRouting = node.shards().get(i);
                String nodeId = shardRouting.relocating() ? shardRouting.relocatingNodeId() : shardRouting.currentNodeId();
                nodeCounts.adjustOrPutValue(nodeId, 1, 1);
            }
        }
        RoutingNode[] nodes = allocation.routingNodes().nodesToShards().values().toArray(new RoutingNode[allocation.routingNodes().nodesToShards().values().size()]);
        Arrays.sort(nodes, new Comparator<RoutingNode>() {
            @Override
            public int compare(RoutingNode o1, RoutingNode o2) {
                return nodeCounts.get(o1.nodeId()) - nodeCounts.get(o2.nodeId());
            }
        });
        return nodes;
    }
}
