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

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.util.concurrent.NotThreadSafe;

import java.util.*;

import static com.google.common.collect.Lists.*;
import static com.google.common.collect.Maps.*;

/**
 * @author kimchy (Shay Banon)
 */
@NotThreadSafe
public class RoutingNodes implements Iterable<RoutingNode> {

    private final MetaData metaData;

    private final RoutingTable routingTable;

    private final Map<String, RoutingNode> nodesToShards = newHashMap();

    private final List<MutableShardRouting> unassigned = newArrayList();

    public RoutingNodes(MetaData metaData, RoutingTable routingTable) {
        this.metaData = metaData;
        this.routingTable = routingTable;
        Map<String, List<MutableShardRouting>> nodesToShards = newHashMap();
        for (IndexRoutingTable indexRoutingTable : routingTable.indicesRouting().values()) {
            for (IndexShardRoutingTable indexShard : indexRoutingTable) {
                for (ShardRouting shard : indexShard) {
                    if (shard.assignedToNode()) {
                        List<MutableShardRouting> entries = nodesToShards.get(shard.currentNodeId());
                        if (entries == null) {
                            entries = newArrayList();
                            nodesToShards.put(shard.currentNodeId(), entries);
                        }
                        entries.add(new MutableShardRouting(shard));
                        if (shard.relocating()) {
                            entries = nodesToShards.get(shard.relocatingNodeId());
                            if (entries == null) {
                                entries = newArrayList();
                                nodesToShards.put(shard.relocatingNodeId(), entries);
                            }
                            // add the counterpart shard with relocatingNodeId reflecting the source from which
                            // it's relocating from.
                            entries.add(new MutableShardRouting(shard.index(), shard.id(), shard.relocatingNodeId(),
                                    shard.currentNodeId(), shard.primary(), ShardRoutingState.INITIALIZING));
                        }
                    } else {
                        unassigned.add(new MutableShardRouting(shard));
                    }
                }
            }
        }
        for (Map.Entry<String, List<MutableShardRouting>> entry : nodesToShards.entrySet()) {
            String nodeId = entry.getKey();
            this.nodesToShards.put(nodeId, new RoutingNode(nodeId, entry.getValue()));
        }
    }

    @Override public Iterator<RoutingNode> iterator() {
        return nodesToShards.values().iterator();
    }

    public RoutingTable routingTable() {
        return routingTable;
    }

    public MetaData metaData() {
        return this.metaData;
    }

    public int requiredAverageNumberOfShardsPerNode() {
        return metaData.totalNumberOfShards() / nodesToShards.size();
    }

    public boolean hasUnassigned() {
        return !unassigned.isEmpty();
    }

    public List<MutableShardRouting> unassigned() {
        return this.unassigned;
    }

    public Map<String, RoutingNode> nodesToShards() {
        return nodesToShards;
    }

    public RoutingNode node(String nodeId) {
        return nodesToShards.get(nodeId);
    }

    public int numberOfShardsOfType(ShardRoutingState state) {
        int count = 0;
        for (RoutingNode routingNode : this) {
            count += routingNode.numberOfShardsWithState(state);
        }
        return count;
    }

    public List<MutableShardRouting> shardsOfType(ShardRoutingState state) {
        List<MutableShardRouting> shards = newArrayList();
        for (RoutingNode routingNode : this) {
            shards.addAll(routingNode.shardsWithState(state));
        }
        return shards;
    }

    public List<RoutingNode> sortedNodesLeastToHigh() {
        return nodesToShardsSorted(new Comparator<RoutingNode>() {
            @Override public int compare(RoutingNode o1, RoutingNode o2) {
                return o1.shards().size() - o2.shards().size();
            }
        });
    }

    public List<RoutingNode> nodesToShardsSorted(Comparator<RoutingNode> comparator) {
        List<RoutingNode> nodes = new ArrayList<RoutingNode>(nodesToShards.values());
        if (comparator != null) {
            Collections.sort(nodes, comparator);
        }
        return nodes;
    }

    public String prettyPrint() {
        StringBuilder sb = new StringBuilder("Routing Nodes:\n");
        for (RoutingNode routingNode : this) {
            sb.append(routingNode.prettyPrint());
        }
        sb.append("---- Unassigned\n");
        for (MutableShardRouting shardEntry : unassigned) {
            sb.append("--------").append(shardEntry.shortSummary()).append('\n');
        }
        return sb.toString();
    }
}
