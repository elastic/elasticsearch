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

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * A {@link RoutingNode} represents a cluster node associated with a single {@link DiscoveryNode} including all shards
 * that are hosted on that nodes. Each {@link RoutingNode} has a unique node id that can be used to identify the node.
 */
public class RoutingNode implements Iterable<ShardRouting> {

    private final String nodeId;

    private final DiscoveryNode node;

    private final LinkedHashMap<ShardId, ShardRouting> shards; // LinkedHashMap to preserve order

    public RoutingNode(String nodeId, DiscoveryNode node, ShardRouting... shards) {
        this(nodeId, node, buildShardRoutingMap(shards));
    }

    RoutingNode(String nodeId, DiscoveryNode node, LinkedHashMap<ShardId, ShardRouting> shards) {
        this.nodeId = nodeId;
        this.node = node;
        this.shards = shards;
    }

    private static LinkedHashMap<ShardId, ShardRouting> buildShardRoutingMap(ShardRouting... shardRoutings) {
        final LinkedHashMap<ShardId, ShardRouting> shards = new LinkedHashMap<>();
        for (ShardRouting shardRouting : shardRoutings) {
            ShardRouting previousValue = shards.put(shardRouting.shardId(), shardRouting);
            if (previousValue != null) {
                throw new IllegalArgumentException("Cannot have two different shards with same shard id " + shardRouting.shardId() +
                    " on same node ");
            }
        }
        return shards;
    }

    @Override
    public Iterator<ShardRouting> iterator() {
        return Collections.unmodifiableCollection(shards.values()).iterator();
    }

    /**
     * Returns the nodes {@link DiscoveryNode}.
     *
     * @return discoveryNode of this node
     */
    public DiscoveryNode node() {
        return this.node;
    }

    @Nullable
    public ShardRouting getByShardId(ShardId id) {
        return shards.get(id);
    }

    /**
     * Get the id of this node
     * @return id of the node
     */
    public String nodeId() {
        return this.nodeId;
    }

    public int size() {
        return shards.size();
    }

    /**
     * Add a new shard to this node
     * @param shard Shard to crate on this Node
     */
    void add(ShardRouting shard) {
        if (shards.containsKey(shard.shardId())) {
            throw new IllegalStateException("Trying to add a shard " + shard.shardId() + " to a node [" + nodeId
                + "] where it already exists. current [" + shards.get(shard.shardId()) + "]. new [" + shard + "]");
        }
        shards.put(shard.shardId(), shard);
    }

    void update(ShardRouting oldShard, ShardRouting newShard) {
        if (shards.containsKey(oldShard.shardId()) == false) {
            // Shard was already removed by routing nodes iterator
            // TODO: change caller logic in RoutingNodes so that this check can go away
            return;
        }
        ShardRouting previousValue = shards.put(newShard.shardId(), newShard);
        assert previousValue == oldShard : "expected shard " + previousValue + " but was " + oldShard;
    }

    void remove(ShardRouting shard) {
        ShardRouting previousValue = shards.remove(shard.shardId());
        assert previousValue == shard : "expected shard " + previousValue + " but was " + shard;
    }

    /**
     * Determine the number of shards with a specific state
     * @param states set of states which should be counted
     * @return number of shards
     */
    public int numberOfShardsWithState(ShardRoutingState... states) {
        int count = 0;
        for (ShardRouting shardEntry : this) {
            for (ShardRoutingState state : states) {
                if (shardEntry.state() == state) {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * Determine the shards with a specific state
     * @param states set of states which should be listed
     * @return List of shards
     */
    public List<ShardRouting> shardsWithState(ShardRoutingState... states) {
        List<ShardRouting> shards = new ArrayList<>();
        for (ShardRouting shardEntry : this) {
            for (ShardRoutingState state : states) {
                if (shardEntry.state() == state) {
                    shards.add(shardEntry);
                }
            }
        }
        return shards;
    }

    /**
     * Determine the shards of an index with a specific state
     * @param index id of the index
     * @param states set of states which should be listed
     * @return a list of shards
     */
    public List<ShardRouting> shardsWithState(String index, ShardRoutingState... states) {
        List<ShardRouting> shards = new ArrayList<>();

        for (ShardRouting shardEntry : this) {
            if (!shardEntry.getIndexName().equals(index)) {
                continue;
            }
            for (ShardRoutingState state : states) {
                if (shardEntry.state() == state) {
                    shards.add(shardEntry);
                }
            }
        }
        return shards;
    }

    /**
     * The number of shards on this node that will not be eventually relocated.
     */
    public int numberOfOwningShards() {
        int count = 0;
        for (ShardRouting shardEntry : this) {
            if (shardEntry.state() != ShardRoutingState.RELOCATING) {
                count++;
            }
        }

        return count;
    }

    public String prettyPrint() {
        StringBuilder sb = new StringBuilder();
        sb.append("-----node_id[").append(nodeId).append("][").append(node == null ? "X" : "V").append("]\n");
        for (ShardRouting entry : shards.values()) {
            sb.append("--------").append(entry.shortSummary()).append('\n');
        }
        return sb.toString();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("routingNode ([");
        sb.append(node.getName());
        sb.append("][");
        sb.append(node.getId());
        sb.append("][");
        sb.append(node.getHostName());
        sb.append("][");
        sb.append(node.getHostAddress());
        sb.append("], [");
        sb.append(shards.size());
        sb.append(" assigned shards])");
        return sb.toString();
    }

    public List<ShardRouting> copyShards() {
        return new ArrayList<>(shards.values());
    }

    public boolean isEmpty() {
        return shards.isEmpty();
    }
}
