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

import com.google.common.collect.Iterators;
import org.elasticsearch.cluster.node.DiscoveryNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * A {@link RoutingNode} represents a cluster node associated with a single {@link DiscoveryNode} including all shards
 * that are hosted on that nodes. Each {@link RoutingNode} has a unique node id that can be used to identify the node.
 */
public class RoutingNode implements Iterable<ShardRouting> {

    private final String nodeId;

    private final DiscoveryNode node;

    private final List<ShardRouting> shards;

    public RoutingNode(String nodeId, DiscoveryNode node) {
        this(nodeId, node, new ArrayList<ShardRouting>());
    }

    public RoutingNode(String nodeId, DiscoveryNode node, List<ShardRouting> shards) {
        this.nodeId = nodeId;
        this.node = node;
        this.shards = shards;
    }

    @Override
    public Iterator<ShardRouting> iterator() {
        return Iterators.unmodifiableIterator(shards.iterator());
    }

    Iterator<ShardRouting> mutableIterator() {
        return shards.iterator();
    }

    /**
     * Returns the nodes {@link DiscoveryNode}.
     * 
     * @return discoveryNode of this node
     */
    public DiscoveryNode node() {
        return this.node;
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
        // TODO use Set with ShardIds for faster lookup.
        for (ShardRouting shardRouting : shards) {
            if (shardRouting.isSameShard(shard)) {
                throw new IllegalStateException("Trying to add a shard [" + shard.shardId().index().name() + "][" + shard.shardId().id() + "] to a node [" + nodeId + "] where it already exists");
            }
        }
        shards.add(shard);
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
            if (!shardEntry.index().equals(index)) {
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
        sb.append("-----node_id[").append(nodeId).append("][" + (node == null ? "X" : "V") + "]\n");
        for (ShardRouting entry : shards) {
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

    public ShardRouting get(int i) {
        return shards.get(i) ;
    }

    public Collection<ShardRouting> copyShards() {
        return new ArrayList<>(shards);
    }

    public boolean isEmpty() {
        return shards.isEmpty();
    }
}
