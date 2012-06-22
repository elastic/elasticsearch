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

package org.elasticsearch.cluster.routing;

import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.cluster.node.DiscoveryNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

/**
 */
public class RoutingNode implements Iterable<MutableShardRouting> {

    private final String nodeId;

    private final DiscoveryNode node;

    private final List<MutableShardRouting> shards;

    public RoutingNode(String nodeId, DiscoveryNode node) {
        this(nodeId, node, new ArrayList<MutableShardRouting>());
    }

    public RoutingNode(String nodeId, DiscoveryNode node, List<MutableShardRouting> shards) {
        this.nodeId = nodeId;
        this.node = node;
        this.shards = shards;
    }

    @Override
    public Iterator<MutableShardRouting> iterator() {
        return shards.iterator();
    }

    public DiscoveryNode node() {
        return this.node;
    }

    public String nodeId() {
        return this.nodeId;
    }

    public List<MutableShardRouting> shards() {
        return this.shards;
    }

    public void add(MutableShardRouting shard) {
        for (MutableShardRouting shardRouting : shards) {
            if (shardRouting.shardId().equals(shard.shardId())) {
                throw new ElasticSearchIllegalStateException("Trying to add a shard [" + shard.shardId().index().name() + "][" + shard.shardId().id() + "] to a node [" + nodeId + "] where it already exists");
            }
        }
        shards.add(shard);
        shard.assignToNode(node.id());
    }

    public void removeByShardId(int shardId) {
        for (Iterator<MutableShardRouting> it = shards.iterator(); it.hasNext(); ) {
            MutableShardRouting shard = it.next();
            if (shard.id() == shardId) {
                it.remove();
            }
        }
    }

    public int numberOfShardsWithState(ShardRoutingState... states) {
        int count = 0;
        for (MutableShardRouting shardEntry : this) {
            for (ShardRoutingState state : states) {
                if (shardEntry.state() == state) {
                    count++;
                }
            }
        }
        return count;
    }

    public List<MutableShardRouting> shardsWithState(ShardRoutingState... states) {
        List<MutableShardRouting> shards = newArrayList();
        for (MutableShardRouting shardEntry : this) {
            for (ShardRoutingState state : states) {
                if (shardEntry.state() == state) {
                    shards.add(shardEntry);
                }
            }
        }
        return shards;
    }

    public List<MutableShardRouting> shardsWithState(String index, ShardRoutingState... states) {
        List<MutableShardRouting> shards = newArrayList();
        for (MutableShardRouting shardEntry : this) {
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

    public int numberOfShardsNotWithState(ShardRoutingState state) {
        int count = 0;
        for (MutableShardRouting shardEntry : this) {
            if (shardEntry.state() != state) {
                count++;
            }
        }
        return count;
    }

    /**
     * The number fo shards on this node that will not be eventually relocated.
     */
    public int numberOfOwningShards() {
        int count = 0;
        for (MutableShardRouting shardEntry : this) {
            if (shardEntry.state() != ShardRoutingState.RELOCATING) {
                count++;
            }
        }

        return count;
    }

    public String prettyPrint() {
        StringBuilder sb = new StringBuilder();
        sb.append("-----node_id[").append(nodeId).append("][" + (node == null ? "X" : "V") + "]\n");
        for (MutableShardRouting entry : shards) {
            sb.append("--------").append(entry.shortSummary()).append('\n');
        }
        return sb.toString();
    }
}
