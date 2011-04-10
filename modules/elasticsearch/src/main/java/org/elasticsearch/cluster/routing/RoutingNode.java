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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.common.collect.Lists.*;

/**
 * @author kimchy (Shay Banon)
 */
public class RoutingNode implements Iterable<MutableShardRouting> {

    private final String nodeId;

    private final List<MutableShardRouting> shards;

    public RoutingNode(String nodeId) {
        this(nodeId, new ArrayList<MutableShardRouting>());
    }

    public RoutingNode(String nodeId, List<MutableShardRouting> shards) {
        this.nodeId = nodeId;
        this.shards = shards;
    }

    @Override public Iterator<MutableShardRouting> iterator() {
        return shards.iterator();
    }

    public String nodeId() {
        return this.nodeId;
    }

    public List<MutableShardRouting> shards() {
        return this.shards;
    }

    public void add(MutableShardRouting shard) {
        shards.add(shard);
        shard.assignToNode(nodeId);
    }

    public void removeByShardId(int shardId) {
        for (Iterator<MutableShardRouting> it = shards.iterator(); it.hasNext();) {
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
        sb.append("-----node_id[").append(nodeId).append("]\n");
        for (MutableShardRouting entry : shards) {
            sb.append("--------").append(entry.shortSummary()).append('\n');
        }
        return sb.toString();
    }
}
