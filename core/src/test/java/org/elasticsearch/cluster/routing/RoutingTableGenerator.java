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

import com.carrotsearch.randomizedtesting.RandomizedContext;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.index.shard.ShardId;

import static org.elasticsearch.cluster.health.ClusterShardHealth.getInactivePrimaryHealth;

public class RoutingTableGenerator {
    private static int node_id = 1;

    private ShardRouting genShardRouting(String index, int shardId, boolean primary) {

        ShardRoutingState state;

        int stateRandomizer = RandomizedContext.current().getRandom().nextInt(40);
        if (stateRandomizer > 5) {
            state = ShardRoutingState.STARTED;
        } else if (stateRandomizer > 3) {
            state = ShardRoutingState.RELOCATING;
        } else {
            state = ShardRoutingState.INITIALIZING;
        }

        switch (state) {
            case STARTED:
                return TestShardRouting.newShardRouting(index, shardId, "node_" + Integer.toString(node_id++),
                                                        null, primary, ShardRoutingState.STARTED);
            case INITIALIZING:
                return TestShardRouting.newShardRouting(index, shardId, "node_" + Integer.toString(node_id++),
                                                        null, primary, ShardRoutingState.INITIALIZING);
            case RELOCATING:
                return TestShardRouting.newShardRouting(index, shardId, "node_" + Integer.toString(node_id++),
                                                        "node_" + Integer.toString(node_id++), primary, ShardRoutingState.RELOCATING);
            default:
                throw new ElasticsearchException("Unknown state: " + state.name());
        }

    }

    public IndexShardRoutingTable genShardRoutingTable(IndexMetaData indexMetaData, int shardId, ShardCounter counter) {
        final String index = indexMetaData.getIndex().getName();
        IndexShardRoutingTable.Builder builder = new IndexShardRoutingTable.Builder(new ShardId(index, "_na_", shardId));
        ShardRouting shardRouting = genShardRouting(index, shardId, true);
        counter.update(shardRouting);
        builder.addShard(shardRouting);
        for (int replicas = indexMetaData.getNumberOfReplicas(); replicas > 0; replicas--) {
            shardRouting = genShardRouting(index, shardId, false);
            counter.update(shardRouting);
            builder.addShard(shardRouting);
        }

        return builder.build();
    }

    public IndexRoutingTable genIndexRoutingTable(IndexMetaData indexMetaData, ShardCounter counter) {
        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(indexMetaData.getIndex());
        for (int shard = 0; shard < indexMetaData.getNumberOfShards(); shard++) {
            builder.addIndexShard(genShardRoutingTable(indexMetaData, shard, counter));
        }
        return builder.build();
    }

    public static class ShardCounter {
        public int active;
        public int relocating;
        public int initializing;
        public int unassigned;
        public int primaryActive;
        public int primaryInactive;
        private boolean inactivePrimaryCausesRed = false;

        public ClusterHealthStatus status() {
            if (primaryInactive > 0) {
                if (inactivePrimaryCausesRed) {
                    return ClusterHealthStatus.RED;
                } else {
                    return ClusterHealthStatus.YELLOW;
                }
            }
            if (unassigned > 0 || initializing > 0) {
                return ClusterHealthStatus.YELLOW;
            }
            return ClusterHealthStatus.GREEN;
        }

        public void update(ShardRouting shardRouting) {
            if (shardRouting.active()) {
                active++;
                if (shardRouting.primary()) {
                    primaryActive++;
                }
                if (shardRouting.relocating()) {
                    relocating++;
                }
                return;
            }

            if (shardRouting.primary()) {
                primaryInactive++;
                if (inactivePrimaryCausesRed == false) {
                    inactivePrimaryCausesRed = getInactivePrimaryHealth(shardRouting) == ClusterHealthStatus.RED;
                }
            }
            if (shardRouting.initializing()) {
                initializing++;
            } else {
                unassigned++;
            }
        }
    }
}
