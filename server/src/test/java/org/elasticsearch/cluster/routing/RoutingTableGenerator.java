/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.routing;

import com.carrotsearch.randomizedtesting.RandomizedContext;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.shard.ShardId;

import static org.elasticsearch.cluster.health.ClusterShardHealth.getInactivePrimaryHealth;

public class RoutingTableGenerator {
    private static int node_id = 1;

    private ShardRouting genShardRouting(String index, int shardId, boolean primary) {

        ShardRoutingState state;

        int stateRandomizer = RandomizedContext.current().getRandom().nextInt(40);
        if (stateRandomizer > 5) {
            state = ShardRoutingState.STARTED;
        } else if (stateRandomizer > 3 || primary) {
            state = ShardRoutingState.RELOCATING;
        } else {
            state = ShardRoutingState.INITIALIZING;
        }

        return switch (state) {
            case STARTED -> TestShardRouting.newShardRouting(
                index,
                shardId,
                "node_" + (node_id++),
                null,
                primary,
                ShardRoutingState.STARTED
            );
            case INITIALIZING -> TestShardRouting.newShardRouting(
                index,
                shardId,
                "node_" + (node_id++),
                null,
                primary,
                ShardRoutingState.INITIALIZING
            );
            case RELOCATING -> TestShardRouting.newShardRouting(
                index,
                shardId,
                "node_" + (node_id++),
                "node_" + (node_id++),
                primary,
                ShardRoutingState.RELOCATING
            );
            default -> throw new ElasticsearchException("Unknown state: " + state.name());
        };

    }

    public IndexShardRoutingTable.Builder genShardRoutingTable(IndexMetadata indexMetadata, int shardId, ShardCounter counter) {
        final String index = indexMetadata.getIndex().getName();
        IndexShardRoutingTable.Builder builder = new IndexShardRoutingTable.Builder(new ShardId(index, "_na_", shardId));
        final ShardRouting primary = genShardRouting(index, shardId, true);
        counter.update(primary);
        builder.addShard(primary);
        for (int replicas = indexMetadata.getNumberOfReplicas(); replicas > 0; replicas--) {
            final ShardRouting replica = genShardRouting(index, shardId, false);
            counter.update(replica);
            builder.addShard(replica);
        }

        return builder;
    }

    public IndexRoutingTable genIndexRoutingTable(IndexMetadata indexMetadata, ShardCounter counter) {
        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(indexMetadata.getIndex());
        for (int shard = 0; shard < indexMetadata.getNumberOfShards(); shard++) {
            builder.addIndexShard(genShardRoutingTable(indexMetadata, shard, counter));
        }
        return builder.build();
    }

    public static class ShardCounter {
        public int active;
        public int relocating;
        public int initializing;
        public int unassigned;
        public int unassignedPrimary;
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
                if (shardRouting.primary()) {
                    unassignedPrimary++;
                }
                unassigned++;
            }
        }
    }
}
