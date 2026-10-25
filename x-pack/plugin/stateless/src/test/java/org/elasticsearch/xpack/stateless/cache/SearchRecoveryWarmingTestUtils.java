/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import java.util.List;

import static org.elasticsearch.cluster.metadata.Metadata.DEFAULT_PROJECT_ID;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class SearchRecoveryWarmingTestUtils {

    private SearchRecoveryWarmingTestUtils() {}

    static IndexShard mockIndexShard(ShardRouting self) {
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.routingEntry()).thenReturn(self);
        when(indexShard.shardId()).thenReturn(self.shardId());
        return indexShard;
    }

    /// One primary-replica pair: [ShardRouting.Role#INDEX_ONLY] primary, [ShardRouting.Role#SEARCH_ONLY] replica.
    static ClusterState clusterStateOneSearchReplica(String indexName, ShardRoutingState replicaState) {
        return ClusterStateCreationUtils.state(
            DEFAULT_PROJECT_ID,
            indexName,
            true,
            STARTED,
            ShardRouting.Role.INDEX_ONLY,
            List.of(new Tuple<>(replicaState, ShardRouting.Role.SEARCH_ONLY))
        );
    }

    /// [ShardRouting.Role#INDEX_ONLY] primary and two [ShardRouting.Role#SEARCH_ONLY] replicas: an active
    /// peer and an [ShardRoutingState#INITIALIZING] copy (the shard under recovery). The index
    /// primary is not searchable, so the peer supplies the other active search copy.
    static ClusterState clusterStateInitializingSearchReplicaWithActivePeer(String indexName) {
        return ClusterStateCreationUtils.state(
            DEFAULT_PROJECT_ID,
            indexName,
            true,
            STARTED,
            ShardRouting.Role.INDEX_ONLY,
            List.of(
                new Tuple<>(randomFrom(STARTED, RELOCATING), ShardRouting.Role.SEARCH_ONLY),
                new Tuple<>(INITIALIZING, ShardRouting.Role.SEARCH_ONLY)
            )
        );
    }

    /// The [ShardRoutingState#INITIALIZING] search replica (second replica) from
    /// [#clusterStateInitializingSearchReplicaWithActivePeer].
    static ShardRouting initializingSearchReplica(ClusterState state, ShardId shardId) {
        var replicas = state.routingTable(DEFAULT_PROJECT_ID).shardRoutingTable(shardId).replicaShards();
        assert replicas.size() == 2 : replicas;
        assert replicas.get(1).initializing();
        return replicas.get(1);
    }
}
