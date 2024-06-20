/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.seqno;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RetentionLeaseSyncActionIT extends ESIntegTestCase {

    public void testActionCompletesWhenReplicaCircuitBreakersAreAtCapacity() {
        internalCluster().startMasterOnlyNodes(1);
        String primary = internalCluster().startDataOnlyNode();
        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put(indexSettings())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
        );

        String replica = internalCluster().startDataOnlyNode();
        ensureGreen("test");

        try (var ignored = fullyAllocateCircuitBreakerOnNode(replica, CircuitBreaker.IN_FLIGHT_REQUESTS)) {
            RetentionLeaseSyncer instance = internalCluster().getInstance(RetentionLeaseSyncer.class, primary);
            PlainActionFuture<ReplicationResponse> retentionLeaseSyncResult = new PlainActionFuture<>();
            ClusterState state = internalCluster().clusterService().state();
            ShardId testIndexShardZero = new ShardId(resolveIndex("test"), 0);
            ShardRouting primaryShard = state.routingTable().shardRoutingTable(testIndexShardZero).primaryShard();
            instance.sync(
                testIndexShardZero,
                primaryShard.allocationId().getId(),
                state.term(),
                RetentionLeases.EMPTY,
                retentionLeaseSyncResult
            );
            safeGet(retentionLeaseSyncResult);
        }
    }
}
