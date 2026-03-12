/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.seqno;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.IndexingPressure;
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
            assertThatRetentionLeaseSyncCompletesSuccessfully(primary);
        }
    }

    public void testActionCompletesWhenPrimaryIndexingPressureIsAtCapacity() {
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

        try (Releasable ignored = fullyAllocatePrimaryIndexingCapacityOnNode(primary)) {
            assertThatRetentionLeaseSyncCompletesSuccessfully(primary);
        }
    }

    private static void assertThatRetentionLeaseSyncCompletesSuccessfully(String primaryNodeName) {
        RetentionLeaseSyncer instance = internalCluster().getInstance(RetentionLeaseSyncer.class, primaryNodeName);
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

    /**
     * Fully allocate primary indexing capacity on a node
     *
     * @param targetNode The name of the node on which to allocate
     * @return A {@link Releasable} which will release the capacity when closed
     */
    private static Releasable fullyAllocatePrimaryIndexingCapacityOnNode(String targetNode) {
        return internalCluster().getInstance(IndexingPressure.class, targetNode)
            .validateAndMarkPrimaryOperationStarted(
                1,
                IndexingPressure.MAX_PRIMARY_BYTES.get(internalCluster().getInstance(Settings.class, targetNode)).getBytes() + 1,
                0,
                true,
                false
            );
    }
}
