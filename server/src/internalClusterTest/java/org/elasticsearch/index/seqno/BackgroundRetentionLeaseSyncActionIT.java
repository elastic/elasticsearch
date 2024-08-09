/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.seqno;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class BackgroundRetentionLeaseSyncActionIT extends ESIntegTestCase {

    public void testActionCompletesWhenReplicaCircuitBreakersAreAtCapacity() throws Exception {
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
            final ClusterState state = internalCluster().clusterService().state();
            final Index testIndex = resolveIndex("test");
            final ShardId testIndexShardZero = new ShardId(testIndex, 0);
            final String testLeaseId = "test-lease/123";
            RetentionLeases newLeases = addTestLeaseToRetentionLeases(primary, testIndex, testLeaseId);
            internalCluster().getInstance(RetentionLeaseSyncer.class, primary)
                .backgroundSync(
                    testIndexShardZero,
                    state.routingTable().shardRoutingTable(testIndexShardZero).primaryShard().allocationId().getId(),
                    state.term(),
                    newLeases
                );

            // Wait for test lease to appear on replica
            IndicesService replicaIndicesService = internalCluster().getInstance(IndicesService.class, replica);
            assertBusy(() -> {
                RetentionLeases retentionLeases = replicaIndicesService.indexService(testIndex).getShard(0).getRetentionLeases();
                assertTrue(retentionLeases.contains(testLeaseId));
            });
        }
    }

    private static RetentionLeases addTestLeaseToRetentionLeases(String primaryNodeName, Index index, String leaseId) {
        IndicesService primaryIndicesService = internalCluster().getInstance(IndicesService.class, primaryNodeName);
        RetentionLeases currentLeases = primaryIndicesService.indexService(index).getShard(0).getRetentionLeases();
        RetentionLease newLease = new RetentionLease(leaseId, 0, System.currentTimeMillis(), "test source");
        return new RetentionLeases(
            currentLeases.primaryTerm(),
            currentLeases.version() + 1,
            Stream.concat(currentLeases.leases().stream(), Stream.of(newLease)).toList()
        );
    }
}
