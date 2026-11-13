/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.shards;

import org.elasticsearch.cluster.routing.ShardAllocationId;
import org.elasticsearch.health.node.HealthInfoTests;
import org.elasticsearch.health.node.UpdateHealthInfoCacheAction;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LocalRecoveryRetriesHealthTrackerTests extends ESTestCase {

    public void testNoRetries() {
        assertThat(determineHealth(Map.of()), equalTo(LocalRecoveryRetriesHealthInfo.EMPTY));
    }

    public void testRetriesAreReportedPerAllocation() {
        var shard1 = randomShardId();
        var shard2 = randomShardId();
        var allocation1 = randomAlphaOfLength(12);
        var allocation2 = randomAlphaOfLength(12);
        int retries1 = randomIntBetween(1, 10);
        int retries2 = randomIntBetween(1, 10);

        var health = determineHealth(
            Map.of(new ShardAllocationId(shard1, allocation1), retries1, new ShardAllocationId(shard2, allocation2), retries2)
        );

        assertThat(
            health.localRetries(),
            equalTo(Map.of(new ShardAllocationId(shard1, allocation1), retries1, new ShardAllocationId(shard2, allocation2), retries2))
        );
    }

    /// The tracker must key its output by [ShardAllocationId], not by shard ID alone -- otherwise two allocations of the
    /// same shard (e.g. a stale entry not yet cleaned up alongside its replacement) would be merged into one, silently
    /// dropping or mixing up one allocation's retry count with the other's.
    public void testMultipleAllocationsOfSameShardAreReported() {
        var shardId = randomShardId();
        var allocation1 = randomAlphaOfLength(12);
        var allocation2 = randomAlphaOfLength(12);
        int retries1 = randomIntBetween(1, 10);
        int retries2 = randomIntBetween(1, 10);

        var health = determineHealth(
            Map.of(new ShardAllocationId(shardId, allocation1), retries1, new ShardAllocationId(shardId, allocation2), retries2)
        );

        assertThat(
            health.localRetries(),
            equalTo(Map.of(new ShardAllocationId(shardId, allocation1), retries1, new ShardAllocationId(shardId, allocation2), retries2))
        );
    }

    public void testSetBuilder() {
        var tracker = new LocalRecoveryRetriesHealthTracker(() -> indicesClusterStateService(Map.of()));
        var builder = mock(UpdateHealthInfoCacheAction.Request.Builder.class);
        var health = new LocalRecoveryRetriesHealthInfo(Map.of(HealthInfoTests.randomShardAllocationId(), randomIntBetween(1, 10)));

        tracker.addToRequestBuilder(builder, health);

        verify(builder).localRecoveryRetriesHealthInfo(health);
    }

    private static LocalRecoveryRetriesHealthInfo determineHealth(Map<ShardAllocationId, Integer> localRecoveryRetryCounts) {
        var tracker = new LocalRecoveryRetriesHealthTracker(() -> indicesClusterStateService(localRecoveryRetryCounts));
        return tracker.determineCurrentHealth();
    }

    /// [LocalRecoveryRetriesHealthTracker] only reads [IndicesClusterStateService#getLocalRecoveryRetryCounts], so that's
    /// the only method mocked here -- the real method's own behaviour (e.g. omitting shards with no pending allocation
    /// state, or ones that never retried) is covered directly against IndicesClusterStateService itself.
    private static IndicesClusterStateService indicesClusterStateService(Map<ShardAllocationId, Integer> localRecoveryRetryCounts) {
        var service = mock(IndicesClusterStateService.class);
        when(service.getLocalRecoveryRetryCounts()).thenReturn(localRecoveryRetryCounts);
        return service;
    }

    private static ShardId randomShardId() {
        return new ShardId(randomAlphaOfLength(8), randomAlphaOfLength(22), randomIntBetween(0, 5));
    }
}
