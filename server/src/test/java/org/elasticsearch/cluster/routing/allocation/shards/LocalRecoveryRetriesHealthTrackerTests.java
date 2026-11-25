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
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.health.node.HealthInfoTests;
import org.elasticsearch.health.node.UpdateHealthInfoCacheAction;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.groupingBy;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LocalRecoveryRetriesHealthTrackerTests extends ESTestCase {

    public void testNoShards() {
        assertThat(determineHealth(Map.of()), equalTo(LocalRecoveryRetriesHealthInfo.EMPTY));
    }

    public void testShardsWithLocalRetriesAreReportedSameIndex() {
        ShardId shardId1 = randomShardId();
        ShardId shardId2 = new ShardId(shardId1.getIndex(), shardId1.id() + 1);
        var shard1 = shard(shardId1, ShardRoutingState.INITIALIZING);
        var shard2 = shard(shardId2, ShardRoutingState.INITIALIZING);
        int retries1 = randomIntBetween(1, 10);
        int retries2 = randomIntBetween(1, 10);

        var health = determineHealth(Map.of(shard1, retries1, shard2, retries2));

        assertThat(
            health.localRetries(),
            equalTo(
                Map.of(
                    ShardAllocationId.fromRouting(shard1.routingEntry()),
                    retries1,
                    ShardAllocationId.fromRouting(shard2.routingEntry()),
                    retries2
                )
            )
        );
    }

    public void testShardsWithLocalRetiesAreReportedDifferentIndices() {
        var shard1 = shard(randomShardId(), ShardRoutingState.INITIALIZING);
        var shard2 = shard(randomShardId(), ShardRoutingState.INITIALIZING);
        int retries1 = randomIntBetween(1, 10);
        int retries2 = randomIntBetween(1, 10);

        var health = determineHealth(Map.of(shard1, retries1, shard2, retries2));

        assertThat(
            health.localRetries(),
            equalTo(
                Map.of(
                    ShardAllocationId.fromRouting(shard1.routingEntry()),
                    retries1,
                    ShardAllocationId.fromRouting(shard2.routingEntry()),
                    retries2
                )
            )
        );
    }

    public void testShardsWithoutLocalRetriesAreOmitted() {
        var shardWithoutRetries = shard(randomShardId(), ShardRoutingState.INITIALIZING);
        var shardWithRetries = shard(randomShardId(), ShardRoutingState.INITIALIZING);
        int retries = randomIntBetween(1, 10);

        LocalRecoveryRetriesHealthInfo health = determineHealth(Map.of(shardWithoutRetries, 0, shardWithRetries, retries));

        assertThat(health.localRetries(), equalTo(Map.of(ShardAllocationId.fromRouting(shardWithRetries.routingEntry()), retries)));
    }

    public void testShardsWhichAreActiveAreOmitted() {
        var inactive = shard(randomShardId(), ShardRoutingState.INITIALIZING);
        var active = shard(randomShardId(), ShardRoutingState.STARTED);
        int retriesInactive = randomIntBetween(1, 10);

        var health = determineHealth(Map.of(active, randomIntBetween(1, 10), inactive, retriesInactive));

        assertThat(health.localRetries(), equalTo(Map.of(ShardAllocationId.fromRouting(inactive.routingEntry()), retriesInactive)));
    }

    public void testSetBuilder() {
        var tracker = new LocalRecoveryRetriesHealthTracker(indicesService(List.of()), () -> mock(IndicesClusterStateService.class));
        var builder = mock(UpdateHealthInfoCacheAction.Request.Builder.class);
        var health = new LocalRecoveryRetriesHealthInfo(Map.of(HealthInfoTests.randomShardAllocationId(), randomIntBetween(1, 10)));

        tracker.addToRequestBuilder(builder, health);

        verify(builder).localRecoveryRetriesHealthInfo(health);
    }

    private static LocalRecoveryRetriesHealthInfo determineHealth(Map<IndexShard, Integer> shardsWithRetries) {
        var service = mock(IndicesClusterStateService.class);
        shardsWithRetries.forEach((shard, retries) -> when(service.getLocalRecoveryRetryCount(shard.routingEntry())).thenReturn(retries));
        var tracker = new LocalRecoveryRetriesHealthTracker(indicesService(List.copyOf(shardsWithRetries.keySet())), () -> service);
        return tracker.determineCurrentHealth();
    }

    /// Creates an [IndicesService] containing one [IndexService] per distinct index among `shards`, grouping
    /// each shard under its own `ShardRouting#index`.
    ///
    /// Mocks are used for [IndicesService], [IndexService] and [IndexShard] because constructing real ones needs a
    /// full node (or at least a data path, analysis registry, and engine factory), and the tracker only uses them
    /// as containers to iterate over and to get a shard's routing entry.
    private static IndicesService indicesService(List<IndexShard> shards) {
        var indexServices = shards.stream()
            .collect(groupingBy(shard -> shard.routingEntry().index()))
            .values()
            .stream()
            .map(shardsInIndex -> {
                var indexService = mock(IndexService.class);
                when(indexService.iterator()).thenAnswer(invocation -> shardsInIndex.iterator());
                return indexService;
            })
            .toList();
        var indicesService = mock(IndicesService.class);
        when(indicesService.iterator()).thenAnswer(invocation -> indexServices.iterator());
        return indicesService;
    }

    private static IndexShard shard(ShardId shardId, ShardRoutingState routingState) {
        var shard = mock(IndexShard.class);
        when(shard.routingEntry()).thenReturn(TestShardRouting.newShardRouting(shardId, randomAlphaOfLength(8), true, routingState));
        return shard;
    }

    private static ShardId randomShardId() {
        return new ShardId(randomAlphaOfLength(8), randomAlphaOfLength(22), randomIntBetween(0, 5));
    }
}
