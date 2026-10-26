/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.shards;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.health.node.UpdateHealthInfoCacheAction;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LocalRecoveryRetriesHealthTrackerTests extends ESTestCase {

    public void testNoShards() {
        assertThat(determineHealth(List.of()), equalTo(LocalRecoveryRetriesHealthInfo.EMPTY));
    }

    public void testShardsWithoutLocalRetriesAreOmitted() {
        var health = determineHealth(List.of(shard(randomShardId(), 0, RecoveryState.Stage.INIT, ShardRoutingState.INITIALIZING)));
        assertThat(health, equalTo(LocalRecoveryRetriesHealthInfo.EMPTY));
    }

    public void testShardsWithLocalRetriesAreReported() {
        var shard1 = shard(
            randomShardId(),
            randomIntBetween(1, 10),
            randomFrom(RecoveryState.Stage.values()),
            ShardRoutingState.INITIALIZING
        );
        var shard2 = shard(
            randomShardId(),
            randomIntBetween(1, 10),
            randomFrom(RecoveryState.Stage.values()),
            ShardRoutingState.INITIALIZING
        );
        var shardWithoutRetries = shard(randomShardId(), 0, randomFrom(RecoveryState.Stage.values()), ShardRoutingState.INITIALIZING);

        var health = determineHealth(List.of(shard1, shard2, shardWithoutRetries));

        assertThat(
            health.localRetries(),
            equalTo(
                Map.of(
                    shard1.shardId(),
                    shard1.recoveryState().getLocalRetries(),
                    shard2.shardId(),
                    shard2.recoveryState().getLocalRetries()
                )
            )
        );
    }

    /// The retry count in a [RecoveryState] is retained after the shard has started, but the health indicator only reads it for shards
    /// which are not active in the routing table, so there is no need to report it for shards which are.
    public void testShardsWhichAreActiveAreOmitted() {
        var active = shard(randomShardId(), randomIntBetween(1, 10), RecoveryState.Stage.DONE, ShardRoutingState.STARTED);
        var inactive = shard(
            randomShardId(),
            randomIntBetween(1, 10),
            randomFrom(RecoveryState.Stage.values()),
            ShardRoutingState.INITIALIZING
        );

        var health = determineHealth(List.of(active, inactive));

        assertThat(health.localRetries(), equalTo(Map.of(inactive.shardId(), inactive.recoveryState().getLocalRetries())));
    }

    /// A recovery can fail and be retried locally after it has completed, but before the shard is started and active in the routing
    /// table (e.g. in post-recovery), so retries are reported for such shards even though the recovery is done.
    public void testShardsWithCompletedRecoveryWhichAreNotYetActiveAreReported() {
        var shard = shard(randomShardId(), randomIntBetween(1, 10), RecoveryState.Stage.DONE, ShardRoutingState.INITIALIZING);

        var health = determineHealth(List.of(shard));

        assertThat(health.localRetries(), equalTo(Map.of(shard.shardId(), shard.recoveryState().getLocalRetries())));
    }

    public void testShardsFromMultipleIndicesAreReported() {
        var shard1 = shard(
            randomShardId(),
            randomIntBetween(1, 10),
            randomFrom(RecoveryState.Stage.values()),
            ShardRoutingState.INITIALIZING
        );
        var shard2 = shard(
            randomShardId(),
            randomIntBetween(1, 10),
            randomFrom(RecoveryState.Stage.values()),
            ShardRoutingState.INITIALIZING
        );

        var tracker = new LocalRecoveryRetriesHealthTracker(indicesService(List.of(List.of(shard1), List.of(shard2))));
        var health = tracker.determineCurrentHealth();

        assertThat(
            health.localRetries(),
            equalTo(
                Map.of(
                    shard1.shardId(),
                    shard1.recoveryState().getLocalRetries(),
                    shard2.shardId(),
                    shard2.recoveryState().getLocalRetries()
                )
            )
        );
    }

    public void testSetBuilder() {
        var tracker = new LocalRecoveryRetriesHealthTracker(indicesService(List.of()));
        var builder = mock(UpdateHealthInfoCacheAction.Request.Builder.class);
        var health = new LocalRecoveryRetriesHealthInfo(Map.of(randomShardId(), randomIntBetween(1, 10)));

        tracker.addToRequestBuilder(builder, health);

        verify(builder).localRecoveryRetriesHealthInfo(health);
    }

    private static LocalRecoveryRetriesHealthInfo determineHealth(List<IndexShard> shards) {
        return new LocalRecoveryRetriesHealthTracker(indicesService(List.of(shards))).determineCurrentHealth();
    }

    /// Creates an [IndicesService] containing one [IndexService] for each of the given lists of shards.
    ///
    /// Mocks are used for [IndicesService], [IndexService] and [IndexShard] because constructing real ones needs a full
    /// node (or at least a data path, analysis registry, and engine factory), and the tracker only uses them as containers to iterate over
    /// and to get a shard's ID, routing entry and [RecoveryState] from. The [RecoveryState] instances are real.
    private static IndicesService indicesService(List<List<IndexShard>> shardsByIndex) {
        var indexServices = shardsByIndex.stream().map(shards -> {
            var indexService = mock(IndexService.class);
            when(indexService.iterator()).thenAnswer(invocation -> shards.iterator());
            return indexService;
        }).toList();
        var indicesService = mock(IndicesService.class);
        when(indicesService.iterator()).thenAnswer(invocation -> indexServices.iterator());
        return indicesService;
    }

    private static IndexShard shard(ShardId shardId, int localRetries, RecoveryState.Stage stage, ShardRoutingState routingState) {
        var shard = mock(IndexShard.class);
        when(shard.shardId()).thenReturn(shardId);
        when(shard.routingEntry()).thenReturn(TestShardRouting.newShardRouting(shardId, randomAlphaOfLength(8), true, routingState));
        when(shard.recoveryState()).thenReturn(recoveryState(shardId, localRetries, stage));
        return shard;
    }

    /// Creates a real [RecoveryState] which has been moved through the recovery stages as far as the given one.
    private static RecoveryState recoveryState(ShardId shardId, int localRetries, RecoveryState.Stage stage) {
        DiscoveryNode node = DiscoveryNodeUtils.create(randomAlphaOfLength(8));
        ShardRouting routing = TestShardRouting.newShardRouting(shardId, node.getId(), true, ShardRoutingState.INITIALIZING);
        var recoveryState = new RecoveryState(routing, node, null, localRetries);
        for (var next : RecoveryState.Stage.values()) {
            if (next == RecoveryState.Stage.CREATED) {
                continue;
            }
            if (next.ordinal() > stage.ordinal()) {
                break;
            }
            if (next == RecoveryState.Stage.FINALIZE) {
                recoveryState.getIndex().setFileDetailsComplete();
            }
            recoveryState.setStage(next);
        }
        assertThat(recoveryState.getStage(), equalTo(stage));
        return recoveryState;
    }

    private static ShardId randomShardId() {
        return new ShardId(randomAlphaOfLength(8), randomAlphaOfLength(22), randomIntBetween(0, 5));
    }
}
