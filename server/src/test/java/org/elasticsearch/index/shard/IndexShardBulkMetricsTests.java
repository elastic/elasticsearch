/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.metric.MetricAttributes;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Drives the bulk latency histogram through a real {@link IndexShard}, covering the primary gate across a replica promotion.
 */
public class IndexShardBulkMetricsTests extends IndexShardTestCase {

    private final RecordingMeterRegistry registry = new RecordingMeterRegistry();
    private final ShardMetrics shardMetrics = ShardMetrics.create(registry);

    @Override
    protected ShardMetrics shardMetrics() {
        return shardMetrics;
    }

    public void testBulkLatencyIsRecordedOncePromotedToPrimary() throws IOException {
        IndexShard shard = newStartedShard(false);
        long tookNanos = randomLongBetween(1, TimeUnit.SECONDS.toNanos(5));

        shard.getBulkOperationListener().afterBulk(randomLongBetween(0, ByteSizeValue.ofGb(1).getBytes()), tookNanos);
        assertThat(
            registry.getRecorder().getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, IndexingMetrics.INDEXING_DURATION_HISTOGRAM),
            empty()
        );

        ShardRouting replicaRouting = shard.routingEntry();
        promoteReplica(
            shard,
            Set.of(replicaRouting.allocationId().getId()),
            new IndexShardRoutingTable.Builder(replicaRouting.shardId()).addShard(
                TestShardRouting.newShardRouting(replicaRouting.shardId(), "ignored", true, ShardRoutingState.STARTED)
            ).addShard(replicaRouting).build()
        );
        assertTrue(shard.routingEntry().primary());

        shard.getBulkOperationListener().afterBulk(randomLongBetween(0, ByteSizeValue.ofGb(1).getBytes()), tookNanos);
        List<Measurement> durations = registry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, IndexingMetrics.INDEXING_DURATION_HISTOGRAM);
        assertThat(durations, hasSize(1));
        assertThat(durations.get(0).getDouble(), equalTo(tookNanos / 1_000_000_000.0));
        assertThat(
            durations.get(0).attributes(),
            equalTo(Map.of(MetricAttributes.ES_INDEX_MODE, shard.indexSettings().getMode().getName()))
        );
        closeShards(shard);
    }

    public void testBulkLatencyIsNotRecordedOnPrimaryRelocationTarget() throws IOException {
        ShardRouting relocationTargetRouting = shardRoutingBuilder(
            new ShardId("index", "_na_", 0),
            "local_node",
            true,
            ShardRoutingState.INITIALIZING
        ).withRelocatingNodeId("other_node")
            .withRecoverySource(RecoverySource.PeerRecoverySource.INSTANCE)
            .withAllocationId(AllocationId.newRelocation(AllocationId.newInitializing()))
            .build();
        IndexShard shard = newShard(relocationTargetRouting, DiscoveryNodeUtils.create("other_node"));
        assertTrue(shard.routingEntry().primary());
        assertTrue(shard.routingEntry().isRelocationTarget());

        shard.getBulkOperationListener()
            .afterBulk(randomLongBetween(0, ByteSizeValue.ofGb(1).getBytes()), randomLongBetween(1, TimeUnit.SECONDS.toNanos(5)));
        assertThat(
            registry.getRecorder().getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, IndexingMetrics.INDEXING_DURATION_HISTOGRAM),
            empty()
        );
        closeShards(shard);
    }
}
