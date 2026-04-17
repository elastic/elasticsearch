/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.elasticsearch.test.MockLog.assertThatLogger;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

@TestLogging(value = "org.elasticsearch.cluster.routing.allocation.ShardChangesObserver:TRACE", reason = "verifies debug level logging")
public class ShardChangesObserverTests extends ESAllocationTestCase {

    public void testLogShardStarting() {
        var indexName = randomIdentifier();
        var indexMetadata = IndexMetadata.builder(indexName).settings(indexSettings(IndexVersion.current(), 1, 0)).build();

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(newNode("node-1")))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(indexMetadata))
            .build();

        assertThatLogger(
            () -> applyStartedShardsUntilNoChange(clusterState, createAllocationService()),
            ShardChangesObserver.class,
            new MockLog.SeenEventExpectation(
                "Should log shard initializing",
                ShardChangesObserver.class.getCanonicalName(),
                Level.TRACE,
                "[" + indexName + "][0][P] initializing from " + RecoverySource.Type.EMPTY_STORE + " on node [node-1]"
            ),
            new MockLog.SeenEventExpectation(
                "Should log shard starting",
                ShardChangesObserver.class.getCanonicalName(),
                Level.DEBUG,
                "[" + indexName + "][0][P] started from " + RecoverySource.Type.EMPTY_STORE + " on node [node-1]"
            )
        );
    }

    public void testLogShardMovement() {
        var allocationId = randomUUID();
        var indexName = randomIdentifier();
        var indexMetadata = IndexMetadata.builder(indexName)
            .settings(indexSettings(IndexVersion.current(), 1, 0).put("index.routing.allocation.exclude._id", "node-1"))
            .putInSyncAllocationIds(0, Set.of(allocationId))
            .build();

        ShardRouting shard = shardRoutingBuilder(new ShardId(indexMetadata.getIndex(), 0), "node-1", true, ShardRoutingState.STARTED)
            .withAllocationId(AllocationId.newInitializing(allocationId))
            .build();

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(newNode("node-1")).add(newNode("node-2")))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder().add(IndexRoutingTable.builder(indexMetadata.getIndex()).addShard(shard)))
            .build();

        assertThatLogger(
            () -> applyStartedShardsUntilNoChange(clusterState, createAllocationService()),
            ShardChangesObserver.class,
            new MockLog.SeenEventExpectation(
                "Should log shard moving",
                ShardChangesObserver.class.getCanonicalName(),
                Level.DEBUG,
                "["
                    + indexName
                    + "][0][P] is relocating ("
                    + BalancedShardsAllocator.MOVE_CANNOT_REMAIN_REASON
                    + ") from [node-1] to [node-2]"
            ),
            new MockLog.SeenEventExpectation(
                "Should log shard starting",
                ShardChangesObserver.class.getCanonicalName(),
                Level.DEBUG,
                "[" + indexName + "][0][P] started from " + RecoverySource.Type.PEER + " on node [node-2]"
            )
        );
    }

    public void testLogShardFailureAndPromotion() {

        var allocationId1 = randomUUID();
        var allocationId2 = randomUUID();
        var indexName = randomIdentifier();
        var indexMetadata = IndexMetadata.builder(indexName)
            .settings(indexSettings(IndexVersion.current(), 1, 1))
            .putInSyncAllocationIds(0, Set.of(allocationId1, allocationId2))
            .build();

        ShardRouting shard1 = shardRoutingBuilder(new ShardId(indexMetadata.getIndex(), 0), "node-1", true, ShardRoutingState.STARTED)
            .withAllocationId(AllocationId.newInitializing(allocationId1))
            .build();

        ShardRouting shard2 = shardRoutingBuilder(new ShardId(indexMetadata.getIndex(), 0), "node-2", false, ShardRoutingState.STARTED)
            .withAllocationId(AllocationId.newInitializing(allocationId1))
            .build();

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(newNode("node-2"))) // node-1 left the cluster
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder().add(IndexRoutingTable.builder(indexMetadata.getIndex()).addShard(shard1).addShard(shard2)))
            .build();

        assertThatLogger(
            () -> createAllocationService().disassociateDeadNodes(clusterState, true, "node-1 left cluster"),
            ShardChangesObserver.class,
            new MockLog.SeenEventExpectation(
                "Should log shard moving",
                ShardChangesObserver.class.getCanonicalName(),
                Level.DEBUG,
                "[" + indexName + "][0][R] is promoted to primary on [node-2]"
            ),
            new MockLog.SeenEventExpectation(
                "Should log shard starting",
                ShardChangesObserver.class.getCanonicalName(),
                Level.DEBUG,
                "[" + indexName + "][0][P] has failed on [node-1]: NODE_LEFT"
            )
        );
    }

    public void testUnassignedMetrics() {
        final var meterRegistry = new RecordingMeterRegistry();
        final long unassignedAtMillis = randomLongBetween(0, System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1));
        final AtomicLong nowMillis = new AtomicLong(unassignedAtMillis);
        final var observer = new ShardChangesObserver(meterRegistry, nowMillis::get);

        final var reason = randomFrom(UnassignedInfo.Reason.values());
        // ALLOCATION_FAILED needs failedAllocations > 0
        final int failedAllocations = reason == UnassignedInfo.Reason.ALLOCATION_FAILED ? randomIntBetween(1, 5) : 0;
        // NODE_RESTARTING needs lastAllocatedNodeId
        final String lastAllocatedNodeId = reason == UnassignedInfo.Reason.NODE_RESTARTING ? randomIdentifier() : null;
        final var unassignedInfo = new UnassignedInfo(
            reason,
            null,
            null,
            failedAllocations,
            System.nanoTime(),
            unassignedAtMillis,
            false,
            UnassignedInfo.AllocationStatus.NO_ATTEMPT,
            Collections.emptySet(),
            lastAllocatedNodeId
        );
        final var shardId = new ShardId("test-index", "_na_", 0);
        final var primary = randomBoolean();
        final var unassignedShard = shardRoutingBuilder(shardId, null, primary, ShardRoutingState.UNASSIGNED).withUnassignedInfo(
            unassignedInfo
        ).build();

        final var recorder = meterRegistry.getRecorder();
        assertThat(
            recorder.getMeasurements(InstrumentType.LONG_HISTOGRAM, ShardChangesObserver.UNASSIGNED_TO_INITIALIZING_METRIC).size(),
            equalTo(0)
        );
        assertThat(
            recorder.getMeasurements(InstrumentType.LONG_HISTOGRAM, ShardChangesObserver.UNASSIGNED_TO_STARTED_METRIC).size(),
            equalTo(0)
        );

        // replica shards must recover from primary
        final var recoverySource = primary ? RecoverySource.EmptyStoreRecoverySource.INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE;
        final var initializedShard = shardRoutingBuilder(shardId, "node-1", primary, ShardRoutingState.INITIALIZING).withRecoverySource(
            recoverySource
        ).withUnassignedInfo(unassignedInfo).build();
        final long initializedTimeMillis = System.currentTimeMillis();
        nowMillis.set(initializedTimeMillis);
        observer.shardInitialized(unassignedShard, initializedShard);

        final var startedShard = shardRoutingBuilder(shardId, "node-1", primary, ShardRoutingState.STARTED).build();
        final long startedTimeMillis = initializedTimeMillis + randomLongBetween(0, 1000L);
        nowMillis.set(startedTimeMillis);
        observer.shardStarted(initializedShard, startedShard);

        final List<Measurement> initializedMetrics = recorder.getMeasurements(
            InstrumentType.LONG_HISTOGRAM,
            ShardChangesObserver.UNASSIGNED_TO_INITIALIZING_METRIC
        );
        assertThat(initializedMetrics, hasSize(1));
        final var initializedMetricValue = initializedMetrics.getFirst();
        assertThat(initializedMetricValue.getLong(), equalTo(Math.max(0, initializedTimeMillis - unassignedAtMillis)));
        assertThat(initializedMetricValue.attributes().get("es_shard_primary"), equalTo(primary));
        assertThat(initializedMetricValue.attributes().get("es_shard_reason"), equalTo(reason.name()));

        final List<Measurement> initializingCounterMetrics = recorder.getMeasurements(
            InstrumentType.LONG_COUNTER,
            ShardChangesObserver.SHARD_INITIALIZING_METRIC
        );
        assertThat(initializingCounterMetrics, hasSize(1));
        assertThat(initializingCounterMetrics.getFirst().getLong(), is(1L));
        assertThat(initializingCounterMetrics.getFirst().attributes().get("es_shard_primary"), equalTo(primary));
        assertThat(initializingCounterMetrics.getFirst().attributes().get("es_shard_reason"), equalTo(reason.name()));

        final List<Measurement> startedMetrics = recorder.getMeasurements(
            InstrumentType.LONG_HISTOGRAM,
            ShardChangesObserver.UNASSIGNED_TO_STARTED_METRIC
        );
        assertThat(startedMetrics, hasSize(1));
        final var startedMetricValue = startedMetrics.getFirst();
        assertThat(startedMetricValue.getLong(), equalTo(Math.max(0, startedTimeMillis - unassignedAtMillis)));
        assertThat(startedMetricValue.attributes().get("es_shard_primary"), equalTo(primary));
        assertThat(startedMetricValue.attributes().get("es_shard_reason"), equalTo(reason.name()));
    }

    public void testRelocationStartedMetric() {
        final var meterRegistry = new RecordingMeterRegistry();
        final var observer = new ShardChangesObserver(meterRegistry, System::currentTimeMillis);
        final var recorder = meterRegistry.getRecorder();

        final var shardId = new ShardId("test-index", "_na_", 0);
        final var startedShard = shardRoutingBuilder(shardId, "node-1", true, ShardRoutingState.STARTED).build();
        final var targetShard = shardRoutingBuilder(shardId, "node-2", true, ShardRoutingState.INITIALIZING).build();

        assertThat(recorder.getMeasurements(InstrumentType.LONG_COUNTER, ShardChangesObserver.RELOCATION_STARTED_METRIC), hasSize(0));

        // known reasons
        final var moveCounts = new HashMap<String, Integer>();
        for (final var reason : ShardChangesObserver.RELOCATION_ATTRIBUTES.keySet()) {
            final int moveCount = randomIntBetween(0, 10);
            moveCounts.put(reason, moveCount);
            for (int i = 0; i < moveCount; i++) {
                observer.relocationStarted(startedShard, targetShard, reason);
            }
        }
        List<Measurement> measurements = recorder.getMeasurements(
            InstrumentType.LONG_COUNTER,
            ShardChangesObserver.RELOCATION_STARTED_METRIC
        );
        for (final var entry : moveCounts.entrySet()) {
            assertMovementCountForReason(measurements, entry.getKey(), entry.getValue());
            assertThat(
                measurements.stream()
                    .map(Measurement::attributes)
                    .filter(attributes -> attributes.get("es_relocation_reason").equals(entry.getKey()))
                    .toList(),
                everyItem(sameInstance(ShardChangesObserver.RELOCATION_ATTRIBUTES.get(entry.getKey())))
            );
        }

        // unknown reason: falls back to dynamic Map.of
        final var unknownReason = randomIdentifier("unknown-");
        final var movements = randomIntBetween(0, 10);
        for (int i = 0; i < movements; i++) {
            observer.relocationStarted(startedShard, targetShard, unknownReason);
        }
        measurements = recorder.getMeasurements(InstrumentType.LONG_COUNTER, ShardChangesObserver.RELOCATION_STARTED_METRIC);
        assertMovementCountForReason(measurements, unknownReason, movements);
    }

    private void assertMovementCountForReason(List<Measurement> measurements, String reason, long expectedCount) {
        long movementCount = measurements.stream()
            .filter(m -> m.attributes().get("es_relocation_reason").equals(reason))
            .mapToLong(Measurement::getLong)
            .sum();
        assertThat(movementCount, is(expectedCount));
    }
}
