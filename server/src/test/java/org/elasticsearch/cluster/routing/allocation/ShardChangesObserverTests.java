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
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.MoveReason;
import org.elasticsearch.cluster.routing.MoveType;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.MetricRecorder;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.metric.Instrument;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.elasticsearch.common.settings.ClusterSettings.createBuiltInClusterSettings;
import static org.elasticsearch.test.MockLog.assertThatLogger;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasEntry;
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
                "[" + indexName + "][0][P] is relocating (" + MoveType.CANNOT_REMAIN.reason() + ") from [node-1] to [node-2]"
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
        final var observer = new ShardChangesObserver(meterRegistry, nowMillis::get, ShardMovementListener.NOOP);

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
        final var observer = new ShardChangesObserver(meterRegistry, System::currentTimeMillis, ShardMovementListener.NOOP);
        final var recorder = meterRegistry.getRecorder();

        final var shardId = new ShardId("test-index", "_na_", 0);
        final var primary = randomBoolean();
        final var startedShard = shardRoutingBuilder(shardId, "node-1", primary, ShardRoutingState.STARTED).build();
        final var targetShard = shardRoutingBuilder(shardId, "node-2", primary, ShardRoutingState.INITIALIZING).build();

        assertThat(recorder.getMeasurements(InstrumentType.LONG_COUNTER, ShardChangesObserver.RELOCATION_STARTED_METRIC), hasSize(0));

        final var reason = new MoveReason(randomFrom(MoveType.values()), randomBoolean() ? null : randomIdentifier());
        final var moveCount = randomIntBetween(1, 10);
        for (int i = 0; i < moveCount; i++) {
            observer.relocationStarted(startedShard, targetShard, reason);
        }
        final var attributes = assertMovementCount(recorder, moveCount);
        assertThat(attributes, everyItem(hasEntry("es_relocation_reason", reason.type().reason())));
        assertThat(
            attributes,
            everyItem(hasEntry("es_can_remain_decider", Objects.requireNonNullElse(reason.canRemainDecider(), "missing")))
        );
        assertThat(attributes, everyItem(hasEntry("es_shard_primary", primary)));
        // the attribute map is cached
        assertThat(attributes, everyItem(sameInstance(attributes.getFirst())));
    }

    public void testRelocationMetricReportsComputedReason() {
        final var allocationId = randomUUID();
        final var indexMetadata = IndexMetadata.builder(randomIdentifier())
            .settings(indexSettings(IndexVersion.current(), 1, 0).put("index.routing.allocation.exclude._id", "node-1"))
            .putInSyncAllocationIds(0, Set.of(allocationId))
            .build();
        final var preserveLabels = randomBoolean();
        final var desiredBalance = randomBoolean();
        final var settings = Settings.builder()
            .put(AllocationService.PRESERVE_DECISION_LABELS_SETTING.getKey(), preserveLabels)
            .put(
                ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING.getKey(),
                desiredBalance ? ClusterModule.DESIRED_BALANCE_ALLOCATOR : ClusterModule.BALANCED_ALLOCATOR
            )
            .build();

        final var attributes = relocationAttributesAfterReroute(indexMetadata, allocationId, settings, List.of());
        assertThat(attributes, hasEntry("es_relocation_reason", MoveType.CANNOT_REMAIN.reason()));
        assertThat(attributes, hasEntry("es_can_remain_decider", preserveLabels ? "filter" : "missing"));
        assertThat(attributes, hasEntry("es_shard_primary", true));

        // the event carries the same reason
        final var relocation = listener.events.stream().filter(e -> e.getFirst().equals("relocationStarted")).findFirst().orElseThrow();
        assertThat(((ShardRouting) relocation.get(1)).currentNodeId(), equalTo("node-1"));
        assertThat(((ShardRouting) relocation.get(2)).currentNodeId(), equalTo("node-2"));
        assertThat(relocation.get(3), equalTo(new MoveReason(MoveType.CANNOT_REMAIN, preserveLabels ? "filter" : null)));
        assertThat(listener.events.getLast().getFirst(), equalTo("relocationCompleted"));
    }

    public void testEventsForwardedToListener() {
        final var observer = new ShardChangesObserver(new RecordingMeterRegistry(), System::currentTimeMillis, listener);

        final var shardId = new ShardId("test-index", "_na_", 0);
        final var primary = randomBoolean();
        final var unassignedShard = shardRoutingBuilder(shardId, null, primary, ShardRoutingState.UNASSIGNED).build();
        final var initializingShard = shardRoutingBuilder(shardId, "node-1", primary, ShardRoutingState.INITIALIZING).build();
        final var startedShard = shardRoutingBuilder(shardId, "node-1", primary, ShardRoutingState.STARTED).build();
        final var relocatingShard = shardRoutingBuilder(shardId, "node-1", primary, ShardRoutingState.RELOCATING).withRelocatingNodeId(
            "node-2"
        ).build();
        final var targetShard = shardRoutingBuilder(shardId, "node-2", primary, ShardRoutingState.INITIALIZING).build();
        final var reason = new MoveReason(randomFrom(MoveType.values()), randomBoolean() ? null : randomIdentifier());
        final var unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, "boom");

        observer.shardInitialized(unassignedShard, initializingShard);
        observer.shardStarted(initializingShard, startedShard);
        observer.relocationStarted(startedShard, targetShard, reason);
        observer.relocationCompleted(relocatingShard);
        observer.shardFailed(startedShard, unassignedInfo);

        assertThat(
            listener.events,
            contains(
                List.of("shardInitialized", unassignedShard, initializingShard),
                List.of("shardStarted", initializingShard, startedShard),
                List.of("relocationStarted", startedShard, targetShard, reason),
                List.of("relocationCompleted", relocatingShard),
                List.of("shardFailed", startedShard, unassignedInfo)
            )
        );
    }

    public void testRelocationMetricReportsComputedReasonWhenReconcilerDisagrees() {
        // a decider that only objects to the shard's node while simulating, so the computation decides a cannot-remain move that the
        // reconciler then makes as a rebalance
        final var simulationOnlyDecider = new AllocationDecider() {
            @Override
            public Decision canRemain(
                IndexMetadata indexMetadata,
                ShardRouting shardRouting,
                RoutingNode node,
                RoutingAllocation allocation
            ) {
                return allocation.isSimulating() && node.nodeId().equals("node-1")
                    ? allocation.decision(Decision.NO, "simulation_only", "no")
                    : allocation.decision(Decision.YES, "simulation_only", "yes");
            }
        };
        final var allocationId = randomUUID();
        final var indexMetadata = IndexMetadata.builder(randomIdentifier())
            .settings(indexSettings(IndexVersion.current(), 1, 0))
            .putInSyncAllocationIds(0, Set.of(allocationId))
            .build();
        final var settings = Settings.builder()
            .put(AllocationService.PRESERVE_DECISION_LABELS_SETTING.getKey(), true)
            .put(ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING.getKey(), ClusterModule.DESIRED_BALANCE_ALLOCATOR)
            .build();
        final var plugins = List.<ClusterPlugin>of(new ClusterPlugin() {
            @Override
            public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
                return List.of(simulationOnlyDecider);
            }
        });

        final var attributes = relocationAttributesAfterReroute(indexMetadata, allocationId, settings, plugins);
        assertThat(attributes, hasEntry("es_relocation_reason", MoveType.CANNOT_REMAIN.reason()));
        assertThat(attributes, hasEntry("es_can_remain_decider", "simulation_only"));
    }

    /** receives the events emitted by the allocation service built by {@link #relocationAttributesAfterReroute} */
    private final ShardMovementListenerTests.RecordingListener listener = new ShardMovementListenerTests.RecordingListener();

    /**
     * Reroutes a one-shard cluster where the shard starts on node-1 and returns the attributes of the single relocation the allocation
     * service with the given settings and plugins starts. The events emitted along the way are collected by {@link #listener}.
     */
    private Map<String, Object> relocationAttributesAfterReroute(
        IndexMetadata indexMetadata,
        String allocationId,
        Settings settings,
        List<ClusterPlugin> clusterPlugins
    ) {
        final var shard = shardRoutingBuilder(new ShardId(indexMetadata.getIndex(), 0), "node-1", true, ShardRoutingState.STARTED)
            .withAllocationId(AllocationId.newInitializing(allocationId))
            .build();
        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(newNode("node-1")).add(newNode("node-2")))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder().add(IndexRoutingTable.builder(indexMetadata.getIndex()).addShard(shard)))
            .build();
        final var clusterSettings = createBuiltInClusterSettings(settings);
        final var meterRegistry = new RecordingMeterRegistry();
        final var allocationService = new AllocationService(
            randomAllocationDeciders(settings, clusterSettings, clusterPlugins),
            createShardsAllocator(settings),
            EmptyClusterInfoService.INSTANCE,
            SNAPSHOT_INFO_SERVICE_WITH_NO_SHARD_SIZES,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY,
            meterRegistry,
            clusterSettings,
            listener
        );
        allocationService.setExistingShardsAllocators(Map.of(GatewayAllocator.ALLOCATOR_NAME, new TestGatewayAllocator()));
        applyStartedShardsUntilNoChange(clusterState, allocationService);
        return assertMovementCount(meterRegistry.getRecorder(), 1).getFirst();
    }

    private static List<Map<String, Object>> assertMovementCount(MetricRecorder<Instrument> recorder, long expectedCount) {
        final var measurements = recorder.getMeasurements(InstrumentType.LONG_COUNTER, ShardChangesObserver.RELOCATION_STARTED_METRIC);
        assertThat(measurements.stream().mapToLong(Measurement::getLong).sum(), is(expectedCount));
        return measurements.stream().map(Measurement::attributes).toList();
    }
}
