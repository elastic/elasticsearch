/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockIndexEventListener;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.junit.After;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RecoveryMetricsIT extends AbstractIndexRecoveryIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestTelemetryPlugin.class);
        plugins.add(MockIndexEventListener.TestPlugin.class);
        return plugins;
    }

    @After
    public void verifyNoOutstandingRecoveriesInStatsAndMetrics() {
        final var nodes = internalCluster().getNodeNames();
        final Predicate<RecoveryStats> noMoreRecoveriesInStats = RecoveryStats::noCurrentRecoveries;
        awaitRecoveryCountStats(Arrays.stream(nodes).collect(Collectors.toMap(node -> node, ignored -> noMoreRecoveriesInStats)));

        final var nodeToTelemetry = Arrays.stream(nodes)
            .collect(
                Collectors.toMap(
                    node -> node,
                    node -> internalCluster().getInstance(PluginsService.class, node)
                        .filterPlugins(TestTelemetryPlugin.class)
                        .findFirst()
                        .orElseThrow()
                )
            );
        final var noMoreRecoveriesInMetrics = Map.of(
            RecoveryMetricsCollector.QUEUED_STORE_RECOVERIES,
            0L,
            RecoveryMetricsCollector.CURRENT_STORE_RECOVERIES,
            0L,
            RecoveryMetricsCollector.QUEUED_PEER_RECOVERIES_AS_SOURCE,
            0L,
            RecoveryMetricsCollector.CURRENT_PEER_RECOVERIES_AS_SOURCE,
            0L,
            RecoveryMetricsCollector.QUEUED_PEER_RECOVERIES_AS_TARGET,
            0L,
            RecoveryMetricsCollector.CURRENT_PEER_RECOVERIES_AS_TARGET,
            0L
        );
        awaitRecoveryCountMetrics(
            nodeToTelemetry,
            Arrays.stream(nodes).collect(Collectors.toMap(node -> node, ignored -> noMoreRecoveriesInMetrics))
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
            .put(PeerRecoverySourceService.INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 1)
            // unthrottle the allocation side on the master
            .put(
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(),
                Integer.MAX_VALUE
            )
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), Integer.MAX_VALUE)
            .put(
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(),
                Integer.MAX_VALUE
            )
            .put(
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(),
                Integer.MAX_VALUE
            )
            .build();
    }

    public void testRecoveryMetricsOnNewIndex() {
        internalCluster().startMasterOnlyNode();
        final var dataNode = internalCluster().startDataOnlyNode();

        final var telemetry = resetAndGetTelemetryPlugin(dataNode);
        final var indexName = randomIndexName();
        createIndex(
            indexName,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen(indexName);
        disableAllocation();

        List<Measurement> recoveryCount = telemetry.getLongCounterMeasurement(RecoveryMetricsCollector.RECOVERY_TOTAL_COUNT_METRIC);
        assertThat("Recovery count measurements", recoveryCount, hasSize(1));
        assertThat("Recovery count", recoveryCount.getFirst().getLong(), equalTo(1L));

        List<Measurement> totalTime = telemetry.getLongHistogramMeasurement(RecoveryMetricsCollector.RECOVERY_TOTAL_TIME_METRIC_IN_SECONDS);
        assertThat("Total time measurements", totalTime, hasSize(1));
        Measurement metric = totalTime.getFirst();
        assertThat("Total time value", metric.getLong(), greaterThanOrEqualTo(0L));
        assertThat("Primary attribute", metric.attributes().get("primary"), equalTo(true));
        assertThat("Recovery type", metric.attributes().get("es_recovery_type"), equalTo("EMPTY_STORE"));
    }

    public void testRecoveryMetricsOnPeerRecovery() {
        internalCluster().startMasterOnlyNode();
        final var sourceNode = internalCluster().startDataOnlyNode();

        // Three single-shard indices. With source max=1 and target max=2, the third index's replica will queue at
        // target while the second queues at source, so all four peer metrics are observable.
        final var indexOne = randomIndexName();
        final var indexTwo = randomIndexName();
        final var indexThree = randomIndexName();
        for (var indexName : List.of(indexOne, indexTwo, indexThree)) {
            createIndex(
                indexName,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
            );
            // Ensure committed segments exist, so FILE_CHUNK actions are issued
            for (int i = 0; i < 50; i++) {
                indexDoc(indexName, Integer.toString(i), "f", randomAlphaOfLength(10));
                refresh(indexName);
            }
            flush(indexName);
        }
        ensureGreen(indexOne, indexTwo, indexThree);

        // Target node has a max of 2 concurrent recoveries slots. Source has a limit of 1.
        // This creates target active=2, target queued=1, source active=1, source queued=1.
        final var targetNode = internalCluster().startDataOnlyNode(
            Settings.builder().put(ThrottlingRecoveryService.INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), 2).build()
        );
        final var targetTelemetry = resetAndGetTelemetryPlugin(targetNode);
        final var sourceTelemetry = resetAndGetTelemetryPlugin(sourceNode);

        final var sourceTransport = MockTransportService.getInstance(sourceNode);
        final var targetTransport = MockTransportService.getInstance(targetNode);
        final var targetTransportService = internalCluster().getInstance(TransportService.class, targetNode);

        final var recoveryBlocked = new CountDownLatch(1);
        final var continueRecovery = new CountDownLatch(1);

        sourceTransport.addSendBehavior(targetTransportService, (connection, requestId, action, request, options) -> {
            if (PeerRecoveryTargetService.Actions.FILE_CHUNK.equals(action)) {
                recoveryBlocked.countDown();
                safeAwait(continueRecovery);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        try {

            for (var indexName : List.of(indexOne, indexTwo, indexThree)) {
                assertAcked(
                    indicesAdmin().prepareUpdateSettings(indexName)
                        .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
                );
            }
            safeAwait(recoveryBlocked);

            awaitRecoveryCountStats(
                Map.of(
                    sourceNode,
                    stats -> stats.currentAsSource() == 1 && stats.currentAsSourceQueued() == 1,
                    targetNode,
                    stats -> stats.currentAsTarget() == 2 && stats.currentAsTargetQueued() == 1
                )
            );
            awaitRecoveryCountMetrics(
                Map.of(sourceNode, sourceTelemetry, targetNode, targetTelemetry),
                Map.of(
                    sourceNode,
                    Map.of(
                        RecoveryMetricsCollector.CURRENT_PEER_RECOVERIES_AS_SOURCE,
                        1L,
                        RecoveryMetricsCollector.QUEUED_PEER_RECOVERIES_AS_SOURCE,
                        1L
                    ),
                    targetNode,
                    Map.of(
                        RecoveryMetricsCollector.CURRENT_PEER_RECOVERIES_AS_TARGET,
                        2L,
                        RecoveryMetricsCollector.QUEUED_PEER_RECOVERIES_AS_TARGET,
                        1L
                    )
                )
            );

            continueRecovery.countDown();
            ensureGreen(indexOne, indexTwo, indexThree);
            disableAllocation();

            List<Measurement> totalTime = targetTelemetry.getLongHistogramMeasurement(
                RecoveryMetricsCollector.RECOVERY_TOTAL_TIME_METRIC_IN_SECONDS
            );
            assertThat("Total time measurements", totalTime, hasSize(greaterThanOrEqualTo(1)));
            Measurement metric = totalTime.getFirst();
            assertThat("Total time value", metric.getLong(), greaterThanOrEqualTo(0L));
            assertThat("Primary attribute", metric.attributes().get("primary"), equalTo(false));
            assertThat("Recovery type", metric.attributes().get("es_recovery_type"), equalTo("PEER"));

            List<Measurement> indexTime = targetTelemetry.getLongHistogramMeasurement(
                RecoveryMetricsCollector.RECOVERY_INDEX_TIME_METRIC_IN_SECONDS
            );
            assertThat("Index time measurements", indexTime, hasSize(greaterThanOrEqualTo(1)));
            assertThat("Index time value", indexTime.getFirst().getLong(), greaterThanOrEqualTo(0L));

            List<Measurement> translogTime = targetTelemetry.getLongHistogramMeasurement(
                RecoveryMetricsCollector.RECOVERY_TRANSLOG_TIME_METRIC_IN_SECONDS
            );
            assertThat("Translog time measurements", translogTime, hasSize(greaterThanOrEqualTo(1)));
            assertThat("Translog time value", translogTime.getFirst().getLong(), greaterThanOrEqualTo(0L));

            awaitRecoveryCountMetrics(
                Map.of(sourceNode, sourceTelemetry, targetNode, targetTelemetry),
                Map.of(
                    sourceNode,
                    Map.of(
                        RecoveryMetricsCollector.CURRENT_PEER_RECOVERIES_AS_SOURCE,
                        0L,
                        RecoveryMetricsCollector.QUEUED_PEER_RECOVERIES_AS_SOURCE,
                        0L
                    ),
                    targetNode,
                    Map.of(
                        RecoveryMetricsCollector.CURRENT_PEER_RECOVERIES_AS_TARGET,
                        0L,
                        RecoveryMetricsCollector.QUEUED_PEER_RECOVERIES_AS_TARGET,
                        0L
                    )
                )
            );
            assertThat(
                "Active outgoing peer recoveries measurements on target",
                targetTelemetry.getLongUpDownCounterMeasurement(RecoveryMetricsCollector.CURRENT_PEER_RECOVERIES_AS_SOURCE),
                hasSize(0)
            );
            assertThat(
                "Queued outgoing peer recoveries measurements on target",
                targetTelemetry.getLongUpDownCounterMeasurement(RecoveryMetricsCollector.QUEUED_PEER_RECOVERIES_AS_SOURCE),
                hasSize(0)
            );
        } finally {
            sourceTransport.clearAllRules();
            targetTransport.clearAllRules();
        }
    }

    public void testRecoveryMetricsOnPrimaryRelocation() {
        internalCluster().startMasterOnlyNode();
        final var node1 = internalCluster().startDataOnlyNode();
        final var node2 = internalCluster().startDataOnlyNode();

        final var indexName = randomIndexName();
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_name", node1)
                .build()
        );
        ensureGreen(indexName);

        indexRandom(
            true,
            false,
            IntStream.range(0, randomIntBetween(10, 100)).mapToObj(i -> prepareIndex(indexName).setSource("field", "value" + i)).toList()
        );
        flush(indexName);

        final var node1Telemetry = resetAndGetTelemetryPlugin(node1);
        final var node2Telemetry = resetAndGetTelemetryPlugin(node2);

        final var node1Transport = MockTransportService.getInstance(node1);
        final var node2Transport = MockTransportService.getInstance(node2);
        final var node2TransportService = internalCluster().getInstance(TransportService.class, node2);

        final var recoveryBlocked = new CountDownLatch(1);
        final var continueRecovery = new CountDownLatch(1);

        node1Transport.addSendBehavior(node2TransportService, (connection, requestId, action, request, options) -> {
            if (PeerRecoveryTargetService.Actions.FILE_CHUNK.equals(action)) {
                recoveryBlocked.countDown();
                safeAwait(continueRecovery);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        try {
            assertAcked(
                indicesAdmin().prepareUpdateSettings(indexName)
                    .setSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_name", node2))
            );
            safeAwait(recoveryBlocked);
            awaitRecoveryCountMetrics(node1, node1Telemetry, Map.of(RecoveryMetricsCollector.CURRENT_PEER_RECOVERIES_AS_SOURCE, 1L));
            assertThat(
                "Active peer recoveries measurements on source",
                node1Telemetry.getLongUpDownCounterMeasurement(RecoveryMetricsCollector.CURRENT_PEER_RECOVERIES_AS_SOURCE),
                hasSize(1)
            );

            continueRecovery.countDown();
            ensureGreen(indexName);
            disableAllocation();

            List<Measurement> totalTime = node2Telemetry.getLongHistogramMeasurement(
                RecoveryMetricsCollector.RECOVERY_TOTAL_TIME_METRIC_IN_SECONDS
            );
            assertThat("Total time measurements", totalTime, hasSize(1));
            Measurement metric = totalTime.getFirst();
            assertThat("Total time value", metric.getLong(), greaterThanOrEqualTo(0L));
            assertThat("Primary attribute", metric.attributes().get("primary"), equalTo(true));
            assertThat("Recovery type", metric.attributes().get("es_recovery_type"), equalTo("PEER"));

            awaitRecoveryCountMetrics(node1, node1Telemetry, Map.of(RecoveryMetricsCollector.CURRENT_PEER_RECOVERIES_AS_SOURCE, 0L));

            assertThat(
                "Active peer recoveries measurements on source (after relocation completed)",
                node1Telemetry.getLongUpDownCounterMeasurement(RecoveryMetricsCollector.CURRENT_PEER_RECOVERIES_AS_SOURCE),
                hasSize(greaterThanOrEqualTo(1))
            );

            List<Measurement> outgoingPeerNode2 = node2Telemetry.getLongUpDownCounterMeasurement(
                RecoveryMetricsCollector.CURRENT_PEER_RECOVERIES_AS_SOURCE
            );
            assertThat(
                "Active outgoing peer recoveries measurements on target (after relocation completed)",
                outgoingPeerNode2,
                hasSize(0)
            );
            outgoingPeerNode2 = node2Telemetry.getLongUpDownCounterMeasurement(RecoveryMetricsCollector.QUEUED_PEER_RECOVERIES_AS_SOURCE);
            assertThat(
                "Queued outgoing peer recoveries measurements on target (after relocation completed)",
                outgoingPeerNode2,
                hasSize(0)
            );
        } finally {
            node1Transport.clearAllRules();
            node2Transport.clearAllRules();
        }
    }

    public void testRecoveryMetricsOnThrottledStoreRecovery() {
        final var node = internalCluster().startNode(
            Settings.builder().put(ThrottlingRecoveryService.INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), 1).build()
        );
        final var telemetry = resetAndGetTelemetryPlugin(node);

        final var indexOne = randomIndexName();
        final var indexTwo = randomIndexName();

        final var firstRecoveryStarted = new CountDownLatch(1);
        final var proceedWithFirstRecovery = new CountDownLatch(1);

        final IndexEventListener indexEventListener = new IndexEventListener() {
            @Override
            public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings, ActionListener<Void> listener) {
                if (indexShard.shardId().getIndexName().equals(indexOne)) {
                    firstRecoveryStarted.countDown();
                    safeAwait(proceedWithFirstRecovery);
                }
                listener.onResponse(null);
            }
        };
        internalCluster().getInstance(MockIndexEventListener.TestEventListener.class, node).setNewDelegate(indexEventListener);

        // First recovery holds the only slot
        assertAcked(prepareCreate(indexOne).setSettings(indexSettings(1, 0).build()).setWaitForActiveShards(ActiveShardCount.NONE));
        safeAwait(firstRecoveryStarted);

        // Second recovery is queued
        assertAcked(prepareCreate(indexTwo).setSettings(indexSettings(1, 0).build()).setWaitForActiveShards(ActiveShardCount.NONE));
        awaitRecoveryCountStats(Map.of(node, stats -> stats.currentFromStore() == 1 && stats.currentFromStoreQueued() == 1));
        awaitRecoveryCountMetrics(
            node,
            telemetry,
            Map.of(RecoveryMetricsCollector.CURRENT_STORE_RECOVERIES, 1L, RecoveryMetricsCollector.QUEUED_STORE_RECOVERIES, 1L)
        );

        proceedWithFirstRecovery.countDown();
        ensureGreen(indexOne, indexTwo);
        disableAllocation();

        awaitRecoveryCountMetrics(
            node,
            telemetry,
            Map.of(RecoveryMetricsCollector.CURRENT_STORE_RECOVERIES, 0L, RecoveryMetricsCollector.QUEUED_STORE_RECOVERIES, 0L)

        );
    }

    public void testDirectCancellationMetricsPreQueued() throws Exception {
        final var node = internalCluster().startNode();
        final var nodeTelemetry = resetAndGetTelemetryPlugin(node);

        final var index = randomIndexName();

        // Pre-queued cancellation
        final var shardCreationBlocked = new CountDownLatch(1);
        final var proceedWithShardCreation = new CountDownLatch(1);
        final var indexShardRouting = new AtomicReference<ShardRouting>();

        final IndexEventListener indexEventListener = new IndexEventListener() {
            @Override
            public void beforeIndexShardCreated(ShardRouting shardRouting, Settings indexSettings) {
                if (shardRouting.getIndexName().equals(index)) {
                    indexShardRouting.set(shardRouting);
                    shardCreationBlocked.countDown();
                    safeAwait(proceedWithShardCreation);
                }
            }
        };
        internalCluster().getInstance(MockIndexEventListener.TestEventListener.class, node).setNewDelegate(indexEventListener);

        prepareCreate(index).setSettings(indexSettings(1, 0)).execute();
        safeAwait(shardCreationBlocked);

        client(node).execute(
            CancelRecoveriesAction.TYPE,
            new CancelRecoveriesAction.Request(
                0L,
                List.of(
                    new CancelRecoveriesAction.ShardRecoveryCancellation(
                        indexShardRouting.get().shardId(),
                        indexShardRouting.get().allocationId().getId(),
                        false
                    )
                )
            )
        ).get();

        // Unblock shard creation, enqueue() will find the allocation ID already cancelled.
        proceedWithShardCreation.countDown();

        awaitRecoveryCountMetrics(node, nodeTelemetry, Map.of(RecoveryMetricsCollector.RECOVERY_DIRECT_CANCELLATIONS_METRIC, 1L));
        List<Measurement> cancellations = nodeTelemetry.getLongCounterMeasurement(
            RecoveryMetricsCollector.RECOVERY_DIRECT_CANCELLATIONS_METRIC
        );
        assertThat("Direct cancellation measurements after pre-queued cancellation", cancellations, hasSize(1));
        assertThat(cancellations.getFirst().attributes().get("es_recovery_type"), equalTo("EMPTY_STORE"));
        assertThat(cancellations.getFirst().attributes().get("es_recovery_scheduling_state"), equalTo("QUEUED"));
    }

    public void testDirectCancellationMetricsQueuedAndStarted() throws Exception {
        final var node1 = internalCluster().startNode();
        final var node2 = internalCluster().startNode(
            Settings.builder().put(ThrottlingRecoveryService.INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), 1).build()
        );
        final var node2Telemetry = resetAndGetTelemetryPlugin(node2);

        final var indexOne = randomIndexName();
        final var indexTwo = randomIndexName();
        final var indexThree = randomIndexName();

        createIndex(
            indexOne,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_name", node1)
                .build()
        );
        ensureGreen(indexOne);

        final var recoveryBlocked = new CountDownLatch(1);
        final var proceedWithBlockedRecovery = new CountDownLatch(1);

        final IndexEventListener indexEventListener = new IndexEventListener() {
            @Override
            public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings, ActionListener<Void> listener) {
                if (indexShard.shardId().getIndexName().equals(indexTwo)) {
                    recoveryBlocked.countDown();
                    safeAwait(proceedWithBlockedRecovery);
                }
                listener.onResponse(null);
            }
        };
        internalCluster().getInstance(MockIndexEventListener.TestEventListener.class, node2).setNewDelegate(indexEventListener);

        // indexTwo's recovery is started and holds the only slot on `node2`.
        assertAcked(
            prepareCreate(indexTwo).setSettings(
                indexSettings(1, 0).put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_name", node2)
            ).setWaitForActiveShards(ActiveShardCount.NONE)
        );
        safeAwait(recoveryBlocked);

        // indexThree's recovery is queued behind indexTwo.
        assertAcked(
            prepareCreate(indexThree).setSettings(
                indexSettings(1, 0).put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_name", node2)
            ).setWaitForActiveShards(ActiveShardCount.NONE)
        );

        // Reroute indexOne onto `node2`, queues an indexOne PEER recovery behind indexThree
        updateIndexSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_name", node2), indexOne);
        awaitRecoveryCountStats(
            Map.of(
                node2,
                stats -> stats.currentFromStore() == 1 && stats.currentFromStoreQueued() == 1 && stats.currentAsTargetQueued() == 1
            )
        );
        disableAllocation();

        final var indicesService = internalCluster().getInstance(IndicesService.class, node2);

        // Directly cancel the queued EMPTY_STORE recovery.
        final var queuedStoreShardId = new ShardId(resolveIndex(indexThree), 0);
        final var queuedStoreAllocationId = indicesService.indexServiceSafe(queuedStoreShardId.getIndex())
            .getShard(0)
            .routingEntry()
            .allocationId()
            .getId();

        final var clusterService = internalCluster().getInstance(ClusterService.class, node2);
        client(node2).execute(
            CancelRecoveriesAction.TYPE,
            new CancelRecoveriesAction.Request(
                clusterService.state().version(),
                List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(queuedStoreShardId, queuedStoreAllocationId, false))
            )
        ).get();

        awaitRecoveryCountMetrics(node2, node2Telemetry, Map.of(RecoveryMetricsCollector.RECOVERY_DIRECT_CANCELLATIONS_METRIC, 1L));
        List<Measurement> cancellations = node2Telemetry.getLongCounterMeasurement(
            RecoveryMetricsCollector.RECOVERY_DIRECT_CANCELLATIONS_METRIC
        );
        assertThat("Direct cancellation measurements after queued store cancellation", cancellations, hasSize(1));
        assertThat(cancellations.getFirst().attributes().get("es_recovery_type"), equalTo("EMPTY_STORE"));
        assertThat(cancellations.getFirst().attributes().get("es_recovery_scheduling_state"), equalTo("QUEUED"));

        // Directly cancel the queued PEER recovery.
        final var queuedPeerShardId = new ShardId(resolveIndex(indexOne), 0);
        final var queuedPeerAllocationId = indicesService.indexServiceSafe(queuedPeerShardId.getIndex())
            .getShard(0)
            .routingEntry()
            .allocationId()
            .getId();
        client(node2).execute(
            CancelRecoveriesAction.TYPE,
            new CancelRecoveriesAction.Request(
                clusterService.state().version(),
                List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(queuedPeerShardId, queuedPeerAllocationId, false))
            )
        ).get();

        awaitRecoveryCountMetrics(node2, node2Telemetry, Map.of(RecoveryMetricsCollector.RECOVERY_DIRECT_CANCELLATIONS_METRIC, 2L));
        cancellations = node2Telemetry.getLongCounterMeasurement(RecoveryMetricsCollector.RECOVERY_DIRECT_CANCELLATIONS_METRIC);
        assertThat("Direct cancellation measurements after queued peer recovery cancellation", cancellations, hasSize(2));
        final Measurement queuedPeerMeasurement = cancellations.stream()
            .filter(measurement -> "PEER".equals(measurement.attributes().get("es_recovery_type")))
            .findFirst()
            .orElseThrow();
        assertThat(queuedPeerMeasurement.attributes().get("es_recovery_scheduling_state"), equalTo("QUEUED"));
        assertThat(queuedPeerMeasurement.getLong(), equalTo(1L));

        // Directly cancel the started recovery
        final var startedShardId = new ShardId(resolveIndex(indexTwo), 0);
        final var startedAllocationId = indicesService.indexServiceSafe(startedShardId.getIndex())
            .getShard(0)
            .routingEntry()
            .allocationId()
            .getId();
        client(node2).execute(
            CancelRecoveriesAction.TYPE,
            new CancelRecoveriesAction.Request(
                clusterService.state().version(),
                List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(startedShardId, startedAllocationId, true))
            )
        ).get();
        proceedWithBlockedRecovery.countDown();

        awaitRecoveryCountMetrics(node2, node2Telemetry, Map.of(RecoveryMetricsCollector.RECOVERY_DIRECT_CANCELLATIONS_METRIC, 3L));
        cancellations = node2Telemetry.getLongCounterMeasurement(RecoveryMetricsCollector.RECOVERY_DIRECT_CANCELLATIONS_METRIC);
        assertThat("Direct cancellation measurements after started recovery cancellation", cancellations, hasSize(3));
        final Measurement startedMeasurement = cancellations.stream()
            .filter(measurement -> "STARTED".equals(measurement.attributes().get("es_recovery_scheduling_state")))
            .findFirst()
            .orElseThrow();
        assertThat(startedMeasurement.attributes().get("es_recovery_type"), equalTo("EMPTY_STORE"));
        assertThat(startedMeasurement.getLong(), equalTo(1L));
    }

    private TestTelemetryPlugin resetAndGetTelemetryPlugin(String node) {
        final TestTelemetryPlugin plugin = internalCluster().getInstance(PluginsService.class, node)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        plugin.resetMeter();
        return plugin;
    }
}
