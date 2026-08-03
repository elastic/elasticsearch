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
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.MockIndexEventListener;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;

/// Integration tests for source-side (see [PeerRecoverySourceService]) and target-side recovery queues (see [ThrottlingRecoveryService])
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class IndexThrottlingRecoveryIT extends AbstractIndexRecoveryIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(super.nodePlugins(), MockIndexEventListener.TestPlugin.class);
    }

    /// Verifies that the source node queues peer recovery requests that exceed
    /// [PeerRecoverySourceService#INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING], and that all queued recoveries
    /// eventually complete successfully once slots become free.
    public void testSourceNodeQueuesRecoveriesPastConcurrencyLimit() throws Exception {
        internalCluster().startMasterOnlyNode();
        final int sourceConcurrentRecoveryLimit = 1;
        final var sourceNode = internalCluster().startDataOnlyNode(
            Settings.builder()
                .put(
                    PeerRecoverySourceService.INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(),
                    sourceConcurrentRecoveryLimit
                )
                .build()
        );
        final int numShards = sourceConcurrentRecoveryLimit + 1;
        final var indexName = "test-queued";
        createIndex(indexName, indexSettings(numShards, 0).build());

        // Ensure committed segments exist, so FILE_CHUNK actions are issued
        for (int i = 0; i < 50; i++) {
            indexDoc(indexName, Integer.toString(i), "f", randomAlphaOfLength(10));
            refresh(indexName);
        }
        flush(indexName);
        ensureGreen(indexName);

        final var fileChunkLatch = new CountDownLatch(1);
        final var transportService = MockTransportService.getInstance(sourceNode);

        // Stall the recovery and keeps its source slot occupied.
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(PeerRecoveryTargetService.Actions.FILE_CHUNK)) {
                safeAwait(fileChunkLatch);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        internalCluster().startDataOnlyNode();
        assertAcked(
            indicesAdmin().prepareUpdateSettings(indexName).setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        awaitRecoveryCountStats(Map.of(sourceNode, stats -> stats.currentAsSourceQueued() == 1 && stats.currentAsSource() == 1));

        fileChunkLatch.countDown();
        ensureGreen(indexName);
    }

    public void testQueuedRecoveryCancelledWhenTargetNodeLeaves() throws Exception {
        internalCluster().startMasterOnlyNode();
        final var sourceNode = internalCluster().startDataOnlyNode(
            Settings.builder()
                .put(PeerRecoverySourceService.INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 1)
                .build()
        );
        final int numShards = 2;
        final var indexName = randomIndexName();
        createIndex(indexName, indexSettings(numShards, 0).build());

        // Ensure committed segments exist, so FILE_CHUNK actions are issued
        for (int i = 0; i < 50; i++) {
            indexDoc(indexName, Integer.toString(i), "f", randomAlphaOfLength(10));
            refresh(indexName);
        }
        flush(indexName);
        ensureGreen(indexName);

        final var fileChunkLatch = new CountDownLatch(1);
        final var transportService = MockTransportService.getInstance(sourceNode);

        // Stall the recovery and keeps its source slot occupied.
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(PeerRecoveryTargetService.Actions.FILE_CHUNK)) {
                safeAwait(fileChunkLatch);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        // Unthrottle the master + keep the primaries on source node
        var allocationSettingsUpdate = Settings.builder()
            .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 4)
            .put(ShardsLimitAllocationDecider.CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 2);

        assertAcked(
            clusterAdmin().prepareUpdateSettings(TimeValue.timeValueSeconds(10), TimeValue.timeValueSeconds(10))
                .setPersistentSettings(allocationSettingsUpdate)
        );

        final var targetNodes = internalCluster().startDataOnlyNodes(2);
        ensureStableCluster(4);
        assertAcked(
            indicesAdmin().prepareUpdateSettings(indexName)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
                        .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0)
                )
        );
        awaitRecoveryCountStats(Map.of(sourceNode, stats -> stats.currentAsSourceQueued() == 3 && stats.currentAsSource() == 1));

        allocationSettingsUpdate = Settings.builder()
            .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), EnableAllocationDecider.Allocation.NONE);
        assertAcked(
            clusterAdmin().prepareUpdateSettings(TimeValue.timeValueSeconds(10), TimeValue.timeValueSeconds(10))
                .setPersistentSettings(allocationSettingsUpdate)
        );
        internalCluster().stopNode(targetNodes.get(1));
        ensureStableCluster(3);
        final var updatedRecoveryStats = getRecoveryStats(sourceNode);
        assertThat("expected cancelled queued recovery after node left", updatedRecoveryStats.currentAsSourceQueued(), lessThan(3));

        assertAcked(
            clusterAdmin().prepareUpdateSettings(TimeValue.timeValueSeconds(10), TimeValue.timeValueSeconds(10))
                .setPersistentSettings(
                    Settings.builder().putNull(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey())
                )
        );
        fileChunkLatch.countDown();
        internalCluster().startDataOnlyNode();
        ensureGreen(indexName);
    }

    public void testQueuedRecoveryCancelledWhenSourceShardClosed() {
        internalCluster().startMasterOnlyNode();
        updateClusterSettings(
            Settings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
        );
        final var sourceNode = internalCluster().startDataOnlyNode(
            Settings.builder()
                .put(PeerRecoverySourceService.INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 1)
                .build()
        );
        final var index1 = randomIndexName();
        final var index2 = randomIndexName();
        createIndex(index1, indexSettings(1, 0).build());
        createIndex(index2, indexSettings(1, 0).build());

        // Ensure committed segments exist, so FILE_CHUNK actions are issued
        for (int i = 0; i < 50; i++) {
            indexDoc(index1, Integer.toString(i), "f", randomAlphaOfLength(10));
            indexDoc(index2, Integer.toString(i), "f", randomAlphaOfLength(10));
            refresh(index1, index2);
        }
        flush(index1, index2);
        ensureGreen(index1, index2);

        final var fileChunkReceivedLatch = new CountDownLatch(1);
        final var proceedRecoveryLatch = new CountDownLatch(1);
        final Set<ShardId> shardsThatStartedRecovery = ConcurrentHashMap.newKeySet();
        final var transportService = MockTransportService.getInstance(sourceNode);

        // Stall the recovery and keeps its source slot occupied.
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(PeerRecoveryTargetService.Actions.FILE_CHUNK)) {
                if (request instanceof RecoveryFileChunkRequest fileChunkRequest) {
                    shardsThatStartedRecovery.add(fileChunkRequest.shardId());
                    fileChunkReceivedLatch.countDown();
                }
                safeAwait(proceedRecoveryLatch);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        internalCluster().startDataOnlyNodes(1);
        assertAcked(
            indicesAdmin().prepareUpdateSettings(index1).setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );

        safeAwait(fileChunkReceivedLatch);
        awaitRecoveryCountStats(Map.of(sourceNode, stats -> stats.currentAsSource() == 1));
        assertAcked(
            indicesAdmin().prepareUpdateSettings(index2).setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        awaitRecoveryCountStats(Map.of(sourceNode, stats -> stats.currentAsSource() == 1 && stats.currentAsSourceQueued() == 1));

        assertThat(shardsThatStartedRecovery, hasSize(1));
        assertThat(shardsThatStartedRecovery.stream().findFirst().get().getIndex().getName(), equalTo(index1));

        assertAcked(indicesAdmin().prepareDelete(index2));
        final var updatedStats = getRecoveryStats(sourceNode);
        assertThat("expected no more queued recovery request", updatedStats.currentAsSourceQueued(), equalTo(0));

        proceedRecoveryLatch.countDown();
        assertThat(shardsThatStartedRecovery, hasSize(1));
    }

    public void testNextPendingRecoveryDispatchedOnActiveRecoveryCancellation() {
        internalCluster().startMasterOnlyNode();
        final var sourceNode = internalCluster().startDataOnlyNode();
        updateClusterSettings(
            Settings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
        );

        final var indexToDelete = randomIndexName();
        final var indexToRecover = randomIndexName();
        createIndex(indexToDelete, indexSettings(1, 0).build());
        createIndex(indexToRecover, indexSettings(1, 0).build());

        // Ensure committed segments exist, so FILE_CHUNK actions are issued
        for (int i = 0; i < 50; i++) {
            indexDoc(indexToDelete, Integer.toString(i), "f", randomAlphaOfLength(10));
            indexDoc(indexToRecover, Integer.toString(i), "f", randomAlphaOfLength(10));
            refresh(indexToDelete, indexToRecover);
        }
        flush(indexToDelete, indexToRecover);
        ensureGreen(indexToDelete, indexToRecover);

        final var fileChunkRequestReceived = new CountDownLatch(1);
        final var proceedWithRecovery = new CountDownLatch(1);
        final Set<ShardId> shardsThatStartedRecovery = ConcurrentHashMap.newKeySet();
        final var transportService = MockTransportService.getInstance(sourceNode);

        // Stall the recovery and keeps its target recovery slot occupied.
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(PeerRecoveryTargetService.Actions.FILE_CHUNK)) {
                if (request instanceof RecoveryFileChunkRequest fileChunkRequest) {
                    shardsThatStartedRecovery.add(fileChunkRequest.shardId());
                    fileChunkRequestReceived.countDown();
                    safeAwait(proceedWithRecovery);
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        // Target node only has 1 slot for concurrent recovery
        String targetNode = internalCluster().startDataOnlyNode(
            Settings.builder().put(ThrottlingRecoveryService.INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), 1).build()
        );

        assertAcked(
            indicesAdmin().prepareUpdateSettings(indexToDelete)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        safeAwait(fileChunkRequestReceived);
        awaitRecoveryCountStats(Map.of(targetNode, stats -> stats.currentAsTarget() == 1 && stats.currentAsTargetQueued() == 0));

        // We expect the new recovery to be enqueued on target
        assertAcked(
            indicesAdmin().prepareUpdateSettings(indexToRecover)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        awaitRecoveryCountStats(Map.of(targetNode, stats -> stats.currentAsTarget() == 1 && stats.currentAsTargetQueued() == 1));

        // Delete the first recovering index, to trigger a cancellation
        assertAcked(indicesAdmin().prepareDelete(indexToDelete));
        proceedWithRecovery.countDown();

        ensureGreen(indexToRecover);
        assertThat(shardsThatStartedRecovery, hasSize(2));
    }

    public void testNextPendingRecoveryDispatchedOnActivePeerRecoveryCompletion() {
        final var sourceNode = internalCluster().startNode();
        updateClusterSettings(
            Settings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
        );

        final var indexOne = randomIndexName();
        final var indexTwo = randomIndexName();
        createIndex(indexOne, indexSettings(1, 0).build());
        createIndex(indexTwo, indexSettings(1, 0).build());
        ensureGreen(indexOne, indexTwo);

        final var startRecoveryRequestBarrier = new CyclicBarrier(2);
        final var transportService = MockTransportService.getInstance(sourceNode);

        transportService.addRequestHandlingBehavior(PeerRecoverySourceService.Actions.START_RECOVERY, (handler, request, channel, task) -> {
            handler.messageReceived(request, channel, task);
            safeAwait(startRecoveryRequestBarrier);
        });

        final var targetNode = internalCluster().startDataOnlyNode(
            Settings.builder().put(ThrottlingRecoveryService.INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), 1).build()
        );

        // First recovery will occupy the only recovery slot
        assertAcked(
            indicesAdmin().prepareUpdateSettings(indexOne).setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        safeAwait(startRecoveryRequestBarrier);

        // Second recovery will be queued
        assertAcked(
            indicesAdmin().prepareUpdateSettings(indexTwo).setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        awaitRecoveryCountStats(Map.of(targetNode, stats -> stats.currentAsTargetQueued() == 1));

        // Wait for second recovery to start
        safeAwait(startRecoveryRequestBarrier);
        ensureGreen(indexOne, indexTwo);
    }

    public void testNextPendingRecoveryDispatchedOnActiveEmptyStoreRecoveryCompletion() {
        final var node = internalCluster().startNode(
            Settings.builder().put(ThrottlingRecoveryService.INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), 1).build()
        );
        final var indexOne = randomIndexName();
        final var indexTwo = randomIndexName();

        final var firstIndexRecoveryStarted = new CountDownLatch(1);
        final var firstIndexBlock = new CountDownLatch(1);
        final var secondIndexRecoveryStarted = new CountDownLatch(1);

        final IndexEventListener indexEventListener = new IndexEventListener() {
            @Override
            public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings, ActionListener<Void> listener) {
                final var name = indexShard.shardId().getIndexName();
                if (name.equals(indexOne)) {
                    firstIndexRecoveryStarted.countDown();
                    safeAwait(firstIndexBlock);
                } else if (name.equals(indexTwo)) {
                    secondIndexRecoveryStarted.countDown();
                }
                listener.onResponse(null);
            }
        };
        internalCluster().getInstance(MockIndexEventListener.TestEventListener.class, node).setNewDelegate(indexEventListener);

        // Create first index and block it in recovery to occupy the slot
        assertAcked(prepareCreate(indexOne).setSettings(indexSettings(1, 0).build()).setWaitForActiveShards(ActiveShardCount.NONE));
        safeAwait(firstIndexRecoveryStarted);

        // Create second index, recovery will be queued
        assertAcked(prepareCreate(indexTwo).setSettings(indexSettings(1, 0).build()).setWaitForActiveShards(ActiveShardCount.NONE));
        awaitRecoveryCountStats(Map.of(node, stats -> stats.currentFromStoreQueued() == 1));

        // Release first recovery
        firstIndexBlock.countDown();
        safeAwait(secondIndexRecoveryStarted);
        awaitRecoveryCountStats(Map.of(node, stats -> stats.currentFromStoreQueued() == 0));

        ensureGreen(indexOne, indexTwo);
    }

    public void testAllQueuedRecoveriesEventuallyComplete() {
        internalCluster().startNode();
        final int limit = between(1, 6);
        final int totalIndices = limit + 2;
        final var indexNames = IntStream.range(0, totalIndices).mapToObj(i -> randomIndexName()).toList();

        Settings.Builder settings = Settings.builder()
            .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), totalIndices)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), totalIndices);
        updateClusterSettings(settings);

        for (final var name : indexNames) {
            createIndex(name, indexSettings(1, 0).build());
            for (int i = 0; i < 50; i++) {
                indexDoc(name, Integer.toString(i), "f", randomAlphaOfLength(10));
                refresh(name);
            }
            flush(name);
        }
        ensureGreen(indexNames.toArray(String[]::new));

        final var targetNode = internalCluster().startDataOnlyNode(
            Settings.builder().put(ThrottlingRecoveryService.INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), limit).build()
        );

        final var releaseRecoveries = new CountDownLatch(1);
        final IndexEventListener recoveryListener = new IndexEventListener() {
            @Override
            public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings, ActionListener<Void> listener) {
                safeAwait(releaseRecoveries);
                listener.onResponse(null);
            }
        };
        internalCluster().getInstance(MockIndexEventListener.TestEventListener.class, targetNode).setNewDelegate(recoveryListener);

        // Start recoveries
        for (final var name : indexNames) {
            assertAcked(
                indicesAdmin().prepareUpdateSettings(name).setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
            );
        }

        // Wait until exactly [limit] recoveries have started
        awaitRecoveryCountStats(
            Map.of(targetNode, stats -> stats.currentAsTarget() == limit && stats.currentAsTargetQueued() == totalIndices - limit)
        );

        releaseRecoveries.countDown();
        ensureGreen(indexNames.toArray(new String[0]));
    }

    public void testDynamicLimitIncreaseDispatchesPendingRecoveriesUpToLimit() {
        internalCluster().startNode();
        final int firstLimit = between(1, 3);
        final int secondLimit = firstLimit + between(1, 3);
        final int totalIndices = secondLimit + between(1, 2);
        final var indexNames = IntStream.range(0, totalIndices).mapToObj(i -> randomIndexName()).toList();

        Settings.Builder settings = Settings.builder()
            .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), totalIndices)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), totalIndices);
        updateClusterSettings(settings);

        for (String indexName : indexNames) {
            createIndex(indexName, indexSettings(1, 0).build());
            for (int i = 0; i < 50; i++) {
                indexDoc(indexName, Integer.toString(i), "f", randomAlphaOfLength(10));
                refresh(indexName);
            }
            flush(indexName);
        }
        ensureGreen(indexNames.toArray(String[]::new));

        final var targetNode = internalCluster().startDataOnlyNode(
            Settings.builder()
                .put(ThrottlingRecoveryService.INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), firstLimit)
                .build()
        );
        final var releaseRecoveries = new CountDownLatch(1);
        final IndexEventListener recoveryListener = new IndexEventListener() {
            @Override
            public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings, ActionListener<Void> listener) {
                safeAwait(releaseRecoveries);
                listener.onResponse(null);
            }
        };
        internalCluster().getInstance(MockIndexEventListener.TestEventListener.class, targetNode).setNewDelegate(recoveryListener);

        // Start recoveries
        for (String indexName : indexNames) {
            assertAcked(
                indicesAdmin().prepareUpdateSettings(indexName)
                    .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
            );
        }

        // Recoveries above the [firstLimit] should be throttled
        awaitRecoveryCountStats(
            Map.of(targetNode, stats -> stats.currentAsTarget() == firstLimit && stats.currentAsTargetQueued() == totalIndices - firstLimit)
        );

        // Pending recoveries should be dispatched when increasing limit
        assertAcked(
            clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .setPersistentSettings(
                    Settings.builder()
                        .put(ThrottlingRecoveryService.INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), secondLimit)
                        .build()
                )
        );
        awaitRecoveryCountStats(
            Map.of(
                targetNode,
                stats -> stats.currentAsTarget() == secondLimit && stats.currentAsTargetQueued() == totalIndices - secondLimit
            )
        );

        releaseRecoveries.countDown();
        ensureGreen(indexNames.toArray(String[]::new));
    }

    private static RecoveryStats getRecoveryStats(String node) {
        return clusterAdmin().prepareNodesStats(node)
            .clear()
            .setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Recovery))
            .get()
            .getNodes()
            .getFirst()
            .getIndices()
            .getRecoveryStats();
    }
}
