/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.admin.indices.ResizeIndexTestUtils;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.SnapshotMetrics;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DirectRecoveryCancellationIT extends AbstractIndexRecoveryIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.addAll(List.of(TestTelemetryPlugin.class, TestRecoveryBlockerPlugin.class, BlockingFsRepositoryPlugin.class));
        return plugins;
    }

    @Before
    public void resetPluginGates() {
        // So that a failed test cannot corrupt subsequent ones.
        TestRecoveryBlockerPlugin.reset();
        BlockingFsRepositoryPlugin.reset();
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
            .build();
    }

    public void testDirectCancellationOfEmptyStoreRecovery() throws Exception {
        final var node = internalCluster().startNode();
        final var indexName = randomIndexName();

        // Block the EMPTY_STORE recovery
        safeAcquire(TestRecoveryBlockerPlugin.beforeRecoveryGate);

        prepareCreate(indexName).setSettings(indexSettings(1, 0).build()).execute();

        // Wait for recovery to be blocked
        safeAcquire(TestRecoveryBlockerPlugin.beforeRecoveryEntered);
        TestRecoveryBlockerPlugin.beforeRecoveryEntered.release();

        disableAllocation();
        waitNoPendingTasksOnAll();

        final var index = resolveIndex(indexName);
        final var shardId = new ShardId(index, 0);
        final var indicesService = internalCluster().getInstance(IndicesService.class, node);
        final var shard = indicesService.indexServiceSafe(index).getShard(0);
        final var allocationId = shard.routingEntry().allocationId().getId();

        final var clusterService = internalCluster().getInstance(ClusterService.class, node);
        final var shardFailureReceived = shardCancelledFailureReceivedLatch(node, shardId);
        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().version(),
            List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(shardId, allocationId, true))
        );

        client(node).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();
        TestRecoveryBlockerPlugin.beforeRecoveryGate.release();

        safeAwait(shardFailureReceived);
        awaitDirectCancellationMetric(node, 1L);
    }

    public void testDirectCancellationOfExistingStoreRecovery() throws Exception {
        final var node = internalCluster().startNode();
        final var indexName = randomIndexName();

        createIndex(indexName, indexSettings(1, 0).build());
        indexDoc(indexName, "1", "f", randomAlphaOfLength(10));
        flush(indexName);
        ensureGreen(indexName);

        assertAcked(indicesAdmin().prepareClose(indexName));

        // Block the EXISTING_STORE recovery
        safeAcquire(TestRecoveryBlockerPlugin.beforeRecoveryGate);

        indicesAdmin().prepareOpen(indexName).execute();

        safeAcquire(TestRecoveryBlockerPlugin.beforeRecoveryEntered);
        TestRecoveryBlockerPlugin.beforeRecoveryEntered.release();

        disableAllocation();
        waitNoPendingTasksOnAll();

        final var index = resolveIndex(indexName);
        final var shardId = new ShardId(index, 0);
        final var indicesService = internalCluster().getInstance(IndicesService.class, node);
        final var shard = indicesService.indexServiceSafe(index).getShard(0);
        final var allocationId = shard.routingEntry().allocationId().getId();

        final var clusterService = internalCluster().getInstance(ClusterService.class, node);
        final var shardFailureReceived = shardCancelledFailureReceivedLatch(node, shardId);
        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().version(),
            List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(shardId, allocationId, true))
        );
        client(node).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();
        TestRecoveryBlockerPlugin.beforeRecoveryGate.release();

        safeAwait(shardFailureReceived);
        awaitDirectCancellationMetric(node, 1L);
    }

    public void testDirectCancellationOfLocalShardsRecovery() throws Exception {
        final var node = internalCluster().startNode();
        final var sourceIndexName = randomIndexName();
        final var targetIndexName = randomIndexName();

        createIndex(sourceIndexName, indexSettings(1, 0).build());
        indexDoc(sourceIndexName, "1", "f", randomAlphaOfLength(10));
        flush(sourceIndexName);
        ensureGreen(sourceIndexName);

        // Required for clone, make the source index read-only
        updateIndexSettings(Settings.builder().put("index.blocks.write", true), sourceIndexName);

        // Block the LOCAL_SHARDS recovery
        safeAcquire(TestRecoveryBlockerPlugin.beforeRecoveryGate);

        ResizeIndexTestUtils.executeResize(ResizeType.CLONE, sourceIndexName, targetIndexName, indexSettings(1, 0));

        safeAcquire(TestRecoveryBlockerPlugin.beforeRecoveryEntered);
        TestRecoveryBlockerPlugin.beforeRecoveryEntered.release();

        disableAllocation();
        waitNoPendingTasksOnAll();

        final var targetIndex = resolveIndex(targetIndexName);
        final var shardId = new ShardId(targetIndex, 0);
        final var indicesService = internalCluster().getInstance(IndicesService.class, node);
        final var shard = indicesService.indexServiceSafe(targetIndex).getShard(0);
        final var allocationId = shard.routingEntry().allocationId().getId();

        final var clusterService = internalCluster().getInstance(ClusterService.class, node);
        final var shardFailureReceived = shardCancelledFailureReceivedLatch(node, shardId);
        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().version(),
            List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(shardId, allocationId, true))
        );
        client(node).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();
        TestRecoveryBlockerPlugin.beforeRecoveryGate.release();

        safeAwait(shardFailureReceived);
        awaitDirectCancellationMetric(node, 1L);
    }

    public void testDirectCancellationOfSnapshotRecoveryBeforeRestore() throws Exception {
        final var node = internalCluster().startNode();
        final var indexName = randomIndexName();
        final var repoName = "test-repo";

        createIndex(indexName, indexSettings(1, 0).build());
        indexDoc(indexName, "1", "f", randomAlphaOfLength(10));
        flush(indexName);
        ensureGreen(indexName);

        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName)
                .setType("fs")
                .setSettings(Settings.builder().put("location", randomRepoPath()))
        );
        clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap").setWaitForCompletion(true).get();
        assertAcked(indicesAdmin().prepareDelete(indexName));

        // Block the SNAPSHOT recovery
        safeAcquire(TestRecoveryBlockerPlugin.beforeRecoveryGate);

        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap").setWaitForCompletion(false).execute();

        safeAcquire(TestRecoveryBlockerPlugin.beforeRecoveryEntered);
        TestRecoveryBlockerPlugin.beforeRecoveryEntered.release();

        disableAllocation();
        waitNoPendingTasksOnAll();

        final var index = resolveIndex(indexName);
        final var shardId = new ShardId(index, 0);
        final var indicesService = internalCluster().getInstance(IndicesService.class, node);
        final var shard = indicesService.indexServiceSafe(index).getShard(0);
        final var allocationId = shard.routingEntry().allocationId().getId();

        final var clusterService = internalCluster().getInstance(ClusterService.class, node);
        final var shardFailureReceived = shardCancelledFailureReceivedLatch(node, shardId);
        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().version(),
            List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(shardId, allocationId, true))
        );
        client(node).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();
        TestRecoveryBlockerPlugin.beforeRecoveryGate.release();

        safeAwait(shardFailureReceived);
        awaitDirectCancellationMetric(node, 1L);
    }

    public void testDirectCancellationOfSnapshotRecoveryDuringRestore() throws Exception {
        final var node = internalCluster().startNode();
        final var indexName = randomIndexName();
        final var repoName = "test-repo";

        createIndex(indexName, indexSettings(1, 0).build());
        indexDoc(indexName, "1", "f", randomAlphaOfLength(10));
        flush(indexName);
        ensureGreen(indexName);

        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName)
                .setType(BlockingFsRepositoryPlugin.REPO_TYPE)
                .setSettings(Settings.builder().put("location", randomRepoPath()))
        );
        clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap").setWaitForCompletion(true).get();
        assertAcked(indicesAdmin().prepareDelete(indexName));

        // Pause restore inside restoreShard
        safeAcquire(BlockingFsRepositoryPlugin.proceedWithRestore);
        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap").setWaitForCompletion(false).execute();

        safeAcquire(BlockingFsRepositoryPlugin.restoreHasStarted);
        BlockingFsRepositoryPlugin.restoreHasStarted.release();

        disableAllocation();
        waitNoPendingTasksOnAll();

        final var index = resolveIndex(indexName);
        final var shardId = new ShardId(index, 0);
        final var indicesService = internalCluster().getInstance(IndicesService.class, node);
        final var shard = indicesService.indexServiceSafe(index).getShard(0);
        final var allocationId = shard.routingEntry().allocationId().getId();

        final var clusterService = internalCluster().getInstance(ClusterService.class, node);
        final var shardFailureReceived = shardCancelledFailureReceivedLatch(node, shardId);
        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().version(),
            List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(shardId, allocationId, true))
        );
        // Set the cancellation flag, then release restoreShard so checkpoint fires after it completes
        client(node).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();
        BlockingFsRepositoryPlugin.proceedWithRestore.release();

        safeAwait(shardFailureReceived);
        awaitDirectCancellationMetric(node, 1L);
    }

    public void testDirectCancellationOfStartedReplicaPeerRecovery() throws Exception {
        final var primaryNode = internalCluster().startNode();
        final var indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 0).build());

        // Ensure committed segments exist, so FILE_CHUNK actions are issued
        for (int i = 0; i < 50; i++) {
            indexDoc(indexName, Integer.toString(i), "f", randomAlphaOfLength(10));
            refresh(indexName);
        }
        flush(indexName);
        ensureGreen(indexName);

        final var blockedRecovery = new CountDownLatch(1);
        final var proceedWithRecovery = new CountDownLatch(1);

        // Stall the recovery
        final var transportService = MockTransportService.getInstance(primaryNode);
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(PeerRecoveryTargetService.Actions.FILE_CHUNK)) {
                blockedRecovery.countDown();
                safeAwait(proceedWithRecovery);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        final var replicaNode = internalCluster().startDataOnlyNode();
        assertAcked(
            indicesAdmin().prepareUpdateSettings(indexName).setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        safeAwait(blockedRecovery);

        disableAllocation();
        waitNoPendingTasksOnAll();

        final var index = resolveIndex(indexName);
        final var shardId = new ShardId(index, 0);
        final var indicesService = internalCluster().getInstance(IndicesService.class, replicaNode);
        final var shard = indicesService.indexServiceSafe(index).getShard(0);
        final var allocationId = shard.routingEntry().allocationId().getId();

        final var shardFailureReceived = shardCancelledFailureReceivedLatch(transportService, shardId);
        final var clusterService = internalCluster().getInstance(ClusterService.class, replicaNode);
        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().version(),
            List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(shardId, allocationId, true))
        );

        client(replicaNode).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();
        proceedWithRecovery.countDown();

        safeAwait(shardFailureReceived);
        waitNoPendingTasksOnAll();
        ensureYellow(indexName);

        awaitReplicaUnassignedDueToCancellation(shardId);
        awaitDirectCancellationMetric(replicaNode, 1L);
    }

    public void testDirectCancellationOfStartedReplicaPeerRecoveryDuringTranslogPrep() throws Exception {
        final var primaryNode = internalCluster().startNode();
        final var indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 0).build());

        for (int i = 0; i < 50; i++) {
            indexDoc(indexName, Integer.toString(i), "f", randomAlphaOfLength(10));
            refresh(indexName);
        }
        flush(indexName);
        ensureGreen(indexName);

        final var blockedRecovery = new CountDownLatch(1);
        final var proceedWithRecovery = new CountDownLatch(1);

        // Stall the recovery right as it prepares for translog operations
        final var transportService = MockTransportService.getInstance(primaryNode);
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(PeerRecoveryTargetService.Actions.PREPARE_TRANSLOG)) {
                blockedRecovery.countDown();
                safeAwait(proceedWithRecovery);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        final var replicaNode = internalCluster().startDataOnlyNode();
        assertAcked(
            indicesAdmin().prepareUpdateSettings(indexName).setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        safeAwait(blockedRecovery);

        disableAllocation();
        waitNoPendingTasksOnAll();

        final var index = resolveIndex(indexName);
        final var shardId = new ShardId(index, 0);
        final var indicesService = internalCluster().getInstance(IndicesService.class, replicaNode);
        final var shard = indicesService.indexServiceSafe(index).getShard(0);
        final var allocationId = shard.routingEntry().allocationId().getId();

        final var shardFailureReceived = shardCancelledFailureReceivedLatch(transportService, shardId);
        final var clusterService = internalCluster().getInstance(ClusterService.class, replicaNode);
        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().version(),
            List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(shardId, allocationId, true))
        );

        client(replicaNode).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();
        proceedWithRecovery.countDown();

        safeAwait(shardFailureReceived);
        waitNoPendingTasksOnAll();
        ensureYellow(indexName);

        awaitReplicaUnassignedDueToCancellation(shardId);
        awaitDirectCancellationMetric(replicaNode, 1L);
    }

    public void testDirectCancellationOfPrimaryRelocationDuringFileTransfer() throws Exception {
        final var sourceNode = internalCluster().startNode();
        final var indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 0).build());

        // Ensure committed segments exist, so FILE_CHUNK actions are issued
        for (int i = 0; i < 50; i++) {
            indexDoc(indexName, Integer.toString(i), "f", randomAlphaOfLength(10));
            refresh(indexName);
        }
        flush(indexName);
        ensureGreen(indexName);

        final var blockedRelocation = new CountDownLatch(1);
        final var proceedWithRelocation = new CountDownLatch(1);

        // Stall the relocation at the file-transfer step on the source
        final var sourceTransportService = MockTransportService.getInstance(sourceNode);
        sourceTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(PeerRecoveryTargetService.Actions.FILE_CHUNK)) {
                blockedRelocation.countDown();
                safeAwait(proceedWithRelocation);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        final var targetNode = internalCluster().startDataOnlyNode();
        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, 0, sourceNode, targetNode));
        safeAwait(blockedRelocation);

        disableAllocation();
        waitNoPendingTasksOnAll();

        final var index = resolveIndex(indexName);
        final var shardId = new ShardId(index, 0);
        final var indicesService = internalCluster().getInstance(IndicesService.class, targetNode);
        final var shard = indicesService.indexServiceSafe(index).getShard(0);
        final var allocationId = shard.routingEntry().allocationId().getId();

        final var shardFailureReceived = shardCancelledFailureReceivedLatch(sourceTransportService, shardId);
        final var clusterService = internalCluster().getInstance(ClusterService.class, targetNode);
        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().version(),
            List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(shardId, allocationId, true))
        );

        client(targetNode).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();
        proceedWithRelocation.countDown();

        safeAwait(shardFailureReceived);

        // The failed relocation leaves the primary on sourceNode
        waitNoPendingTasksOnAll();
        ensureGreen(indexName);
        final var finalState = internalCluster().getInstance(ClusterService.class, sourceNode).state();
        final var primaryShard = finalState.routingTable().shardRoutingTable(shardId).primaryShard();
        assertTrue("primary shard is still started", primaryShard.started());
        assertThat(
            "primary shard is still located on source node",
            primaryShard.currentNodeId(),
            equalTo(finalState.nodes().resolveNode(sourceNode).getId())
        );
        assertThat(directCancellationMetric(targetNode), equalTo(1L));
    }

    /// A cancellation requested once the primary handoff has started should be silently ignored.
    /// The handoff and the relocation complete normally, and the flag is eventually wiped by `postRecovery()`.
    public void testDirectCancellationAtPrimaryHandoffGetsIgnored() throws Exception {
        final var sourceNode = internalCluster().startNode();
        final var indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 0).build());

        for (int i = 0; i < 50; i++) {
            indexDoc(indexName, Integer.toString(i), "f", randomAlphaOfLength(10));
            refresh(indexName);
        }
        flush(indexName);
        ensureGreen(indexName);

        final var targetNode = internalCluster().startDataOnlyNode();

        // Stall the primary context handoff on the target
        final var blockedHandoff = new CountDownLatch(1);
        final var proceedWithHandoff = new CountDownLatch(1);
        MockTransportService.getInstance(targetNode)
            .addRequestHandlingBehavior(PeerRecoveryTargetService.Actions.HANDOFF_PRIMARY_CONTEXT, (handler, request, channel, task) -> {
                blockedHandoff.countDown();
                safeAwait(proceedWithHandoff);
                handler.messageReceived(request, channel, task);
            });

        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, 0, sourceNode, targetNode));
        safeAwait(blockedHandoff);

        disableAllocation();
        waitNoPendingTasksOnAll();

        final var index = resolveIndex(indexName);
        final var shardId = new ShardId(index, 0);
        final var indicesService = internalCluster().getInstance(IndicesService.class, targetNode);
        final var shard = indicesService.indexServiceSafe(index).getShard(0);
        final var allocationId = shard.routingEntry().allocationId().getId();

        final var clusterService = internalCluster().getInstance(ClusterService.class, targetNode);
        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().version(),
            List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(shardId, allocationId, true))
        );
        client(targetNode).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();
        proceedWithHandoff.countDown();

        waitNoPendingTasksOnAll();
        ensureGreen(indexName);
        awaitClusterState(state -> {
            final var primaryShard = state.routingTable().shardRoutingTable(shardId).primaryShard();
            return primaryShard.started() && primaryShard.currentNodeId().equals(state.nodes().resolveNode(targetNode).getId());
        });

        assertThat(shard.state(), equalTo(IndexShardState.STARTED));
        shard.ensureRecoveryNotCancelled();
        assertThat(directCancellationMetric(targetNode), equalTo(0L));
    }

    public void testDirectCancellationIgnoredAfterRecoveryFinalize() throws Exception {
        final var node = internalCluster().startNode();
        final var indexName = randomIndexName();

        safeAcquire(TestRecoveryBlockerPlugin.afterRecoveryGate);
        prepareCreate(indexName).setSettings(indexSettings(1, 0).build()).execute();

        safeAcquire(TestRecoveryBlockerPlugin.afterRecoveryEntered);
        TestRecoveryBlockerPlugin.afterRecoveryEntered.release();

        disableAllocation();
        waitNoPendingTasksOnAll();

        final var index = resolveIndex(indexName);
        final var shardId = new ShardId(index, 0);
        final var indicesService = internalCluster().getInstance(IndicesService.class, node);
        final var shard = indicesService.indexServiceSafe(index).getShard(0);
        final var allocationId = shard.routingEntry().allocationId().getId();

        final var clusterService = internalCluster().getInstance(ClusterService.class, node);
        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().version(),
            List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(shardId, allocationId, true))
        );

        // All checkpoints are already past, so the flag is never read.
        client(node).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();

        // Release the gate. postRecovery() will clear the cancellation flag and the shard transitions to STARTED.
        TestRecoveryBlockerPlugin.afterRecoveryGate.release();

        ensureGreen(indexName);

        // Confirm the flag was cleared by postRecovery() and the shard is now in STARTED state.
        final var indexShard = indicesService.indexServiceSafe(index).getShard(0);
        assertThat(indexShard.state(), equalTo(IndexShardState.STARTED));
        indexShard.ensureRecoveryNotCancelled();
        assertThat(directCancellationMetric(node), equalTo(0L));
    }

    public void testCancellationArrivesBeforeShardLockIsAcquired() throws Exception {
        final var node = internalCluster().startNode();
        final var indexName = randomIndexName();

        // Block before the primary shard is created
        safeAcquire(TestRecoveryBlockerPlugin.beforeShardCreatedGate);

        prepareCreate(indexName).setSettings(indexSettings(1, 0).build()).execute();

        safeAcquire(TestRecoveryBlockerPlugin.beforeShardCreatedEntered);
        TestRecoveryBlockerPlugin.beforeShardCreatedEntered.release();

        final var latestShard = TestRecoveryBlockerPlugin.latestCreatedShard.get();
        final var index = latestShard.shardId().getIndex();
        final var indicesService = internalCluster().getInstance(IndicesService.class, node);
        final var indexService = indicesService.indexServiceSafe(index);
        assertThat(indexService.numberOfShards(), equalTo(0));

        final var shardId = latestShard.shardId();
        final var clusterService = internalCluster().getInstance(ClusterService.class, node);
        final var allocationId = latestShard.allocationId();

        final var shardFailureReceived = shardCancelledFailureReceivedLatch(node, shardId);
        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().version(),
            List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(shardId, allocationId.getId(), true))
        );
        client(node).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();

        TestRecoveryBlockerPlugin.beforeShardCreatedGate.release();
        safeAwait(shardFailureReceived);

        awaitClusterState(state -> {
            final var primaryShard = state.routingTable().shardRoutingTable(shardId).primaryShard();
            return (primaryShard.unassigned() && primaryShard.unassignedInfo().failedAllocations() > 0)
                || Objects.equals(primaryShard.allocationId(), allocationId) == false;
        });
        awaitDirectCancellationMetric(node, 1L);
    }

    private void disableAllocation() {
        assertAcked(
            clusterAdmin().prepareUpdateSettings(TimeValue.timeValueSeconds(10), TimeValue.timeValueSeconds(10))
                .setPersistentSettings(
                    Settings.builder()
                        .put(
                            EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(),
                            EnableAllocationDecider.Allocation.NONE
                        )
                )
        );
    }

    private void awaitDirectCancellationMetric(String node, long value) {
        awaitRecoveryCountMetrics(
            node,
            internalCluster().getInstance(PluginsService.class, node).filterPlugins(TestTelemetryPlugin.class).findFirst().orElseThrow(),
            Map.of(RecoveryMetricsCollector.RECOVERY_DIRECT_CANCELLATIONS_METRIC, value)
        );
    }

    private long directCancellationMetric(String node) {
        final TestTelemetryPlugin plugin = internalCluster().getInstance(PluginsService.class, node)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        return plugin.getLongCounterMeasurement(RecoveryMetricsCollector.RECOVERY_DIRECT_CANCELLATIONS_METRIC)
            .stream()
            .mapToLong(Measurement::getLong)
            .sum();
    }

    private void awaitReplicaUnassignedDueToCancellation(ShardId shardId) {
        awaitClusterState(state -> {
            final var indexShardRoutingTable = state.routingTable().shardRoutingTable(shardId);
            assertTrue("Primary shard should be active", indexShardRoutingTable.primaryShard().active());

            final var unassignedShards = indexShardRoutingTable.shardsWithState(ShardRoutingState.UNASSIGNED);
            if (unassignedShards.isEmpty()) {
                return false;
            }
            assertThat(unassignedShards, hasSize(1));
            final var unassignedInfo = unassignedShards.getFirst().unassignedInfo();
            assertNotNull("Replica should have non-null unassigned info", unassignedInfo);
            assertThat(
                "Expected unassignment reason to be RECOVERY_CANCELLED",
                unassignedInfo.reason(),
                equalTo(UnassignedInfo.Reason.RECOVERY_CANCELLED)
            );
            assertNull("Unassigned info failure should be null", unassignedInfo.failure());
            return true;
        });
    }

    private static CountDownLatch shardCancelledFailureReceivedLatch(String node, ShardId shardId) {
        return shardCancelledFailureReceivedLatch(MockTransportService.getInstance(node), shardId);
    }

    private static CountDownLatch shardCancelledFailureReceivedLatch(MockTransportService transportService, ShardId shardId) {
        final var shardFailureReceivedLatch = new CountDownLatch(1);
        transportService.addRequestHandlingBehavior(ShardStateAction.SHARD_FAILED_ACTION_NAME, (handler, request, channel, task) -> {
            if (request instanceof ShardStateAction.FailedShardEntry failedShard) {
                if (failedShard.getShardId().equals(shardId)
                    && ExceptionsHelper.unwrap(failedShard.getFailure(), RecoveryCancelledException.class) != null) {
                    shardFailureReceivedLatch.countDown();
                }
            }
            handler.messageReceived(request, channel, task);
        });
        return shardFailureReceivedLatch;
    }

    public static class BlockingFsRepositoryPlugin extends Plugin implements RepositoryPlugin {
        static final String REPO_TYPE = "blocking_fs";
        static final Semaphore restoreHasStarted = new Semaphore(0);
        static final Semaphore proceedWithRestore = new Semaphore(1);

        static void reset() {
            restoreHasStarted.drainPermits();
            proceedWithRestore.drainPermits();
            proceedWithRestore.release();
        }

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings,
            RepositoriesMetrics repositoriesMetrics,
            SnapshotMetrics snapshotMetrics
        ) {
            return Map.of(
                REPO_TYPE,
                (projectId, metadata) -> new FsRepository(
                    projectId,
                    metadata,
                    env,
                    namedXContentRegistry,
                    clusterService,
                    bigArrays,
                    recoverySettings
                ) {
                    @Override
                    public void restoreShard(
                        Store store,
                        SnapshotId snapshotId,
                        IndexId indexId,
                        ShardId snapshotShardId,
                        RecoveryState recoveryState,
                        ActionListener<Void> listener
                    ) {
                        restoreHasStarted.release();
                        safeAcquire(proceedWithRestore);
                        proceedWithRestore.release();
                        safeAcquire(restoreHasStarted);
                        super.restoreShard(store, snapshotId, indexId, snapshotShardId, recoveryState, listener);
                    }
                }
            );
        }
    }

    public static class TestRecoveryBlockerPlugin extends Plugin {
        static final Semaphore beforeRecoveryGate = new Semaphore(1);
        static final Semaphore beforeRecoveryEntered = new Semaphore(0);

        static final Semaphore afterRecoveryGate = new Semaphore(1);
        static final Semaphore afterRecoveryEntered = new Semaphore(0);

        static final Semaphore beforeShardCreatedGate = new Semaphore(1);
        static final Semaphore beforeShardCreatedEntered = new Semaphore(0);
        static AtomicReference<ShardRouting> latestCreatedShard = new AtomicReference<>();

        static void reset() {
            beforeRecoveryGate.drainPermits();
            beforeRecoveryGate.release();
            beforeRecoveryEntered.drainPermits();
            afterRecoveryGate.drainPermits();
            afterRecoveryGate.release();
            afterRecoveryEntered.drainPermits();
            beforeShardCreatedGate.drainPermits();
            beforeShardCreatedGate.release();
            beforeShardCreatedEntered.drainPermits();
            latestCreatedShard.set(null);
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addIndexEventListener(new IndexEventListener() {
                @Override
                public void beforeIndexShardCreated(ShardRouting routing, Settings indexSettings) {
                    latestCreatedShard.set(routing);
                    beforeShardCreatedEntered.release();
                    safeAcquire(beforeShardCreatedGate);
                    beforeShardCreatedGate.release();
                    safeAcquire(beforeShardCreatedEntered);

                }

                @Override
                public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings, ActionListener<Void> listener) {
                    if (indexShard.recoveryState() == null
                        || indexShard.recoveryState().getRecoverySource().getType() == RecoverySource.Type.PEER) {
                        listener.onResponse(null);
                        return;
                    }
                    beforeRecoveryEntered.release();
                    safeAcquire(beforeRecoveryGate);
                    beforeRecoveryGate.release();
                    safeAcquire(beforeRecoveryEntered);
                    listener.onResponse(null);
                }

                @Override
                public void afterIndexShardRecovery(IndexShard indexShard, ActionListener<Void> listener) {
                    if (indexShard.routingEntry().primary() == false) {
                        listener.onResponse(null);
                        return;
                    }
                    afterRecoveryEntered.release();
                    safeAcquire(afterRecoveryGate);
                    afterRecoveryGate.release();
                    safeAcquire(afterRecoveryEntered);
                    listener.onResponse(null);
                }
            });
        }
    }
}
