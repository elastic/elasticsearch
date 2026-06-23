/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.Level;
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
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

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
        TestRecoveryBlockerPlugin.beforeRecoveryGate.drainPermits();
        TestRecoveryBlockerPlugin.beforeRecoveryGate.release();
        TestRecoveryBlockerPlugin.beforeRecoveryEntered.drainPermits();
        TestRecoveryBlockerPlugin.afterRecoveryGate.drainPermits();
        TestRecoveryBlockerPlugin.afterRecoveryGate.release();
        TestRecoveryBlockerPlugin.afterRecoveryEntered.drainPermits();
        TestRecoveryBlockerPlugin.beforeShardCreatedGate.drainPermits();
        TestRecoveryBlockerPlugin.beforeShardCreatedGate.release();
        TestRecoveryBlockerPlugin.beforeShardCreatedEntered.drainPermits();
        TestRecoveryBlockerPlugin.latestCreatedShard.set(null);
        BlockingFsRepositoryPlugin.restoreHasStarted.drainPermits();
        BlockingFsRepositoryPlugin.proceedWithRestore.drainPermits();
        BlockingFsRepositoryPlugin.proceedWithRestore.release();
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
        assertTrue(TestRecoveryBlockerPlugin.beforeRecoveryGate.tryAcquire(10, TimeUnit.SECONDS));

        prepareCreate(indexName).setSettings(indexSettings(1, 0).build()).execute();

        // Wait for recovery to be blocked
        assertTrue(TestRecoveryBlockerPlugin.beforeRecoveryEntered.tryAcquire(10, TimeUnit.SECONDS));
        TestRecoveryBlockerPlugin.beforeRecoveryEntered.release();

        disableAllocation();

        final var index = resolveIndex(indexName);
        final var shardId = new ShardId(index, 0);
        final var indicesService = internalCluster().getInstance(IndicesService.class, node);
        final var shard = indicesService.indexServiceSafe(index).getShard(0);
        final var allocationId = shard.routingEntry().allocationId().getId();
        final var clusterService = internalCluster().getInstance(ClusterService.class, node);

        final var shardFailureReceived = new CountDownLatch(1);
        final var transportService = MockTransportService.getInstance(node);
        transportService.addRequestHandlingBehavior("internal:cluster/shard/failure", (handler, request, channel, task) -> {
            if (request instanceof ShardStateAction.FailedShardEntry failedShard) {
                if (failedShard.getShardId().equals(shardId)
                    && ExceptionsHelper.unwrap(failedShard.getFailure(), RecoveryCancelledException.class) != null) {
                    shardFailureReceived.countDown();
                }
            }
            handler.messageReceived(request, channel, task);
        });

        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().version(),
            List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(shardId, allocationId, true))
        );

        client(node).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();
        TestRecoveryBlockerPlugin.beforeRecoveryGate.release();

        safeAwait(shardFailureReceived);
        assertThat(directCancellationMetric(node), equalTo(1L));
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
        assertTrue(TestRecoveryBlockerPlugin.beforeRecoveryGate.tryAcquire(10, TimeUnit.SECONDS));

        indicesAdmin().prepareOpen(indexName).execute();

        assertTrue(TestRecoveryBlockerPlugin.beforeRecoveryEntered.tryAcquire(10, TimeUnit.SECONDS));
        TestRecoveryBlockerPlugin.beforeRecoveryEntered.release();
        disableAllocation();

        final var index = resolveIndex(indexName);
        final var shardId = new ShardId(index, 0);
        final var indicesService = internalCluster().getInstance(IndicesService.class, node);
        final var shard = indicesService.indexServiceSafe(index).getShard(0);
        final var allocationId = shard.routingEntry().allocationId().getId();
        final var clusterService = internalCluster().getInstance(ClusterService.class, node);

        final var shardFailureReceived = new CountDownLatch(1);
        final var transportService = MockTransportService.getInstance(node);
        transportService.addRequestHandlingBehavior("internal:cluster/shard/failure", (handler, request, channel, task) -> {
            if (request instanceof ShardStateAction.FailedShardEntry failedShard) {
                if (failedShard.getShardId().equals(shardId)
                    && ExceptionsHelper.unwrap(failedShard.getFailure(), RecoveryCancelledException.class) != null) {
                    shardFailureReceived.countDown();
                }
            }
            handler.messageReceived(request, channel, task);
        });

        waitNoPendingTasksOnAll();
        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().version(),
            List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(shardId, allocationId, true))
        );
        client(node).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();
        TestRecoveryBlockerPlugin.beforeRecoveryGate.release();

        safeAwait(shardFailureReceived);
        assertThat(directCancellationMetric(node), equalTo(1L));
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
        assertTrue(TestRecoveryBlockerPlugin.beforeRecoveryGate.tryAcquire(10, TimeUnit.SECONDS));

        ResizeIndexTestUtils.executeResize(ResizeType.CLONE, sourceIndexName, targetIndexName, indexSettings(1, 0));

        assertTrue(TestRecoveryBlockerPlugin.beforeRecoveryEntered.tryAcquire(10, TimeUnit.SECONDS));
        TestRecoveryBlockerPlugin.beforeRecoveryEntered.release();
        disableAllocation();

        final var targetIndex = resolveIndex(targetIndexName);
        final var shardId = new ShardId(targetIndex, 0);
        final var indicesService = internalCluster().getInstance(IndicesService.class, node);
        final var shard = indicesService.indexServiceSafe(targetIndex).getShard(0);
        final var allocationId = shard.routingEntry().allocationId().getId();
        final var clusterService = internalCluster().getInstance(ClusterService.class, node);

        final var shardFailureReceived = new CountDownLatch(1);
        final var transportService = MockTransportService.getInstance(node);
        transportService.addRequestHandlingBehavior("internal:cluster/shard/failure", (handler, request, channel, task) -> {
            if (request instanceof ShardStateAction.FailedShardEntry failedShard) {
                if (failedShard.getShardId().equals(shardId)
                    && ExceptionsHelper.unwrap(failedShard.getFailure(), RecoveryCancelledException.class) != null) {
                    shardFailureReceived.countDown();
                }
            }
            handler.messageReceived(request, channel, task);
        });

        waitNoPendingTasksOnAll();
        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().version(),
            List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(shardId, allocationId, true))
        );
        client(node).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();
        TestRecoveryBlockerPlugin.beforeRecoveryGate.release();

        safeAwait(shardFailureReceived);
        assertThat(directCancellationMetric(node), equalTo(1L));
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
        assertTrue(TestRecoveryBlockerPlugin.beforeRecoveryGate.tryAcquire(10, TimeUnit.SECONDS));

        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap").setWaitForCompletion(false).execute();

        assertTrue(TestRecoveryBlockerPlugin.beforeRecoveryEntered.tryAcquire(30, TimeUnit.SECONDS));
        TestRecoveryBlockerPlugin.beforeRecoveryEntered.release();
        disableAllocation();

        final var index = resolveIndex(indexName);
        final var shardId = new ShardId(index, 0);
        final var indicesService = internalCluster().getInstance(IndicesService.class, node);
        final var shard = indicesService.indexServiceSafe(index).getShard(0);
        final var allocationId = shard.routingEntry().allocationId().getId();
        final var clusterService = internalCluster().getInstance(ClusterService.class, node);

        final var shardFailureReceived = new CountDownLatch(1);
        final var transportService = MockTransportService.getInstance(node);
        transportService.addRequestHandlingBehavior("internal:cluster/shard/failure", (handler, request, channel, task) -> {
            if (request instanceof ShardStateAction.FailedShardEntry failedShard) {
                if (failedShard.getShardId().equals(shardId)
                    && ExceptionsHelper.unwrap(failedShard.getFailure(), RecoveryCancelledException.class) != null) {
                    shardFailureReceived.countDown();
                }
            }
            handler.messageReceived(request, channel, task);
        });

        waitNoPendingTasksOnAll();
        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().version(),
            List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(shardId, allocationId, true))
        );
        client(node).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();
        TestRecoveryBlockerPlugin.beforeRecoveryGate.release();

        safeAwait(shardFailureReceived);
        assertThat(directCancellationMetric(node), equalTo(1L));
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
        assertTrue(BlockingFsRepositoryPlugin.proceedWithRestore.tryAcquire(10, TimeUnit.SECONDS));
        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap").setWaitForCompletion(false).execute();
        assertTrue(BlockingFsRepositoryPlugin.restoreHasStarted.tryAcquire(30, TimeUnit.SECONDS));
        BlockingFsRepositoryPlugin.restoreHasStarted.release();
        disableAllocation();

        final var index = resolveIndex(indexName);
        final var shardId = new ShardId(index, 0);
        final var indicesService = internalCluster().getInstance(IndicesService.class, node);
        final var shard = indicesService.indexServiceSafe(index).getShard(0);
        final var allocationId = shard.routingEntry().allocationId().getId();
        final var clusterService = internalCluster().getInstance(ClusterService.class, node);

        final var shardFailureReceived = new CountDownLatch(1);
        final var transportService = MockTransportService.getInstance(node);
        transportService.addRequestHandlingBehavior("internal:cluster/shard/failure", (handler, request, channel, task) -> {
            if (request instanceof ShardStateAction.FailedShardEntry failedShard) {
                if (failedShard.getShardId().equals(shardId)
                    && ExceptionsHelper.unwrap(failedShard.getFailure(), RecoveryCancelledException.class) != null) {
                    shardFailureReceived.countDown();
                }
            }
            handler.messageReceived(request, channel, task);
        });

        waitNoPendingTasksOnAll();
        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().version(),
            List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(shardId, allocationId, true))
        );
        // Set the cancellation flag, then release restoreShard so checkpoint fires after it completes
        client(node).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();
        BlockingFsRepositoryPlugin.proceedWithRestore.release();

        safeAwait(shardFailureReceived);
        assertThat(directCancellationMetric(node), equalTo(1L));
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

        final var index = resolveIndex(indexName);
        final var shardId = new ShardId(index, 0);
        final var indicesService = internalCluster().getInstance(IndicesService.class, replicaNode);
        final var shard = indicesService.indexServiceSafe(index).getShard(0);
        final var allocationId = shard.routingEntry().allocationId().getId();

        final var shardFailureReceived = new CountDownLatch(1);
        transportService.addRequestHandlingBehavior("internal:cluster/shard/failure", (handler, request, channel, task) -> {
            if (request instanceof ShardStateAction.FailedShardEntry failedShard) {
                if (failedShard.getShardId().equals(shardId) && failedShard.getFailure() instanceof RecoveryCancelledException) {
                    shardFailureReceived.countDown();
                }
            }
            handler.messageReceived(request, channel, task);
        });

        final var clusterService = internalCluster().getInstance(ClusterService.class, replicaNode);
        waitNoPendingTasksOnAll();

        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().version(),
            List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(shardId, allocationId, true))
        );

        client(replicaNode).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();
        proceedWithRecovery.countDown();

        safeAwait(shardFailureReceived);
        waitNoPendingTasksOnAll();
        ensureYellow(indexName);

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
                "Expected unassignment reason to be ALLOCATION_FAILED",
                unassignedInfo.reason(),
                equalTo(UnassignedInfo.Reason.ALLOCATION_FAILED)
            );

            final var failure = unassignedInfo.failure();
            assertNotNull("Unassigned info should have failure details", failure);
            assertThat(
                "Failure should be RecoveryCancelledException",
                ExceptionsHelper.unwrap(failure, RecoveryCancelledException.class),
                notNullValue()
            );
            return true;
        });
        assertThat(directCancellationMetric(replicaNode), equalTo(1L));
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

        final var targetNode = internalCluster().startNode();
        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, 0, sourceNode, targetNode));
        safeAwait(blockedRelocation);
        disableAllocation();

        final var index = resolveIndex(indexName);
        final var shardId = new ShardId(index, 0);
        final var indicesService = internalCluster().getInstance(IndicesService.class, targetNode);
        final var shard = indicesService.indexServiceSafe(index).getShard(0);
        final var allocationId = shard.routingEntry().allocationId().getId();

        final var shardFailureReceived = new CountDownLatch(1);
        sourceTransportService.addRequestHandlingBehavior("internal:cluster/shard/failure", (handler, request, channel, task) -> {
            if (request instanceof ShardStateAction.FailedShardEntry failedShard) {
                if (failedShard.getShardId().equals(shardId)
                    && ExceptionsHelper.unwrap(failedShard.getFailure(), RecoveryCancelledException.class) != null) {
                    shardFailureReceived.countDown();
                }
            }
            handler.messageReceived(request, channel, task);
        });

        final var clusterService = internalCluster().getInstance(ClusterService.class, targetNode);
        waitNoPendingTasksOnAll();
        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().version(),
            List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(shardId, allocationId, true))
        );

        client(targetNode).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();
        proceedWithRelocation.countDown();

        safeAwait(shardFailureReceived);
        assertThat(directCancellationMetric(targetNode), equalTo(1L));

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
    }

    public void testDirectCancellationOfPrimaryRelocationAtHandoverBoundary() throws Exception {
        final var sourceNode = internalCluster().startNode();
        final var indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 0).build());

        for (int i = 0; i < 50; i++) {
            indexDoc(indexName, Integer.toString(i), "f", randomAlphaOfLength(10));
            refresh(indexName);
        }
        flush(indexName);
        ensureGreen(indexName);

        final var targetNode = internalCluster().startNode();

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

        final var index = resolveIndex(indexName);
        final var shardId = new ShardId(index, 0);
        final var indicesService = internalCluster().getInstance(IndicesService.class, targetNode);
        final var shard = indicesService.indexServiceSafe(index).getShard(0);
        final var allocationId = shard.routingEntry().allocationId().getId();

        final var shardFailureReceived = new CountDownLatch(1);
        MockTransportService.getInstance(sourceNode)
            .addRequestHandlingBehavior("internal:cluster/shard/failure", (handler, request, channel, task) -> {
                if (request instanceof ShardStateAction.FailedShardEntry failedShard) {
                    if (failedShard.getShardId().equals(shardId)
                        && ExceptionsHelper.unwrap(failedShard.getFailure(), RecoveryCancelledException.class) != null) {
                        shardFailureReceived.countDown();
                    }
                }
                handler.messageReceived(request, channel, task);
            });

        // Send cancellation while handoff is blocked
        final var clusterService = internalCluster().getInstance(ClusterService.class, targetNode);
        waitNoPendingTasksOnAll();
        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().version(),
            List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(shardId, allocationId, true))
        );
        client(targetNode).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();

        // activateWithPrimaryContext should detect the flag and abort the handover
        proceedWithHandoff.countDown();

        safeAwait(shardFailureReceived);
        assertThat(directCancellationMetric(targetNode), equalTo(1L));

        // The aborted handover leaves the primary on sourceNode
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
    }

    @TestLogging(reason = "test asserts DEBUG log", value = "org.elasticsearch.indices.recovery.TransportCancelRecoveriesAction:DEBUG")
    public void testDirectCancellationFailsAfterPrimaryHandover() throws Exception {
        final var sourceNode = internalCluster().startNode();
        final var indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 0).build());

        for (int i = 0; i < 50; i++) {
            indexDoc(indexName, Integer.toString(i), "f", randomAlphaOfLength(10));
            refresh(indexName);
        }
        flush(indexName);
        ensureGreen(indexName);

        final var targetNode = internalCluster().startNode();

        // Block inside afterIndexShardRecovery. At that point isPrimaryMode=true but state is still RECOVERING.
        assertTrue(TestRecoveryBlockerPlugin.afterRecoveryGate.tryAcquire(10, TimeUnit.SECONDS));

        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, 0, sourceNode, targetNode));

        // Wait until target is blocked
        assertTrue(TestRecoveryBlockerPlugin.afterRecoveryEntered.tryAcquire(10, TimeUnit.SECONDS));
        TestRecoveryBlockerPlugin.afterRecoveryEntered.release();
        disableAllocation();

        final var index = resolveIndex(indexName);
        final var shardId = new ShardId(index, 0);
        final var indicesService = internalCluster().getInstance(IndicesService.class, targetNode);
        final var shard = indicesService.indexServiceSafe(index).getShard(0);
        final var allocationId = shard.routingEntry().allocationId().getId();

        final var clusterService = internalCluster().getInstance(ClusterService.class, targetNode);
        waitNoPendingTasksOnAll();
        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().version(),
            List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(shardId, allocationId, true))
        );

        // The cancellation fails since isPrimaryMode=true.
        try (var mockLog = MockLog.capture(TransportCancelRecoveriesAction.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "debug log for rejected post-handover cancellation",
                    TransportCancelRecoveriesAction.class.getCanonicalName(),
                    Level.DEBUG,
                    "*cancellation flag cannot be set on*"
                )
            );
            client(targetNode).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();
            mockLog.assertAllExpectationsMatched();
        }
        assertThat(directCancellationMetric(targetNode), equalTo(0L));

        // Let the relocation proceed
        TestRecoveryBlockerPlugin.afterRecoveryGate.release();

        // The relocation completed successfully, the primary is now on targetNode
        waitNoPendingTasksOnAll();
        ensureGreen(indexName);
        awaitClusterState(state -> {
            final var primaryShard = state.routingTable().shardRoutingTable(shardId).primaryShard();
            return primaryShard.started() && primaryShard.currentNodeId().equals(state.nodes().resolveNode(targetNode).getId());
        });
    }

    public void testDirectCancellationIgnoredAfterRecoveryFinalize() throws Exception {
        final var node = internalCluster().startNode();
        final var indexName = randomIndexName();

        assertTrue(TestRecoveryBlockerPlugin.afterRecoveryGate.tryAcquire(10, TimeUnit.SECONDS));
        prepareCreate(indexName).setSettings(indexSettings(1, 0).build()).execute();

        assertTrue(TestRecoveryBlockerPlugin.afterRecoveryEntered.tryAcquire(10, TimeUnit.SECONDS));
        TestRecoveryBlockerPlugin.afterRecoveryEntered.release();

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
        disableAllocation();

        // All checkpoints are already past, so the flag is never read.
        client(node).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();

        // Release the gate. postRecovery() will clear the cancellation flag and the shard transitions to STARTED.
        TestRecoveryBlockerPlugin.afterRecoveryGate.release();

        ensureGreen(indexName);
        assertThat(directCancellationMetric(node), equalTo(0L));

        // Confirm the flag was cleared by postRecovery() and the shard is now in STARTED state.
        final var indexShard = indicesService.indexServiceSafe(index).getShard(0);
        assertThat(indexShard.state(), equalTo(IndexShardState.STARTED));
        indexShard.ensureRecoveryNotCancelled();
    }

    public void testCancellationArrivesBeforeShardLockIsAcquired() throws Exception {
        final var node = internalCluster().startNode();
        final var indexName = randomIndexName();

        // Block after before replica shard is created
        assertTrue(TestRecoveryBlockerPlugin.beforeShardCreatedGate.tryAcquire(10, TimeUnit.SECONDS));

        prepareCreate(indexName).setSettings(indexSettings(1, 0).build()).execute();

        assertTrue(TestRecoveryBlockerPlugin.beforeShardCreatedEntered.tryAcquire(10, TimeUnit.SECONDS));
        TestRecoveryBlockerPlugin.beforeShardCreatedEntered.release();

        final var index = resolveIndex(indexName);
        final var indicesService = internalCluster().getInstance(IndicesService.class, node);
        final var indexService = indicesService.indexServiceSafe(index);
        assertThat(indexService.numberOfShards(), equalTo(0));

        final var shardId = new ShardId(index, 0);
        final var clusterService = internalCluster().getInstance(ClusterService.class, node);
        final var allocationId = TestRecoveryBlockerPlugin.latestCreatedShard.get().allocationId();

        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().version(),
            List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(shardId, allocationId.getId(), true))
        );
        client(node).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();

        TestRecoveryBlockerPlugin.beforeShardCreatedGate.release();
        ensureGreen(indexName);
        assertThat(directCancellationMetric(node), equalTo(0L));
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

    public static class BlockingFsRepositoryPlugin extends Plugin implements RepositoryPlugin {
        static final String REPO_TYPE = "blocking_fs";
        static final Semaphore restoreHasStarted = new Semaphore(0);
        static final Semaphore proceedWithRestore = new Semaphore(1);

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
                        try {
                            assertTrue(proceedWithRestore.tryAcquire(10, TimeUnit.SECONDS));
                            proceedWithRestore.release();
                            assertTrue(restoreHasStarted.tryAcquire(10, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
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

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addIndexEventListener(new IndexEventListener() {
                @Override
                public void beforeIndexShardCreated(ShardRouting routing, Settings indexSettings) {
                    try {
                        latestCreatedShard.set(routing);
                        beforeShardCreatedEntered.release();
                        assertTrue(beforeShardCreatedGate.tryAcquire(10, TimeUnit.SECONDS));
                        beforeShardCreatedGate.release();
                        assertTrue(beforeShardCreatedEntered.tryAcquire(10, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                }

                @Override
                public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings, ActionListener<Void> listener) {
                    if (indexShard.recoveryState() == null
                        || indexShard.recoveryState().getRecoverySource().getType() == RecoverySource.Type.PEER) {
                        listener.onResponse(null);
                        return;
                    }
                    try {
                        beforeRecoveryEntered.release();
                        assertTrue(beforeRecoveryGate.tryAcquire(10, TimeUnit.SECONDS));
                        beforeRecoveryGate.release();
                        assertTrue(beforeRecoveryEntered.tryAcquire(10, TimeUnit.SECONDS));
                        listener.onResponse(null);
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                }

                @Override
                public void afterIndexShardRecovery(IndexShard indexShard, ActionListener<Void> listener) {
                    if (indexShard.routingEntry().primary() == false) {
                        listener.onResponse(null);
                        return;
                    }
                    try {
                        afterRecoveryEntered.release();
                        assertTrue(afterRecoveryGate.tryAcquire(10, TimeUnit.SECONDS));
                        afterRecoveryGate.release();
                        assertTrue(afterRecoveryEntered.tryAcquire(10, TimeUnit.SECONDS));
                        listener.onResponse(null);
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                }
            });
        }
    }
}
