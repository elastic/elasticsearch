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
import org.elasticsearch.action.admin.indices.ResizeIndexTestUtils;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.action.shard.ShardStateAction.SHARD_FAILED_ACTION_NAME;
import static org.elasticsearch.indices.recovery.RetryRecoveryIT.FailureTarget.AFTER_INDEX_SHARD_RECOVERY;
import static org.elasticsearch.indices.recovery.RetryRecoveryIT.FailureTarget.BEFORE_INDEX_SHARD_RECOVERY;
import static org.elasticsearch.indices.recovery.RetryRecoveryIT.FailureTarget.STATE_CHANGED_POST_RECOVERY;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
@TestLogging(
    reason = "Useful for test investigation during development",
    value = "org.elasticsearch.indices.recovery.ThrottlingRecoveryService:TRACE,"
        + "org.elasticsearch.indices.cluster.IndicesClusterStateService:TRACE,"
        + "org.elasticsearch.index.shard.IndexShard:TRACE,"
        + "org.elasticsearch.indices.recovery.PeerRecoveryTargetService:TRACE"
)
public class RetryRecoveryIT extends AbstractIndexRecoveryIntegTestCase {
    private static final String RETRY_MESSAGE = "RETRY_CAUSE";
    private static final RuntimeException RETRY_CAUSE = new RuntimeException(RETRY_MESSAGE);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(RetryRecoveryTestPlugin.class);
        return plugins;
    }

    @After
    public void reset() {
        RetryRecoveryTestPlugin.reset();
    }

    public void testRetryOnFailureOnRecoveryFromEmptyStore() {
        String node = internalCluster().startNode();
        String indexName = randomIndexName();

        MockTransportService transportService = MockTransportService.getInstance(node);
        try {
            failTestIfReceiveShardFailure(transportService);

            RetryRecoveryTestPlugin.failureTarget.set(BEFORE_INDEX_SHARD_RECOVERY);

            // Recover from empty store
            createIndex(indexName, indexSettings(1, 0).build());

            ensureGreen(indexName);
            assertThat(RetryRecoveryTestPlugin.recoveryCounter.get(), equalTo(2));
        } finally {
            transportService.clearAllRules();
        }
    }

    public void testRetryOnFailureOnRecoveryFromExistingStore() {
        String node = internalCluster().startNode();
        final var indexName = randomIndexName();

        createIndex(indexName, indexSettings(1, 0).build());
        indexDoc(indexName, "1", "f", randomAlphaOfLength(10));
        flush(indexName);
        ensureGreen(indexName);
        assertAcked(indicesAdmin().prepareClose(indexName));

        MockTransportService transportService = MockTransportService.getInstance(node);
        try {
            failTestIfReceiveShardFailure(transportService);

            RetryRecoveryTestPlugin.reset();
            RetryRecoveryTestPlugin.armRandomFailure();

            // Recover from existing store
            assertAcked(indicesAdmin().prepareOpen(indexName).execute());

            ensureGreen(indexName);
            assertThat(RetryRecoveryTestPlugin.recoveryCounter.get(), equalTo(2));
        } finally {
            transportService.clearAllRules();
        }
    }

    public void testRetryOnFailureOnRecoveryFromLocalShard() {
        String node = internalCluster().startNode();
        final var sourceIndexName = randomIndexName();
        final var targetIndexName = randomIndexName();

        createIndex(sourceIndexName, indexSettings(1, 0).build());
        indexDoc(sourceIndexName, "1", "f", randomAlphaOfLength(10));
        flush(sourceIndexName);
        ensureGreen(sourceIndexName);

        // Required for clone
        updateIndexSettings(Settings.builder().put("index.blocks.write", true), sourceIndexName);

        MockTransportService transportService = MockTransportService.getInstance(node);
        try {
            failTestIfReceiveShardFailure(transportService);

            RetryRecoveryTestPlugin.reset();
            RetryRecoveryTestPlugin.armRandomFailure();

            // Recover from local shard
            ResizeIndexTestUtils.executeResize(ResizeType.CLONE, sourceIndexName, targetIndexName, indexSettings(1, 0));

            ensureGreen(sourceIndexName);
            ensureGreen(targetIndexName);
            assertThat(RetryRecoveryTestPlugin.recoveryCounter.get(), equalTo(2));
        } finally {
            transportService.clearAllRules();
        }
    }

    public void testRetryOnFailureOnRecoveryFromSnapshot() {
        String node = internalCluster().startNode();
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

        MockTransportService transportService = MockTransportService.getInstance(node);
        try {
            failTestIfReceiveShardFailure(transportService);

            RetryRecoveryTestPlugin.reset();
            RetryRecoveryTestPlugin.armRandomFailure();

            // Recover from snapshot
            clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap").setWaitForCompletion(true).execute();

            ensureGreen(indexName);
            assertThat(RetryRecoveryTestPlugin.recoveryCounter.get(), equalTo(2));
        } finally {
            transportService.clearAllRules();
        }
    }

    public void testDontRetryAfterCancellationOfRecoveryFromEmptyStore() throws Exception {
        String node = internalCluster().startNode();
        String indexName = randomIndexName();

        MockTransportService transportService = MockTransportService.getInstance(node);
        try {
            failTestIfReceiveShardFailure(transportService);

            Gate gate = RetryRecoveryTestPlugin.beforeIndexShardRecoveryGate;
            gate.block();

            prepareCreate(indexName, indexSettings(1, 0)).execute();
            gate.await();

            cancelRecovery(indexName, node);
            gate.release();

            // Expect the failed recovery to remove the shard locally and not recreate it
            assertBusy(
                () -> assertNull(
                    internalCluster().getInstance(IndicesService.class, node).indexServiceSafe(resolveIndex(indexName)).getShardOrNull(0)
                )
            );
            assertThat(RetryRecoveryTestPlugin.recoveryCounter.get(), equalTo(1));
            assertThat(
                clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, indexName).get().getStatus(),
                equalTo(ClusterHealthStatus.YELLOW)
            );
        } finally {
            transportService.clearAllRules();
        }
    }

    public void testDontRetryAfterCancellationOfRecoveryFromExistingStore() throws Exception {
        String node = internalCluster().startNode();
        String indexName = randomIndexName();

        createIndex(indexName, indexSettings(1, 0).build());
        indexDoc(indexName, "1", "f", randomAlphaOfLength(10));
        flush(indexName);
        ensureGreen(indexName);
        assertAcked(indicesAdmin().prepareClose(indexName));

        MockTransportService transportService = MockTransportService.getInstance(node);
        try {
            failTestIfReceiveShardFailure(transportService);

            RetryRecoveryTestPlugin.reset();
            Gate gate = RetryRecoveryTestPlugin.beforeIndexShardRecoveryGate;
            gate.block();

            indicesAdmin().prepareOpen(indexName).execute();
            gate.await();

            cancelRecovery(indexName, node);
            gate.release();

            // Expect the failed recovery to remove the shard locally and not recreate it
            assertBusy(
                () -> assertNull(
                    internalCluster().getInstance(IndicesService.class, node).indexServiceSafe(resolveIndex(indexName)).getShardOrNull(0)
                )
            );
            assertThat(RetryRecoveryTestPlugin.recoveryCounter.get(), equalTo(1));
            // EXISTING_STORE inactive primaries are RED (see ClusterShardHealth#getInactivePrimaryHealth)
            assertThat(clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, indexName).get().getStatus(), equalTo(ClusterHealthStatus.RED));
        } finally {
            transportService.clearAllRules();
        }
    }

    public void testDontRetryAfterCancellationOfRecoveryFromLocalShard() throws Exception {
        String node = internalCluster().startNode();
        final var sourceIndexName = randomIndexName();
        final var targetIndexName = randomIndexName();

        createIndex(sourceIndexName, indexSettings(1, 0).build());
        indexDoc(sourceIndexName, "1", "f", randomAlphaOfLength(10));
        flush(sourceIndexName);
        ensureGreen(sourceIndexName);

        // Required for clone
        updateIndexSettings(Settings.builder().put("index.blocks.write", true), sourceIndexName);

        MockTransportService transportService = MockTransportService.getInstance(node);
        try {
            failTestIfReceiveShardFailure(transportService);

            RetryRecoveryTestPlugin.reset();
            Gate gate = RetryRecoveryTestPlugin.beforeIndexShardRecoveryGate;
            gate.block();

            ResizeIndexTestUtils.executeResize(ResizeType.CLONE, sourceIndexName, targetIndexName, indexSettings(1, 0));
            gate.await();

            cancelRecovery(targetIndexName, node);
            gate.release();

            // Expect the failed recovery to remove the target shard locally and not recreate it
            assertBusy(
                () -> assertNull(
                    internalCluster().getInstance(IndicesService.class, node)
                        .indexServiceSafe(resolveIndex(targetIndexName))
                        .getShardOrNull(0)
                )
            );
            assertThat(RetryRecoveryTestPlugin.recoveryCounter.get(), equalTo(1));
            assertThat(
                clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, targetIndexName).get().getStatus(),
                equalTo(ClusterHealthStatus.YELLOW)
            );
        } finally {
            transportService.clearAllRules();
        }
    }

    public void testDontRetryAfterCancellationOfRecoveryFromSnapshot() throws Exception {
        String node = internalCluster().startNode();
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

        MockTransportService transportService = MockTransportService.getInstance(node);
        try {
            failTestIfReceiveShardFailure(transportService);

            RetryRecoveryTestPlugin.reset();
            Gate gate = RetryRecoveryTestPlugin.beforeIndexShardRecoveryGate;
            gate.block();

            clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap").setWaitForCompletion(false).execute();
            gate.await();

            cancelRecovery(indexName, node);
            gate.release();

            // Expect the failed recovery to remove the shard locally and not recreate it
            assertBusy(
                () -> assertNull(
                    internalCluster().getInstance(IndicesService.class, node).indexServiceSafe(resolveIndex(indexName)).getShardOrNull(0)
                )
            );
            assertThat(RetryRecoveryTestPlugin.recoveryCounter.get(), equalTo(1));
            assertThat(
                clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, indexName).get().getStatus(),
                equalTo(ClusterHealthStatus.YELLOW)
            );
        } finally {
            transportService.clearAllRules();
        }
    }

    public void testRetryOnFailureOnRecoveryFromEmptyStoreRaceWithIndexDeletion() throws Exception {
        String node = internalCluster().startNode();
        String indexName = randomIndexName();

        MockTransportService transportService = MockTransportService.getInstance(node);
        try {
            failTestIfReceiveShardFailure(transportService);

            RetryRecoveryTestPlugin.armRandomFailure();
            Gate gate = RetryRecoveryTestPlugin.randomGateBeforeTargetFailure();
            gate.block();

            prepareCreate(indexName, indexSettings(1, 0)).execute();
            gate.await();
            indicesAdmin().prepareDelete(indexName).execute();

            // Release will make recovery/retry race with index deletion
            gate.release();

            waitNoPendingTasksOnAll();
            assertThat(indexExists(indexName), equalTo(false));
        } finally {
            transportService.clearAllRules();
        }
    }

    public void testRetryOnFailureOnRecoveryFromExistingStoreRaceWithIndexDeletion() throws Exception {
        String node = internalCluster().startNode();
        String indexName = randomIndexName();

        createIndex(indexName, indexSettings(1, 0).build());
        indexDoc(indexName, "1", "f", randomAlphaOfLength(10));
        flush(indexName);
        ensureGreen(indexName);
        assertAcked(indicesAdmin().prepareClose(indexName));

        MockTransportService transportService = MockTransportService.getInstance(node);
        try {
            failTestIfReceiveShardFailure(transportService);

            RetryRecoveryTestPlugin.armRandomFailure();
            Gate gate = RetryRecoveryTestPlugin.randomGateBeforeTargetFailure();
            gate.block();

            // Recover from existing store
            indicesAdmin().prepareOpen(indexName).execute();
            gate.await();
            indicesAdmin().prepareDelete(indexName).execute();

            // Release recovery will make recovery/retry race with index deletion
            gate.release();

            waitNoPendingTasksOnAll();
            assertThat(indexExists(indexName), equalTo(false));
        } finally {
            transportService.clearAllRules();
        }
    }

    public void testRetryOnFailureOnRecoveryFromLocalShardRaceWithIndexDeletion() throws Exception {
        String node = internalCluster().startNode();
        final var sourceIndexName = randomIndexName();
        final var targetIndexName = randomIndexName();

        createIndex(sourceIndexName, indexSettings(1, 0).build());
        indexDoc(sourceIndexName, "1", "f", randomAlphaOfLength(10));
        flush(sourceIndexName);
        ensureGreen(sourceIndexName);

        // Required for clone
        updateIndexSettings(Settings.builder().put("index.blocks.write", true), sourceIndexName);

        MockTransportService transportService = MockTransportService.getInstance(node);
        try {
            failTestIfReceiveShardFailure(transportService);

            RetryRecoveryTestPlugin.armRandomFailure();
            Gate gate = RetryRecoveryTestPlugin.randomGateBeforeTargetFailure();
            gate.block();

            // Recover from local shard async
            ResizeIndexTestUtils.executeResize(ResizeType.CLONE, sourceIndexName, targetIndexName, indexSettings(1, 0));
            gate.await();
            indicesAdmin().prepareDelete(targetIndexName).execute();

            // Release recovery will make recovery/retry race with index deletion
            gate.release();

            waitNoPendingTasksOnAll();
            assertThat(indexExists(targetIndexName), equalTo(false));
        } finally {
            transportService.clearAllRules();
        }
    }

    public void testRetryOnFailureOnRecoveryFromSnapshotRaceWithIndexDeletion() throws Exception {
        String node = internalCluster().startNode();
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

        MockTransportService transportService = MockTransportService.getInstance(node);
        try {
            failTestIfReceiveShardFailure(transportService);

            RetryRecoveryTestPlugin.reset();
            RetryRecoveryTestPlugin.armRandomFailure();
            Gate gate = RetryRecoveryTestPlugin.randomGateBeforeTargetFailure();
            gate.block();

            // Recover from snapshot async
            clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap").setWaitForCompletion(false).execute();
            gate.await();
            indicesAdmin().prepareDelete(indexName).execute();

            // Release recovery will make recovery/retry race with index deletion
            gate.release();

            waitNoPendingTasksOnAll();
            assertThat(indexExists(indexName), equalTo(false));
        } finally {
            transportService.clearAllRules();
        }
    }

    public void testRetryOnFailureOnRecoveryFromEmptyStoreRaceWithNetworkDisruption() throws Exception {
        String masterA = internalCluster().startMasterOnlyNode();
        String masterB = internalCluster().startMasterOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        String indexName = randomIndexName();

        MockTransportService masterATransport = MockTransportService.getInstance(masterA);
        MockTransportService masterBTransport = MockTransportService.getInstance(masterB);
        try {
            failTestIfReceiveShardFailure(masterATransport);
            failTestIfReceiveShardFailure(masterBTransport);

            RetryRecoveryTestPlugin.armRandomFailure();
            Gate gate = RetryRecoveryTestPlugin.randomGateBeforeTargetFailure();
            gate.block();

            // Create index async
            prepareCreate(indexName, indexSettings(1, 0)).execute();
            gate.await();

            // Isolating dataNode will cause shard to go unassigned
            NetworkDisruption disruption = new NetworkDisruption(
                new NetworkDisruption.TwoPartitions(Set.of(dataNode), Set.of(masterA, masterB)),
                NetworkDisruption.DISCONNECT
            );
            internalCluster().setDisruptionScheme(disruption);
            disruption.startDisrupting();
            String dataNodeId = internalCluster().clusterService(dataNode).localNode().getId();
            awaitClusterState(masterA, state -> state.nodes().nodeExists(dataNodeId) == false);

            // Release recovery will make recovery/retry race with network disruption
            gate.release();
            disruption.stopDisrupting();

            waitNoPendingTasksOnAll();
            ensureGreen(indexName);
        } finally {
            masterATransport.clearAllRules();
            masterBTransport.clearAllRules();
        }
    }

    public void testRetryOnFailureOnRecoveryFromFromExistingStoreRaceWithNetworkDisruption() throws Exception {
        String masterA = internalCluster().startMasterOnlyNode();
        String masterB = internalCluster().startMasterOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        String indexName = randomIndexName();

        createIndex(indexName, indexSettings(1, 0).build());
        indexDoc(indexName, "1", "f", randomAlphaOfLength(10));
        flush(indexName);
        ensureGreen(indexName);
        assertAcked(indicesAdmin().prepareClose(indexName));

        MockTransportService masterATransport = MockTransportService.getInstance(masterA);
        MockTransportService masterBTransport = MockTransportService.getInstance(masterB);
        try {
            failTestIfReceiveShardFailure(masterATransport);
            failTestIfReceiveShardFailure(masterBTransport);

            RetryRecoveryTestPlugin.armRandomFailure();
            Gate gate = RetryRecoveryTestPlugin.randomGateBeforeTargetFailure();
            gate.block();

            // Recover from existing store async
            indicesAdmin().prepareOpen(indexName).execute();
            gate.await();

            // Isolating dataNode will cause shard to go unassigned
            NetworkDisruption disruption = new NetworkDisruption(
                new NetworkDisruption.TwoPartitions(Set.of(dataNode), Set.of(masterA, masterB)),
                NetworkDisruption.DISCONNECT
            );
            internalCluster().setDisruptionScheme(disruption);
            disruption.startDisrupting();
            String dataNodeId = internalCluster().clusterService(dataNode).localNode().getId();
            awaitClusterState(masterA, state -> state.nodes().nodeExists(dataNodeId) == false);

            // Release recovery will make recovery/retry race with network disruption
            gate.release();
            disruption.stopDisrupting();

            waitNoPendingTasksOnAll();
            ensureGreen(indexName);
        } finally {
            masterATransport.clearAllRules();
            masterBTransport.clearAllRules();
        }
    }

    public void testRetryOnFailureOnRecoveryFromLocalShardRaceWithNetworkDisruption() throws Exception {
        String masterA = internalCluster().startMasterOnlyNode();
        String masterB = internalCluster().startMasterOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        final var sourceIndexName = randomIndexName();
        final var targetIndexName = randomIndexName();

        createIndex(sourceIndexName, indexSettings(1, 0).build());
        indexDoc(sourceIndexName, "1", "f", randomAlphaOfLength(10));
        flush(sourceIndexName);
        ensureGreen(sourceIndexName);

        // Required for clone
        updateIndexSettings(Settings.builder().put("index.blocks.write", true), sourceIndexName);

        MockTransportService masterATransport = MockTransportService.getInstance(masterA);
        MockTransportService masterBTransport = MockTransportService.getInstance(masterB);
        try {
            failTestIfReceiveShardFailure(masterATransport);
            failTestIfReceiveShardFailure(masterBTransport);

            RetryRecoveryTestPlugin.armRandomFailure();
            Gate gate = RetryRecoveryTestPlugin.randomGateBeforeTargetFailure();
            gate.block();

            // Recover from local shard async
            ResizeIndexTestUtils.executeResize(ResizeType.CLONE, sourceIndexName, targetIndexName, indexSettings(1, 0));
            gate.await();

            // Isolating dataNode will cause shard to go unassigned
            NetworkDisruption disruption = new NetworkDisruption(
                new NetworkDisruption.TwoPartitions(Set.of(dataNode), Set.of(masterA, masterB)),
                NetworkDisruption.DISCONNECT
            );
            internalCluster().setDisruptionScheme(disruption);
            disruption.startDisrupting();
            String dataNodeId = internalCluster().clusterService(dataNode).localNode().getId();
            awaitClusterState(masterA, state -> state.nodes().nodeExists(dataNodeId) == false);

            // Release recovery will make recovery/retry race with network disruption
            gate.release();
            disruption.stopDisrupting();

            waitNoPendingTasksOnAll();
            ensureGreen(sourceIndexName);
            ensureGreen(targetIndexName);
        } finally {
            masterATransport.clearAllRules();
            masterBTransport.clearAllRules();
        }
    }

    public void testRetryOnFailureOnRecoveryFromSnapshotRaceWithNetworkDisruption() throws Exception {
        String masterA = internalCluster().startMasterOnlyNode();
        String masterB = internalCluster().startMasterOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
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

        MockTransportService masterATransport = MockTransportService.getInstance(masterA);
        MockTransportService masterBTransport = MockTransportService.getInstance(masterB);
        try {
            failTestIfReceiveShardFailure(masterATransport);
            failTestIfReceiveShardFailure(masterBTransport);

            RetryRecoveryTestPlugin.armRandomFailure();
            Gate gate = RetryRecoveryTestPlugin.randomGateBeforeTargetFailure();
            gate.block();

            // Recover from snapshot async
            clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap").setWaitForCompletion(false).execute();
            gate.await();

            // Isolating dataNode will cause shard to go unassigned
            NetworkDisruption disruption = new NetworkDisruption(
                new NetworkDisruption.TwoPartitions(Set.of(dataNode), Set.of(masterA, masterB)),
                NetworkDisruption.DISCONNECT
            );
            internalCluster().setDisruptionScheme(disruption);
            disruption.startDisrupting();
            String dataNodeId = internalCluster().clusterService(dataNode).localNode().getId();
            awaitClusterState(masterA, state -> state.nodes().nodeExists(dataNodeId) == false);

            // Release recovery will make recovery/retry race with network disruption
            gate.release();
            disruption.stopDisrupting();

            waitNoPendingTasksOnAll();
            ensureGreen(indexName);
        } finally {
            masterATransport.clearAllRules();
            masterBTransport.clearAllRules();
        }
    }

    /// Synchronous direct cancellation of recovery for shard 0 of given index on given node
    private static void cancelRecovery(String indexName, String node) throws InterruptedException, ExecutionException {
        final var index = resolveIndex(indexName);
        final var shardId = new ShardId(index, 0);
        final var indicesService = internalCluster().getInstance(IndicesService.class, node);
        final var shard = indicesService.indexServiceSafe(index).getShard(0);
        final var allocationId = shard.routingEntry().allocationId().getId();
        final var clusterService = internalCluster().getInstance(ClusterService.class, node);
        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().term(),
            clusterService.state().version(),
            List.of(new ShardRecoveryCancellation(shardId, allocationId, true))
        );
        client(node).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();
    }

    /// Local recovery retries is about preventing the round trip to master on a failed recovery
    /// (we retry directly on the data node instead).
    /// Since master would also retry, that could mask potential bugs in the local retry functionality.
    /// This utility method prevents that by failing the test if master receives a "shard failed" message.
    private static void failTestIfReceiveShardFailure(MockTransportService mockTransportService) {
        mockTransportService.addRequestHandlingBehavior(
            SHARD_FAILED_ACTION_NAME,
            (handler, request, channel, task) -> fail("should not send shard failure")
        );
    }

    /// Think of a Gate as... well, a gate with a visitor and a guard.
    /// The visitor tries to [enter] the gate and when it leaves, [exit] the gate.
    /// The guard might prevent the visitor from entering by [block] the gate, then [await] for visitor to try to [enter],
    /// and finally [release] to let the visitor in.
    /// Visitor/T1:
    /// ```
    /// gate.enter();
    /// // Do stuff while inside
    /// gate.exit();
    /// ```
    /// Guard/T2:
    /// ```
    /// gate.block();
    /// gate.await();
    /// // Do stuff while visitor is waiting to enter
    /// gate.release();
    /// ```
    static class Gate {
        private final Semaphore gate = new Semaphore(1);
        private final Semaphore entered = new Semaphore(0);
        /// Name is useful for logging while testing
        private final String name;

        Gate(String name) {
            this.name = name;
        }

        void reset() {
            gate.drainPermits();
            gate.release();
            entered.drainPermits();
        }

        /// Block visitor from enter
        void block() {
            safeAcquire(gate);
        }

        /// Wait for visitor to try and enter
        void await() {
            safeAcquire(entered);
            entered.release();
        }

        /// Allow visitor to enter
        public void release() {
            gate.release();
        }

        /// Try to enter through the gate
        void enter() {
            entered.release();
            safeAcquire(gate);
        }

        /// Exit through the gate
        void exit() {
            gate.release();
            safeAcquire(entered);
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /// This plugin does a few things:
    /// - Count number of recovery attempts [recoveryCounter]
    /// - Inject failures into recover path through [IndexEventListener] and [failureTarget] + [FailureTarget]
    /// - Concurrency control by injecting [Gate]s on shard creation/recovery path through [IndexEventListener]
    /// - Set indices.recovery.local_retry=true
    public static class RetryRecoveryTestPlugin extends Plugin {
        private static final AtomicReference<FailureTarget> failureTarget = new AtomicReference<>(null);
        private static final AtomicInteger recoveryCounter = new AtomicInteger();

        // Gates in the order they are invoked
        private static final Gate beforeIndexShardCreatedGate = new Gate("beforeIndexShardCreateGate");
        private static final Gate onStoreCreatedGate = new Gate("onStoreCreatedGate");
        private static final Gate afterIndexShardCreatedGate = new Gate("afterIndexShardCreatedGate");
        private static final Gate stateChangeRecoveringGate = new Gate("stateChangeRecoveringGate");
        private static final Gate beforeIndexShardRecoveryGate = new Gate("beforeIndexShardRecoveryGate");
        private static final Gate stateChangePostRecoveryGate = new Gate("stateChangePostRecoveryGate");
        private static final List<Gate> allGates = List.of(
            beforeIndexShardCreatedGate,
            onStoreCreatedGate,
            afterIndexShardCreatedGate,
            stateChangeRecoveringGate,
            beforeIndexShardRecoveryGate,
            stateChangePostRecoveryGate
        );

        public static void reset() {
            failureTarget.set(null);
            recoveryCounter.set(0);
            allGates.forEach(Gate::reset);
        }

        /// Arm index event listener with a random failure target
        /// This will cause the next recovery to fail with a [RETRY_CAUSE]
        /// exception when it reaches the [FailureTarget]
        public static void armRandomFailure() {
            failureTarget.set(randomFrom(FailureTarget.values()));
        }

        /// Returns a [Gate] that sits at some random point before the currently armed [FailureTarget].
        /// This is useful because we want to race recovery retry against some other concurrent event or operation
        /// and in order to do that we want to make that the recovery has started but not yet failed.
        public static Gate randomGateBeforeTargetFailure() {
            assert failureTarget.get() != null;
            List<Gate> validGates = switch (failureTarget.get()) {
                case BEFORE_INDEX_SHARD_RECOVERY, AFTER_INDEX_SHARD_RECOVERY -> allGatesExcept(stateChangePostRecoveryGate);
                case STATE_CHANGED_POST_RECOVERY -> allGates;
            };
            return randomFrom(validGates);
        }

        public static List<Gate> allGatesExcept(Gate... excluded) {
            List<Gate> result = new ArrayList<>(allGates);
            for (Gate gate : excluded) {
                result.remove(gate);
            }
            return result;
        }

        @Override
        public Settings additionalSettings() {
            return Settings.builder()
                .put(super.additionalSettings())
                .put(IndicesClusterStateService.LOCAL_RECOVERY_RETRY.getKey(), true)
                .build();
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addIndexEventListener(new IndexEventListener() {

                @Override
                public void beforeIndexShardCreated(ShardRouting routing, Settings indexSettings) {
                    // Failure here will not cause recovery retry, only gate
                    beforeIndexShardCreatedGate.enter();
                    beforeIndexShardCreatedGate.exit();
                }

                @Override
                public void onStoreCreated(ShardId shardId) {
                    // Failure here will not cause recovery retry, only gate
                    onStoreCreatedGate.enter();
                    onStoreCreatedGate.exit();
                }

                @Override
                public void afterIndexShardCreated(IndexShard indexShard) {
                    // Failure here will not cause recovery retry, only gate
                    afterIndexShardCreatedGate.enter();
                    afterIndexShardCreatedGate.exit();
                }

                @Override
                public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings, ActionListener<Void> listener) {
                    beforeIndexShardRecoveryGate.enter();
                    try {
                        maybeThrow(BEFORE_INDEX_SHARD_RECOVERY);
                        listener.onResponse(null);
                    } finally {
                        beforeIndexShardRecoveryGate.exit();
                    }
                }

                @Override
                public void afterIndexShardRecovery(IndexShard indexShard, ActionListener<Void> listener) {
                    maybeThrow(AFTER_INDEX_SHARD_RECOVERY);
                    listener.onResponse(null);
                }

                @Override
                public void indexShardStateChanged(
                    IndexShard indexShard,
                    IndexShardState previousState,
                    IndexShardState currentState,
                    String reason
                ) {
                    if (currentState == IndexShardState.RECOVERING) {
                        stateChangeRecoveringGate.enter();
                        recoveryCounter.incrementAndGet();
                        stateChangeRecoveringGate.exit();
                    }
                    if (currentState == IndexShardState.POST_RECOVERY) {
                        stateChangePostRecoveryGate.enter();
                        try {
                            maybeThrow(STATE_CHANGED_POST_RECOVERY);
                        } finally {
                            stateChangePostRecoveryGate.exit();
                        }
                    }
                }

                private void maybeThrow(FailureTarget target) {
                    if (failureTarget.compareAndSet(target, null)) {
                        throw RETRY_CAUSE;
                    }
                }
            });
        }
    }

    /// Failure target describe different possible failure points during recovery.
    /// Typically on different calls to [IndexEventListener].
    enum FailureTarget {
        BEFORE_INDEX_SHARD_RECOVERY,
        AFTER_INDEX_SHARD_RECOVERY,
        STATE_CHANGED_POST_RECOVERY
    }
}
