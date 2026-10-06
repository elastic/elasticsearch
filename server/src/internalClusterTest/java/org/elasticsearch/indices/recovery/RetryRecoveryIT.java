/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.action.admin.indices.ResizeIndexTestUtils;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
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
import org.elasticsearch.test.MockIndexEventListener;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.action.shard.ShardStateAction.SHARD_FAILED_ACTION_NAME;
import static org.elasticsearch.indices.recovery.RetryRecoveryIT.FailureTarget.AFTER_INDEX_SHARD_RECOVERY;
import static org.elasticsearch.indices.recovery.RetryRecoveryIT.FailureTarget.BEFORE_INDEX_SHARD_RECOVERY;
import static org.elasticsearch.indices.recovery.RetryRecoveryIT.FailureTarget.STATE_CHANGED_POST_RECOVERY;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RetryRecoveryIT extends AbstractIndexRecoveryIntegTestCase {
    private static final String RETRY_MESSAGE = "RETRY_CAUSE";
    private static final RuntimeException RETRY_CAUSE = new RuntimeException(RETRY_MESSAGE);

    private final AtomicReference<FailureTarget> failureTarget = new AtomicReference<>();
    private final AtomicInteger recoveryCounter = new AtomicInteger();
    private final AtomicReference<CyclicBarrier> recoveryBarrier = new AtomicReference<>();

    private final IndexEventListener recoveryListener = new IndexEventListener() {
        @Override
        public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings, ActionListener<Void> listener) {
            maybePauseRecovery();
            maybeThrow(BEFORE_INDEX_SHARD_RECOVERY);
            listener.onResponse(null);
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
                recoveryCounter.incrementAndGet();
            }
            if (currentState == IndexShardState.POST_RECOVERY) {
                maybeThrow(STATE_CHANGED_POST_RECOVERY);
            }
        }

        private void maybeThrow(FailureTarget target) {
            if (failureTarget.compareAndSet(target, null)) {
                throw RETRY_CAUSE;
            }
        }

        private void maybePauseRecovery() {
            final var barrier = recoveryBarrier.getAndSet(null);
            if (barrier != null) {
                safeAwait(barrier);
                safeAwait(barrier);
            }
        }
    };

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockIndexEventListener.TestPlugin.class);
        plugins.add(RetryRecoveryTestPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(IndicesClusterStateService.INDICES_RECOVERY_LOCAL_RETRY_SETTING.getKey(), true)
            .build();
    }

    @After
    public void resetDirectoryAce() {
        RetryRecoveryTestPlugin.reset();
    }

    public void testRetryOnFailureOnRecoveryFromEmptyStore() {
        String node = internalCluster().startNode();
        String indexName = randomIndexName();

        MockTransportService transportService = MockTransportService.getInstance(node);
        try {
            failTestIfReceiveShardFailure(transportService);

            armRandomFailure();

            // Recover from empty store
            createIndex(indexName, indexSettings(1, 0).build());

            ensureGreen(indexName);
            assertLocalRetries(indexName, 1);
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

            armRandomFailure();

            // Recover from existing store
            assertAcked(indicesAdmin().prepareOpen(indexName).execute());

            ensureGreen(indexName);
            assertLocalRetries(indexName, 1);
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

            armRandomFailure();

            // Recover from local shard
            ResizeIndexTestUtils.executeResize(ResizeType.CLONE, sourceIndexName, targetIndexName, indexSettings(1, 0));

            ensureGreen(sourceIndexName);
            ensureGreen(targetIndexName);
            assertLocalRetries(targetIndexName, 1);
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

            armRandomFailure();

            // Recover from snapshot
            clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap").setWaitForCompletion(true).execute();

            ensureGreen(indexName);
            assertLocalRetries(indexName, 1);
        } finally {
            transportService.clearAllRules();
        }
    }

    public void testDontRetryAfterShardClosedDuringRecoveryFromEmptyStore() throws Exception {
        String node = internalCluster().startNode();
        String indexName = randomIndexName();

        MockTransportService transportService = MockTransportService.getInstance(node);
        try {
            failTestIfReceiveShardFailure(transportService);

            final var recoveryBarrier = armRecoveryPause();

            prepareCreate(indexName, indexSettings(1, 0)).execute();
            safeAwait(recoveryBarrier);

            PlainActionFuture<Void> closed = new PlainActionFuture<>();
            internalCluster().getInstance(IndicesService.class, node)
                .indexServiceSafe(resolveIndex(indexName))
                .removeShard(0, "test", internalCluster().getInstance(ThreadPool.class, node).generic(), closed);
            safeAwait(recoveryBarrier);
            safeGet(closed);

            assertThat(recoveryCounter.get(), equalTo(1));
            assertThat(
                clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, indexName).get().getStatus(),
                equalTo(ClusterHealthStatus.YELLOW)
            );
        } finally {
            transportService.clearAllRules();
        }
    }

    public void testDontRetryAfterShardClosedDuringRecoveryFromExistingStore() throws Exception {
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

            final var recoveryBarrier = armRecoveryPause();

            indicesAdmin().prepareOpen(indexName).execute();
            safeAwait(recoveryBarrier);

            PlainActionFuture<Void> closed = new PlainActionFuture<>();
            internalCluster().getInstance(IndicesService.class, node)
                .indexServiceSafe(resolveIndex(indexName))
                .removeShard(0, "test", internalCluster().getInstance(ThreadPool.class, node).generic(), closed);
            safeAwait(recoveryBarrier);
            safeGet(closed);

            assertThat(recoveryCounter.get(), equalTo(1));
            // EXISTING_STORE inactive primaries are RED (see ClusterShardHealth#getInactivePrimaryHealth)
            assertThat(clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, indexName).get().getStatus(), equalTo(ClusterHealthStatus.RED));
        } finally {
            transportService.clearAllRules();
        }
    }

    public void testDontRetryAfterShardClosedDuringRecoveryFromLocalShard() throws Exception {
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

            final var recoveryBarrier = armRecoveryPause();

            // Recover from local shard
            ResizeIndexTestUtils.executeResize(ResizeType.CLONE, sourceIndexName, targetIndexName, indexSettings(1, 0));
            safeAwait(recoveryBarrier);

            PlainActionFuture<Void> closed = new PlainActionFuture<>();
            internalCluster().getInstance(IndicesService.class, node)
                .indexServiceSafe(resolveIndex(targetIndexName))
                .removeShard(0, "test", internalCluster().getInstance(ThreadPool.class, node).generic(), closed);
            safeAwait(recoveryBarrier);
            safeGet(closed);

            assertThat(recoveryCounter.get(), equalTo(1));
            assertThat(
                clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, targetIndexName).get().getStatus(),
                equalTo(ClusterHealthStatus.YELLOW)
            );
        } finally {
            transportService.clearAllRules();
        }
    }

    public void testDontRetryAfterShardClosedDuringRecoveryFromSnapshot() throws Exception {
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

            final var recoveryBarrier = armRecoveryPause();

            // Recover from snapshot
            clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap").setWaitForCompletion(true).execute();
            safeAwait(recoveryBarrier);

            PlainActionFuture<Void> closed = new PlainActionFuture<>();
            internalCluster().getInstance(IndicesService.class, node)
                .indexServiceSafe(resolveIndex(indexName))
                .removeShard(0, "test", internalCluster().getInstance(ThreadPool.class, node).generic(), closed);
            safeAwait(recoveryBarrier);
            safeGet(closed);

            assertThat(recoveryCounter.get(), equalTo(1));
            assertThat(
                clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, indexName).get().getStatus(),
                equalTo(ClusterHealthStatus.YELLOW)
            );
        } finally {
            transportService.clearAllRules();
        }
    }

    public void testRetryOnAlreadyClosedExceptionDuringCreateEmptyFromEmptyStore() {
        String node = internalCluster().startNode();
        String indexName = randomIndexName();

        MockTransportService transportService = MockTransportService.getInstance(node);
        try {
            failTestIfReceiveShardFailure(transportService);

            RetryRecoveryTestPlugin.armDirectoryAce();

            // Recover from empty store; Store.createEmpty hits a one-shot AlreadyClosedException
            createIndex(indexName, indexSettings(1, 0).build());

            ensureGreen(indexName);
            assertLocalRetries(indexName, 1);
        } finally {
            transportService.clearAllRules();
        }
    }

    public void testRetryOnAlreadyClosedExceptionDuringAddIndicesFromLocalShards() {
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

            RetryRecoveryTestPlugin.armDirectoryAce();

            // Recover from local shards; addIndices' temporary IndexWriter hits ACE once
            ResizeIndexTestUtils.executeResize(ResizeType.CLONE, sourceIndexName, targetIndexName, indexSettings(1, 0));

            ensureGreen(sourceIndexName);
            ensureGreen(targetIndexName);
            assertLocalRetries(targetIndexName, 1);
        } finally {
            transportService.clearAllRules();
        }
    }

    public void testRetryOnAlreadyClosedExceptionDuringBootstrapNewHistoryFromExistingStore() throws Exception {
        internalCluster().startMasterOnlyNode();
        String node1 = internalCluster().startNode();
        final var indexName = randomIndexName();

        createIndex(indexName, indexSettings(1, 1).build());
        indexDoc(indexName, "1", "f", randomAlphaOfLength(10));
        flush(indexName);
        String node2 = internalCluster().startNode();
        ensureGreen(indexName);

        Settings node1DataPathSettings = internalCluster().dataPathSettings(node1);
        internalCluster().stopNode(node1);

        // Index on node2 so node1's copy becomes stale in the master's in-sync set
        indexDoc(indexName, "2", "f", randomAlphaOfLength(10));
        flush(indexName);
        internalCluster().stopNode(node2);

        node1 = internalCluster().startNode(node1DataPathSettings);
        // Only one data node remains; drop replicas so allocate_stale_primary can go green
        updateIndexSettings(Settings.builder().put("index.number_of_replicas", 0), indexName);

        MockTransportService transportService = MockTransportService.getInstance(node1);
        try {
            failTestIfReceiveShardFailure(transportService);

            RetryRecoveryTestPlugin.armDirectoryAce();

            // Force stale primary → bootstrapNewHistory temporary IndexWriter hits ACE once
            ClusterRerouteUtils.reroute(client(), new AllocateStalePrimaryAllocationCommand(indexName, 0, node1, true));

            ensureGreen(indexName);
            assertLocalRetries(indexName, 1);
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

            armRandomFailure();
            final var recoveryBarrier = armRecoveryPause();

            prepareCreate(indexName, indexSettings(1, 0)).execute();
            safeAwait(recoveryBarrier);
            indicesAdmin().prepareDelete(indexName).execute();

            // Release will make recovery/retry race with index deletion
            safeAwait(recoveryBarrier);

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

            armRandomFailure();
            final var recoveryBarrier = armRecoveryPause();

            // Recover from existing store
            indicesAdmin().prepareOpen(indexName).execute();
            safeAwait(recoveryBarrier);
            indicesAdmin().prepareDelete(indexName).execute();

            // Release recovery will make recovery/retry race with index deletion
            safeAwait(recoveryBarrier);

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

            armRandomFailure();
            final var recoveryBarrier = armRecoveryPause();

            // Recover from local shard async
            ResizeIndexTestUtils.executeResize(ResizeType.CLONE, sourceIndexName, targetIndexName, indexSettings(1, 0));
            safeAwait(recoveryBarrier);
            indicesAdmin().prepareDelete(targetIndexName).execute();

            // Release recovery will make recovery/retry race with index deletion
            safeAwait(recoveryBarrier);

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

            armRandomFailure();
            final var recoveryBarrier = armRecoveryPause();

            // Recover from snapshot async
            clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap").setWaitForCompletion(false).execute();
            safeAwait(recoveryBarrier);
            indicesAdmin().prepareDelete(indexName).execute();

            // Release recovery will make recovery/retry race with index deletion
            safeAwait(recoveryBarrier);

            waitNoPendingTasksOnAll();
            assertThat(indexExists(indexName), equalTo(false));
        } finally {
            transportService.clearAllRules();
        }
    }

    public void testRetryOnFailureOnRecoveryFromEmptyStoreRaceWithNetworkDisruption() throws Exception {
        String master = internalCluster().startMasterOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        String indexName = randomIndexName();

        MockTransportService masterATransport = MockTransportService.getInstance(master);
        try {
            failTestIfReceiveShardFailure(masterATransport);

            armRandomFailure();
            final var recoveryBarrier = armRecoveryPause();

            // Create index async
            prepareCreate(indexName, indexSettings(1, 0)).execute();
            safeAwait(recoveryBarrier);

            // Isolating dataNode will cause shard to go unassigned
            NetworkDisruption disruption = new NetworkDisruption(
                new NetworkDisruption.TwoPartitions(Set.of(dataNode), Set.of(master)),
                NetworkDisruption.DISCONNECT
            );
            internalCluster().setDisruptionScheme(disruption);
            disruption.startDisrupting();
            String dataNodeId = internalCluster().clusterService(dataNode).localNode().getId();
            awaitClusterState(master, state -> state.nodes().nodeExists(dataNodeId) == false);

            // Release recovery will make recovery/retry race with network disruption
            safeAwait(recoveryBarrier);
            disruption.stopDisrupting();

            waitNoPendingTasksOnAll();
            ensureGreen(indexName);
        } finally {
            masterATransport.clearAllRules();
        }
    }

    public void testRetryOnFailureOnRecoveryFromExistingStoreRaceWithNetworkDisruption() throws Exception {
        String master = internalCluster().startMasterOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        String indexName = randomIndexName();

        createIndex(indexName, indexSettings(1, 0).build());
        indexDoc(indexName, "1", "f", randomAlphaOfLength(10));
        flush(indexName);
        ensureGreen(indexName);
        assertAcked(indicesAdmin().prepareClose(indexName));

        MockTransportService masterATransport = MockTransportService.getInstance(master);
        try {
            failTestIfReceiveShardFailure(masterATransport);

            armRandomFailure();
            final var recoveryBarrier = armRecoveryPause();

            // Recover from existing store async
            indicesAdmin().prepareOpen(indexName).execute();
            safeAwait(recoveryBarrier);

            // Isolating dataNode will cause shard to go unassigned
            NetworkDisruption disruption = new NetworkDisruption(
                new NetworkDisruption.TwoPartitions(Set.of(dataNode), Set.of(master)),
                NetworkDisruption.DISCONNECT
            );
            internalCluster().setDisruptionScheme(disruption);
            disruption.startDisrupting();
            String dataNodeId = internalCluster().clusterService(dataNode).localNode().getId();
            awaitClusterState(master, state -> state.nodes().nodeExists(dataNodeId) == false);

            // Release recovery will make recovery/retry race with network disruption
            safeAwait(recoveryBarrier);
            disruption.stopDisrupting();

            waitNoPendingTasksOnAll();
            ensureGreen(indexName);
        } finally {
            masterATransport.clearAllRules();
        }
    }

    public void testRetryOnFailureOnRecoveryFromLocalShardRaceWithNetworkDisruption() throws Exception {
        String master = internalCluster().startMasterOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        final var sourceIndexName = randomIndexName();
        final var targetIndexName = randomIndexName();

        createIndex(sourceIndexName, indexSettings(1, 0).build());
        indexDoc(sourceIndexName, "1", "f", randomAlphaOfLength(10));
        flush(sourceIndexName);
        ensureGreen(sourceIndexName);

        // Required for clone
        updateIndexSettings(Settings.builder().put("index.blocks.write", true), sourceIndexName);

        MockTransportService masterATransport = MockTransportService.getInstance(master);
        try {
            failTestIfReceiveShardFailure(masterATransport);

            armRandomFailure();
            final var recoveryBarrier = armRecoveryPause();

            // Recover from local shard async
            ResizeIndexTestUtils.executeResize(ResizeType.CLONE, sourceIndexName, targetIndexName, indexSettings(1, 0));
            safeAwait(recoveryBarrier);

            // Isolating dataNode will cause shard to go unassigned
            NetworkDisruption disruption = new NetworkDisruption(
                new NetworkDisruption.TwoPartitions(Set.of(dataNode), Set.of(master)),
                NetworkDisruption.DISCONNECT
            );
            internalCluster().setDisruptionScheme(disruption);
            disruption.startDisrupting();
            String dataNodeId = internalCluster().clusterService(dataNode).localNode().getId();
            awaitClusterState(master, state -> state.nodes().nodeExists(dataNodeId) == false);

            // Release recovery will make recovery/retry race with network disruption
            safeAwait(recoveryBarrier);
            disruption.stopDisrupting();

            waitNoPendingTasksOnAll();
            ensureGreen(sourceIndexName);
            ensureGreen(targetIndexName);
        } finally {
            masterATransport.clearAllRules();
        }
    }

    public void testRetryOnFailureOnRecoveryFromSnapshotRaceWithNetworkDisruption() throws Exception {
        String master = internalCluster().startMasterOnlyNode();
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

        MockTransportService masterATransport = MockTransportService.getInstance(master);
        try {
            failTestIfReceiveShardFailure(masterATransport);

            armRandomFailure();
            final var recoveryBarrier = armRecoveryPause();

            // Recover from snapshot async
            clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap").setWaitForCompletion(false).execute();
            safeAwait(recoveryBarrier);

            // Isolating dataNode will cause shard to go unassigned
            NetworkDisruption disruption = new NetworkDisruption(
                new NetworkDisruption.TwoPartitions(Set.of(dataNode), Set.of(master)),
                NetworkDisruption.DISCONNECT
            );
            internalCluster().setDisruptionScheme(disruption);
            disruption.startDisrupting();
            String dataNodeId = internalCluster().clusterService(dataNode).localNode().getId();
            awaitClusterState(master, state -> state.nodes().nodeExists(dataNodeId) == false);

            // Release recovery will make recovery/retry race with network disruption
            safeAwait(recoveryBarrier);
            disruption.stopDisrupting();

            waitNoPendingTasksOnAll();
            ensureGreen(indexName);
        } finally {
            masterATransport.clearAllRules();
        }
    }

    public void testClusterStateCreateWhileRetryContextAppliesLocalRetries() throws Exception {
        String master = internalCluster().startMasterOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        String indexName = randomIndexName();

        MockTransportService masterTransport = MockTransportService.getInstance(master);
        try {
            failTestIfReceiveShardFailure(masterTransport);

            failureTarget.set(BEFORE_INDEX_SHARD_RECOVERY);
            final var recoveryBarrier = armRecoveryPause();

            prepareCreate(indexName, indexSettings(1, 0)).execute();
            safeAwait(recoveryBarrier);
            ShardId shardId = new ShardId(resolveIndex(indexName), 0);

            // Hold the applier so RETRY schedules behind this IMMEDIATE blocker, then a HIGH CS apply
            // can recreate from the retry context before the NORMAL retry runs.
            var applier = internalCluster().getInstance(ClusterService.class, dataNode).getClusterApplierService();
            final var applierBarrier = new CyclicBarrier(2);
            applier.runOnApplierThread("block-applier", Priority.IMMEDIATE, clusterState -> {
                safeAwait(applierBarrier);
                safeAwait(applierBarrier);
            }, ActionListener.noop());
            safeAwait(applierBarrier);

            safeAwait(recoveryBarrier);
            assertBusy(
                () -> assertTrue(
                    "expected NORMAL retry-recovery task on data-node applier",
                    hasPending(applier, Priority.NORMAL, "retry recovery")
                )
            );

            client().execute(
                TransportClusterRerouteAction.TYPE,
                new ClusterRerouteRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT),
                ActionListener.noop()
            );
            assertBusy(
                () -> assertTrue(
                    "expected HIGH ApplyCommitRequest on data-node applier",
                    hasPending(applier, Priority.HIGH, "ApplyCommitRequest")
                )
            );

            CountDownLatch afterCs = new CountDownLatch(1);
            AtomicReference<AssertionError> afterCsFailure = new AtomicReference<>();
            applier.runOnApplierThread("assert-cs-applied-local-retries", Priority.HIGH, clusterState -> {
                try {
                    assertThat(
                        "cluster-state apply should have recreated the shard from the retry context",
                        recoveryCounter.get(),
                        equalTo(2)
                    );
                    IndexShard shard = internalCluster().getInstance(IndicesService.class, dataNode).getShardOrNull(shardId);
                    assertNotNull("cluster-state apply must create the shard while retry context carries localRetries", shard);
                    assertThat(shard.recoveryState().getLocalRetries(), equalTo(1));
                } catch (AssertionError e) {
                    afterCsFailure.set(e);
                } finally {
                    afterCs.countDown();
                }
            }, ActionListener.noop());

            // 1. HIGH CS apply (creates with localRetries=1)
            // 2. HIGH assert
            // 3. NORMAL retry (retry context already cleared / shard exists)
            safeAwait(applierBarrier);
            safeAwait(afterCs);
            if (afterCsFailure.get() != null) {
                throw afterCsFailure.get();
            }

            ensureGreen(indexName);
            assertLocalRetries(indexName, 1);
        } finally {
            masterTransport.clearAllRules();
        }
    }

    private static boolean hasPending(ClusterApplierService applier, Priority priority, String sourceSubstring) {
        for (var pending : applier.pendingTasks()) {
            if (pending.priority == priority && pending.executing == false && pending.task.toString().contains(sourceSubstring)) {
                return true;
            }
        }
        return false;
    }

    private void assertLocalRetries(String indexName, int expected) {
        final var recoveryInfos = indicesAdmin().prepareRecoveries(indexName).get().shardRecoveryInfos().get(indexName);
        assertThat(recoveryInfos, hasSize(1));
        assertThat(recoveryInfos.get(0).recoveryState().getLocalRetries(), equalTo(expected));
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

    /// One-shot pause in [IndexEventListener#beforeIndexShardRecovery]. Recovery takes the barrier
    /// with {@code getAndSet(null)} so a later retry does not pause again.
    private CyclicBarrier armRecoveryPause() {
        final var barrier = new CyclicBarrier(2);
        assertNull(recoveryBarrier.getAndSet(barrier));
        installRecoveryListener();
        return barrier;
    }

    /// Arm the recovery listener with a random failure target.
    /// This will cause the next recovery to fail with [RETRY_CAUSE] when it reaches that [FailureTarget].
    private void armRandomFailure() {
        failureTarget.set(randomFrom(FailureTarget.values()));
        installRecoveryListener();
    }

    private void installRecoveryListener() {
        for (var listener : internalCluster().getInstances(MockIndexEventListener.TestEventListener.class)) {
            listener.setNewDelegate(recoveryListener);
        }
    }

    /// Inject a one-shot [AlreadyClosedException] from the Lucene Directory during temporary IndexWriter use.
    public static class RetryRecoveryTestPlugin extends Plugin {
        private static final AtomicBoolean throwAceOnCreateOutput = new AtomicBoolean();

        public static void reset() {
            throwAceOnCreateOutput.set(false);
        }

        /// Arm the Directory wrapper so the next [IndexOutput] create throws [AlreadyClosedException].
        /// Used to fail temporary IndexWriters used in StoreRecovery once, then allow retry to succeed.
        public static void armDirectoryAce() {
            throwAceOnCreateOutput.set(true);
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.setDirectoryWrapper((directory, shardRouting) -> new FilterDirectory(directory) {
                @Override
                public IndexOutput createOutput(String name, IOContext context) throws IOException {
                    if (throwAceOnCreateOutput.compareAndSet(true, false)) {
                        throw new AlreadyClosedException("test createEmpty ACE");
                    }
                    return super.createOutput(name, context);
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
