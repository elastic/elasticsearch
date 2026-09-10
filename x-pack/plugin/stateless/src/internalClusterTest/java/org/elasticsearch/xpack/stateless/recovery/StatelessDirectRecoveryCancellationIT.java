/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.action.admin.indices.ResizeIndexTestUtils;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.cluster.action.shard.FailedShardEntry;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.CancelRecoveriesAction;
import org.elasticsearch.indices.recovery.CompositeRecoverySchedulingListener;
import org.elasticsearch.indices.recovery.RecoveryCancelledException;
import org.elasticsearch.indices.recovery.RecoveryMetricsCollector;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.ShardRecoveryCancellation;
import org.elasticsearch.indices.recovery.TestRecoverySchedulingListener;
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
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.stateless.recovery.TransportStatelessPrimaryRelocationAction.PRIMARY_CONTEXT_HANDOFF_ACTION_NAME;
import static org.hamcrest.Matchers.equalTo;

/// Mirrors [DirectRecoveryCancellationIT] tests for the stateless path
public class StatelessDirectRecoveryCancellationIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
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
        awaitNoRecoveriesInStatsAndMetrics();
    }

    public void testDirectCancellationOfEmptyStoreRecovery() throws Exception {
        final var node = startMasterAndIndexNode();
        final var indexName = randomIndexName();

        // Block the EMPTY_STORE recovery
        safeAcquire(TestRecoveryBlockerPlugin.beforeRecoveryGate);

        prepareCreate(indexName).setSettings(indexSettings(1, 0).build()).execute();

        safeAcquire(TestRecoveryBlockerPlugin.beforeRecoveryEntered);
        TestRecoveryBlockerPlugin.beforeRecoveryEntered.release();

        waitNoPendingTasksOnAll();

        final var index = resolveIndex(indexName);
        final var shardId = new ShardId(index, 0);
        final var indicesService = internalCluster().getInstance(IndicesService.class, node);
        final var shard = indicesService.indexServiceSafe(index).getShard(0);
        final var allocationId = shard.routingEntry().allocationId().getId();
        final var clusterService = internalCluster().getInstance(ClusterService.class, node);

        final var shardFailureReceived = shardCancelledFailureReceivedLatch(node, shardId);

        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().term(),
            clusterService.state().version(),
            List.of(new ShardRecoveryCancellation(shardId, allocationId, true))
        );

        client(node).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();
        TestRecoveryBlockerPlugin.beforeRecoveryGate.release();

        safeAwait(shardFailureReceived);
        assertThat(directCancellationMetric(node), equalTo(1L));
    }

    public void testDirectCancellationOfExistingStoreRecovery() throws Exception {
        final var node = startMasterAndIndexNode();
        final var indexName = randomIndexName();

        createIndex(indexName, indexSettings(1, 0).build());
        indexDocs(indexName, between(1, 50));
        flush(indexName);
        ensureGreen(indexName);

        assertAcked(indicesAdmin().prepareClose(indexName));

        // Block the EXISTING_STORE recovery
        safeAcquire(TestRecoveryBlockerPlugin.beforeRecoveryGate);

        indicesAdmin().prepareOpen(indexName).execute();

        safeAcquire(TestRecoveryBlockerPlugin.beforeRecoveryEntered);
        TestRecoveryBlockerPlugin.beforeRecoveryEntered.release();

        waitNoPendingTasksOnAll();

        final var index = resolveIndex(indexName);
        final var shardId = new ShardId(index, 0);
        final var indicesService = internalCluster().getInstance(IndicesService.class, node);
        final var shard = indicesService.indexServiceSafe(index).getShard(0);
        final var allocationId = shard.routingEntry().allocationId().getId();
        final var clusterService = internalCluster().getInstance(ClusterService.class, node);

        final var shardFailureReceived = shardCancelledFailureReceivedLatch(node, shardId);

        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().term(),
            clusterService.state().version(),
            List.of(new ShardRecoveryCancellation(shardId, allocationId, true))
        );
        client(node).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();
        TestRecoveryBlockerPlugin.beforeRecoveryGate.release();

        safeAwait(shardFailureReceived);
        assertThat(directCancellationMetric(node), equalTo(1L));
    }

    public void testDirectCancellationOfLocalShardsRecovery() throws Exception {
        final var node = startMasterAndIndexNode();
        final var sourceIndexName = randomIndexName();
        final var targetIndexName = randomIndexName();

        createIndex(sourceIndexName, indexSettings(1, 0).build());
        indexDocs(sourceIndexName, between(1, 50));
        flush(sourceIndexName);
        ensureGreen(sourceIndexName);

        // Required for clone, make the source index read-only
        updateIndexSettings(Settings.builder().put("index.blocks.write", true), sourceIndexName);

        // Block the LOCAL_SHARDS recovery
        safeAcquire(TestRecoveryBlockerPlugin.beforeRecoveryGate);

        ResizeIndexTestUtils.executeResize(ResizeType.CLONE, sourceIndexName, targetIndexName, indexSettings(1, 0));

        safeAcquire(TestRecoveryBlockerPlugin.beforeRecoveryEntered);
        TestRecoveryBlockerPlugin.beforeRecoveryEntered.release();

        waitNoPendingTasksOnAll();

        final var targetIndex = resolveIndex(targetIndexName);
        final var shardId = new ShardId(targetIndex, 0);
        final var indicesService = internalCluster().getInstance(IndicesService.class, node);
        final var shard = indicesService.indexServiceSafe(targetIndex).getShard(0);
        final var allocationId = shard.routingEntry().allocationId().getId();
        final var clusterService = internalCluster().getInstance(ClusterService.class, node);

        final var shardFailureReceived = shardCancelledFailureReceivedLatch(node, shardId);

        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().term(),
            clusterService.state().version(),
            List.of(new ShardRecoveryCancellation(shardId, allocationId, true))
        );
        client(node).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();
        TestRecoveryBlockerPlugin.beforeRecoveryGate.release();

        safeAwait(shardFailureReceived);
        assertThat(directCancellationMetric(node), equalTo(1L));
    }

    public void testDirectCancellationOfSnapshotRecoveryBeforeRestore() throws Exception {
        final var node = startMasterAndIndexNode();
        final var indexName = randomIndexName();
        final var repoName = randomRepoName();

        createIndex(indexName, indexSettings(1, 0).build());
        indexDocs(indexName, between(1, 50));
        flush(indexName);
        ensureGreen(indexName);

        final var snapshot = randomSnapshotName();
        createRepository(repoName, "fs");
        createSnapshot(repoName, snapshot, List.of(indexName), List.of());
        assertAcked(indicesAdmin().prepareDelete(indexName));

        // Block the SNAPSHOT recovery
        safeAcquire(TestRecoveryBlockerPlugin.beforeRecoveryGate);

        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshot).setWaitForCompletion(false).execute();

        safeAcquire(TestRecoveryBlockerPlugin.beforeRecoveryEntered);
        TestRecoveryBlockerPlugin.beforeRecoveryEntered.release();

        waitNoPendingTasksOnAll();

        final var index = resolveIndex(indexName);
        final var shardId = new ShardId(index, 0);
        final var indicesService = internalCluster().getInstance(IndicesService.class, node);
        final var shard = indicesService.indexServiceSafe(index).getShard(0);
        final var allocationId = shard.routingEntry().allocationId().getId();
        final var clusterService = internalCluster().getInstance(ClusterService.class, node);

        final var shardFailureReceived = shardCancelledFailureReceivedLatch(node, shardId);

        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().term(),
            clusterService.state().version(),
            List.of(new ShardRecoveryCancellation(shardId, allocationId, true))
        );
        client(node).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();
        TestRecoveryBlockerPlugin.beforeRecoveryGate.release();

        safeAwait(shardFailureReceived);
        assertThat(directCancellationMetric(node), equalTo(1L));

        // delete the index to speed up cleanup (avoids a big wait for the cluster to become green)
        assertAcked(indicesAdmin().prepareDelete(indexName));
    }

    public void testDirectCancellationOfSnapshotRecoveryDuringRestore() throws Exception {
        final var node = startMasterAndIndexNode();
        final var indexName = randomIdentifier();
        final var repoName = randomRepoName();

        createIndex(indexName, indexSettings(1, 0).build());
        indexDocs(indexName, between(1, 50));
        flush(indexName);
        ensureGreen(indexName);

        createRepository(logger, repoName, BlockingFsRepositoryPlugin.REPO_TYPE);
        final var snapshot = randomSnapshotName();
        createSnapshot(repoName, snapshot, List.of(indexName), List.of());
        assertAcked(indicesAdmin().prepareDelete(indexName));

        // Pause restore inside restoreShard
        safeAcquire(BlockingFsRepositoryPlugin.proceedWithRestore);
        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshot).setWaitForCompletion(false).execute();

        safeAcquire(BlockingFsRepositoryPlugin.restoreHasStarted);
        BlockingFsRepositoryPlugin.restoreHasStarted.release();

        waitNoPendingTasksOnAll();

        final var index = resolveIndex(indexName);
        final var shardId = new ShardId(index, 0);
        final var indicesService = internalCluster().getInstance(IndicesService.class, node);
        final var shard = indicesService.indexServiceSafe(index).getShard(0);
        final var allocationId = shard.routingEntry().allocationId().getId();
        final var clusterService = internalCluster().getInstance(ClusterService.class, node);

        final var shardFailureReceived = shardCancelledFailureReceivedLatch(node, shardId);

        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().term(),
            clusterService.state().version(),
            List.of(new ShardRecoveryCancellation(shardId, allocationId, true))
        );

        client(node).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();
        BlockingFsRepositoryPlugin.proceedWithRestore.release();

        safeAwait(shardFailureReceived);
        assertThat(directCancellationMetric(node), equalTo(1L));

        // delete the index to speed up cleanup (avoids a big wait for the cluster to become green)
        assertAcked(indicesAdmin().prepareDelete(indexName));
    }

    /// Tests [TransportStatelessPrimaryRelocationAction]
    public void testDirectCancellationOfPrimaryRelocation() {
        final var masterNode = startMasterOnlyNode();
        final var sourceNode = startIndexNode();

        final var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());
        indexDocs(indexName, between(1, 50));
        flush(indexName);
        ensureGreen(indexName);

        final var targetNode = startIndexNode();
        safeAcquire(TestRecoveryBlockerPlugin.afterShardCreatedGate);

        client().execute(
            TransportClusterRerouteAction.TYPE,
            new ClusterRerouteRequest(TEST_REQUEST_TIMEOUT, TimeValue.ZERO).add(
                new MoveAllocationCommand(indexName, 0, sourceNode, targetNode)
            )
        );

        safeAcquire(TestRecoveryBlockerPlugin.afterShardCreatedEntered);
        TestRecoveryBlockerPlugin.afterShardCreatedEntered.release();

        // Request cancellation directly on the shard object while still on the (blocked) cluster applier thread's
        // capture of it, rather than going through CancelRecoveriesAction, which is gonna be racy for this specific test.
        final var shard = TestRecoveryBlockerPlugin.latestCreatedIndexShard.get();
        final var shardId = shard.shardId();
        final var shardFailureReceived = shardCancelledFailureReceivedLatch(masterNode, shardId);
        shard.requestRecoveryCancellation();

        TestRecoveryBlockerPlugin.afterShardCreatedGate.release();

        safeAwait(shardFailureReceived);
        assertThat(directCancellationMetric(targetNode), equalTo(1L));
    }

    public void testDirectCancellationAtPrimaryHandoffGetsIgnored() throws Exception {
        startMasterOnlyNode();
        final var sourceNode = startIndexNode();

        final var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());
        indexDocs(indexName, between(1, 50));
        flush(indexName);
        ensureGreen(indexName);

        final var targetNode = startIndexNode();

        // Stall the primary context handoff on the target.
        final var blockedHandoff = new CountDownLatch(1);
        final var proceedWithHandoff = new CountDownLatch(1);
        MockTransportService.getInstance(targetNode)
            .addRequestHandlingBehavior(PRIMARY_CONTEXT_HANDOFF_ACTION_NAME, (handler, request, channel, task) -> {
                blockedHandoff.countDown();
                safeAwait(proceedWithHandoff);
                handler.messageReceived(request, channel, task);
            });

        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, 0, sourceNode, targetNode));
        safeAwait(blockedHandoff);

        waitNoPendingTasksOnAll();

        final var index = resolveIndex(indexName);
        final var shardId = new ShardId(index, 0);
        final var indicesService = internalCluster().getInstance(IndicesService.class, targetNode);
        final var shard = indicesService.indexServiceSafe(index).getShard(0);
        final var allocationId = shard.routingEntry().allocationId().getId();

        final var clusterService = internalCluster().getInstance(ClusterService.class, targetNode);
        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().term(),
            clusterService.state().version(),
            List.of(new ShardRecoveryCancellation(shardId, allocationId, true))
        );
        client(targetNode).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();

        proceedWithHandoff.countDown();
        ensureGreen(indexName);
        awaitClusterState(state -> {
            final var primaryShard = state.routingTable().shardRoutingTable(shardId).primaryShard();
            return primaryShard.started() && primaryShard.currentNodeId().equals(state.nodes().resolveNode(targetNode).getId());
        });

        assertThat(shard.state(), equalTo(IndexShardState.STARTED));
        shard.ensureRecoveryNotCancelled();
        assertThat(directCancellationMetric(targetNode), equalTo(0L));
    }

    /// Tests [TransportStatelessUnpromotableRelocationAction]
    public void testDirectCancellationOfSearchShardPeerRecovery() throws Exception {
        final var masterNode = startMasterOnlyNode();
        final var indexNode = startIndexNode();
        final var searchNode = startSearchNode();

        final var indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);
        indexDocs(indexName, between(1, 50));
        flush(indexName);

        final var blockedRegistration = new CountDownLatch(1);
        final var proceedWithRegistration = new CountDownLatch(1);
        MockTransportService.getInstance(indexNode)
            .addRequestHandlingBehavior(TransportRegisterCommitForRecoveryAction.NAME, (handler, request, channel, task) -> {
                blockedRegistration.countDown();
                safeAwait(proceedWithRegistration);
                handler.messageReceived(request, channel, task);
            });

        setReplicaCount(1, indexName);
        safeAwait(blockedRegistration);

        waitNoPendingTasksOnAll();

        final var index = resolveIndex(indexName);
        final var shardId = new ShardId(index, 0);
        final var indicesService = internalCluster().getInstance(IndicesService.class, searchNode);
        final var shard = indicesService.indexServiceSafe(index).getShard(0);
        assertThat(shard.state(), equalTo(IndexShardState.RECOVERING));
        final var allocationId = shard.routingEntry().allocationId().getId();

        final var shardFailureReceived = shardCancelledFailureReceivedLatch(masterNode, shardId);

        final var clusterService = internalCluster().getInstance(ClusterService.class, searchNode);
        final var cancellationRequest = new CancelRecoveriesAction.Request(
            clusterService.state().term(),
            clusterService.state().version(),
            List.of(new ShardRecoveryCancellation(shardId, allocationId, true))
        );
        client(searchNode).execute(CancelRecoveriesAction.TYPE, cancellationRequest).get();
        proceedWithRegistration.countDown();

        safeAwait(shardFailureReceived);
        assertThat(directCancellationMetric(searchNode), equalTo(1L));
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

    private static CountDownLatch shardCancelledFailureReceivedLatch(String node, ShardId shardId) {
        final var shardFailureReceivedLatch = new CountDownLatch(1);
        MockTransportService.getInstance(node)
            .addRequestHandlingBehavior(ShardStateAction.SHARD_FAILED_ACTION_NAME, (handler, request, channel, task) -> {
                if (request instanceof FailedShardEntry failedShard) {
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

        static final Semaphore beforeShardCreatedGate = new Semaphore(1);
        static final Semaphore beforeShardCreatedEntered = new Semaphore(0);
        static final AtomicReference<ShardRouting> latestShardRouting = new AtomicReference<>();

        static final Semaphore afterShardCreatedGate = new Semaphore(1);
        static final Semaphore afterShardCreatedEntered = new Semaphore(0);
        static final AtomicReference<IndexShard> latestCreatedIndexShard = new AtomicReference<>();

        static void reset() {
            beforeRecoveryGate.drainPermits();
            beforeRecoveryGate.release();
            beforeRecoveryEntered.drainPermits();
            beforeShardCreatedGate.drainPermits();
            beforeShardCreatedGate.release();
            beforeShardCreatedEntered.drainPermits();
            latestShardRouting.set(null);
            afterShardCreatedGate.drainPermits();
            afterShardCreatedGate.release();
            afterShardCreatedEntered.drainPermits();
            latestCreatedIndexShard.set(null);
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addIndexEventListener(new IndexEventListener() {
                @Override
                public void beforeIndexShardCreated(ShardRouting routing, Settings indexSettings) {
                    latestShardRouting.set(routing);
                    beforeShardCreatedEntered.release();
                    safeAcquire(beforeShardCreatedGate);
                    beforeShardCreatedGate.release();
                    safeAcquire(beforeShardCreatedEntered);
                }

                @Override
                public void afterIndexShardCreated(IndexShard indexShard) {
                    // Only block unsearchable-primary PEER recoveries (i.e. stateless primary relocations), so that
                    // this doesn't race with unrelated shard creations elsewhere in the cluster (e.g. other indices).
                    final var shardRouting = indexShard.routingEntry();
                    if (shardRouting.recoverySource().getType() != RecoverySource.Type.PEER
                        || shardRouting.primary() == false
                        || shardRouting.isSearchable()) {
                        return;
                    }
                    latestCreatedIndexShard.set(indexShard);
                    afterShardCreatedEntered.release();
                    safeAcquire(afterShardCreatedGate);
                    afterShardCreatedGate.release();
                    safeAcquire(afterShardCreatedEntered);
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
            });
        }
    }

    private void awaitNoRecoveriesInStatsAndMetrics() {
        final var nodes = internalCluster().getNodeNames();
        final var telemetries = Arrays.stream(nodes)
            .collect(
                Collectors.toMap(
                    node -> node,
                    node -> internalCluster().getInstance(PluginsService.class, node)
                        .filterPlugins(TestTelemetryPlugin.class)
                        .findFirst()
                        .orElseThrow()
                )
            );
        final var recoveryCountMetrics = Set.of(
            RecoveryMetricsCollector.QUEUED_STORE_RECOVERIES,
            RecoveryMetricsCollector.CURRENT_STORE_RECOVERIES,
            RecoveryMetricsCollector.QUEUED_PEER_RECOVERIES_AS_SOURCE,
            RecoveryMetricsCollector.CURRENT_PEER_RECOVERIES_AS_SOURCE,
            RecoveryMetricsCollector.QUEUED_PEER_RECOVERIES_AS_TARGET,
            RecoveryMetricsCollector.CURRENT_PEER_RECOVERIES_AS_TARGET
        );

        final CountDownLatch conditionLatch = new CountDownLatch(1);
        final AtomicBoolean success = new AtomicBoolean();

        final Map<String, IndicesService> indicesServices = new ConcurrentHashMap<>();
        final Map<String, CompositeRecoverySchedulingListener> schedulingListeners = new ConcurrentHashMap<>();
        for (String nodeName : nodes) {
            indicesServices.put(nodeName, internalCluster().getInstance(IndicesService.class, nodeName));
            schedulingListeners.put(nodeName, internalCluster().getInstance(CompositeRecoverySchedulingListener.class, nodeName));
        }

        final var listener = new TestRecoverySchedulingListener() {
            @Override
            public void onRecoverySchedulingChange() {
                if (success.get()) {
                    return;
                }
                // Check no more recoveries in stats or metrics
                for (final var node : nodes) {
                    final var indicesService = indicesServices.get(node);

                    final var stats = new RecoveryStats();
                    for (IndexService indexService : indicesService) {
                        for (IndexShard shard : indexService) {
                            stats.add(shard.recoveryStats());
                        }
                    }
                    if (stats.noCurrentRecoveries() == false) {
                        return;
                    }
                    final var telemetry = telemetries.get(node);
                    for (final var metric : recoveryCountMetrics) {
                        if (telemetry.getLongUpDownCounterMeasurement(metric).stream().mapToLong(Measurement::getLong).sum() != 0L) {
                            return;
                        }
                    }
                }
                conditionLatch.countDown();
                success.set(true);
            }
        };
        for (final var node : nodes) {
            schedulingListeners.get(node).addListener(listener);
        }
        try {
            // In case conditions were already met before we added the listener everywhere
            listener.onRecoverySchedulingChange();
            safeAwait(conditionLatch);
        } finally {
            for (final var nodeName : nodes) {
                schedulingListeners.get(nodeName).removeListener(listener);
            }
        }
    }
}
