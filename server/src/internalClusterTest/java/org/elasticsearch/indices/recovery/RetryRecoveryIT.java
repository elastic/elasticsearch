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
import org.elasticsearch.action.admin.indices.ResizeIndexTestUtils;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RecoveryFailureStrategySelectorPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.indices.recovery.FailureStrategy.RETRY;
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
        internalCluster().startNode();
        String indexName = randomIndexName();

        RetryRecoveryTestPlugin.armRandomFailure();

        // Recover from empty store
        createIndex(indexName, indexSettings(1, 0).build());

        ensureGreen(indexName);
        assertThat(RetryRecoveryTestPlugin.retryCounter.get(), equalTo(1));
        assertThat(RetryRecoveryTestPlugin.recoveryCounter.get(), equalTo(2));
    }

    public void testRetryOnFailureOnRecoveryFromExistingStore() {
        internalCluster().startNode();
        final var indexName = randomIndexName();

        createIndex(indexName, indexSettings(1, 0).build());
        indexDoc(indexName, "1", "f", randomAlphaOfLength(10));
        flush(indexName);
        ensureGreen(indexName);
        assertAcked(indicesAdmin().prepareClose(indexName));

        RetryRecoveryTestPlugin.reset();
        RetryRecoveryTestPlugin.armRandomFailure();

        // Recover from existing store
        assertAcked(indicesAdmin().prepareOpen(indexName).execute());

        ensureGreen(indexName);
        assertThat(RetryRecoveryTestPlugin.retryCounter.get(), equalTo(1));
        assertThat(RetryRecoveryTestPlugin.recoveryCounter.get(), equalTo(2));
    }

    public void testRetryOnFailureOnRecoveryFromLocalShard() {
        internalCluster().startNode();
        final var sourceIndexName = randomIndexName();
        final var targetIndexName = randomIndexName();

        createIndex(sourceIndexName, indexSettings(1, 0).build());
        indexDoc(sourceIndexName, "1", "f", randomAlphaOfLength(10));
        flush(sourceIndexName);
        ensureGreen(sourceIndexName);

        // Required for clone
        updateIndexSettings(Settings.builder().put("index.blocks.write", true), sourceIndexName);

        RetryRecoveryTestPlugin.reset();
        RetryRecoveryTestPlugin.armRandomFailure();

        // Recover from local shard
        ResizeIndexTestUtils.executeResize(ResizeType.CLONE, sourceIndexName, targetIndexName, indexSettings(1, 0));

        ensureGreen(sourceIndexName);
        ensureGreen(targetIndexName);
        assertThat(RetryRecoveryTestPlugin.retryCounter.get(), equalTo(1));
        assertThat(RetryRecoveryTestPlugin.recoveryCounter.get(), equalTo(2));
    }

    public void testRetryOnFailureOnRecoveryFromSnapshot() {
        internalCluster().startNode();
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

        RetryRecoveryTestPlugin.reset();
        RetryRecoveryTestPlugin.armRandomFailure();

        // Recover from snapshot
        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap").setWaitForCompletion(true).execute();

        ensureGreen(indexName);
        assertThat(RetryRecoveryTestPlugin.retryCounter.get(), equalTo(1));
        assertThat(RetryRecoveryTestPlugin.recoveryCounter.get(), equalTo(2));
    }

    public void testRetryOnFailureOnRecoveryFromPeer() {
        String source = internalCluster().startNode();
        final var indexName = randomIndexName();

        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.require._name", source).build());
        indexDoc(indexName, "1", "f", randomAlphaOfLength(10));
        flush(indexName);
        ensureGreen(indexName);

        String target = internalCluster().startNode();

        armRandomPeerRecoveryFailure(target);
        RetryRecoveryTestPlugin.reset();

        // Recover from peer
        indicesAdmin().prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put("index.routing.allocation.require._name", target))
            .execute();

        ensureGreen(indexName);
        assertAllShardsOnNodes(indexName, target);
        assertThat(RetryRecoveryTestPlugin.retryCounter.get(), equalTo(1));
        assertThat(RetryRecoveryTestPlugin.recoveryCounter.get(), equalTo(2));
    }

    public void testRetryOnFailureOnRecoveryFromEmptyStoreRaceWithIndexDeletion() throws Exception {
        internalCluster().startNode();
        String indexName = randomIndexName();

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
    }

    public void testRetryOnFailureOnRecoveryFromExistingStoreRaceWithIndexDeletion() throws Exception {
        internalCluster().startNode();
        String indexName = randomIndexName();

        createIndex(indexName, indexSettings(1, 0).build());
        indexDoc(indexName, "1", "f", randomAlphaOfLength(10));
        flush(indexName);
        ensureGreen(indexName);
        assertAcked(indicesAdmin().prepareClose(indexName));

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
    }

    public void testRetryOnFailureOnRecoveryFromLocalShardRaceWithIndexDeletion() throws Exception {
        internalCluster().startNode();
        final var sourceIndexName = randomIndexName();
        final var targetIndexName = randomIndexName();

        createIndex(sourceIndexName, indexSettings(1, 0).build());
        indexDoc(sourceIndexName, "1", "f", randomAlphaOfLength(10));
        flush(sourceIndexName);
        ensureGreen(sourceIndexName);

        // Required for clone
        updateIndexSettings(Settings.builder().put("index.blocks.write", true), sourceIndexName);

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
    }

    public void testRetryOnFailureOnRecoveryFromSnapshotRaceWithIndexDeletion() throws Exception {
        internalCluster().startNode();
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
    }

    public void testRetryOnFailureOnRecoveryFromPeerRaceWithIndexDeletion() throws Exception {
        String source = internalCluster().startNode();
        final var indexName = randomIndexName();

        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.require._name", source).build());
        indexDoc(indexName, "1", "f", randomAlphaOfLength(10));
        flush(indexName);
        ensureGreen(indexName);

        String target = internalCluster().startNode();
        armRandomPeerRecoveryFailure(target);
        Gate gate = randomFrom(RetryRecoveryTestPlugin.allGatesExcept(RetryRecoveryTestPlugin.stateChangePostRecoveryGate));
        gate.block();

        // Recover from peer async
        indicesAdmin().prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put("index.routing.allocation.require._name", target))
            .execute();
        gate.await();
        indicesAdmin().prepareDelete(indexName).execute();

        // Release recovery will make recovery/retry race with index deletion
        gate.release();

        waitNoPendingTasksOnAll();
        assertThat(indexExists(indexName), equalTo(false));
    }

    public void testRetryOnFailureOnRecoveryFromEmptyStoreRaceWithNetworkDisruption() throws Exception {
        String masterA = internalCluster().startMasterOnlyNode();
        String masterB = internalCluster().startMasterOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        String indexName = randomIndexName();

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
    }

    public void testRetryOnFailureOnRecoveryFromPeerRaceWithNetworkDisruption() throws Exception {
        String master = internalCluster().startMasterOnlyNode();
        String source = internalCluster().startNode();
        final var indexName = randomIndexName();

        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.require._name", source).build());
        indexDoc(indexName, "1", "f", randomAlphaOfLength(10));
        flush(indexName);
        ensureGreen(indexName);

        String target = internalCluster().startNode();

        armRandomPeerRecoveryFailure(target);
        Gate gate = randomFrom(RetryRecoveryTestPlugin.allGatesExcept(RetryRecoveryTestPlugin.stateChangePostRecoveryGate));
        gate.block();

        // Recover from peer async
        indicesAdmin().prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put("index.routing.allocation.require._name", target))
            .execute();
        gate.await();

        // Disrupt connection between target and master (source can talk to both)
        NetworkDisruption disruption = new NetworkDisruption(
            new NetworkDisruption.Bridge(source, Set.of(master), Set.of(target)),
            NetworkDisruption.DISCONNECT
        );
        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();
        String targetId = internalCluster().clusterService(target).localNode().getId();
        awaitClusterState(master, state -> state.nodes().nodeExists(targetId) == false);

        gate.release();
        disruption.stopDisrupting();

        waitNoPendingTasksOnAll();
        ensureGreen(indexName);
        assertAllShardsOnNodes(indexName, target);
    }

    /// Arm target transport service to fail when receiving a random peer recovery request from source.
    /// This will fail the next recovery attempt, resulting in the following source - target interaction:
    /// Target: send error response back to source on data channel
    /// Source: sends an error response back on the coordination channel (as response to start_recovery request)
    /// Target: trigger retry through RecoveryResponseHandler -> failRecovery(..., RETRY) --> listener.onRecoveryFailure(..., RETRY)
    private void armRandomPeerRecoveryFailure(String target) {
        AtomicInteger targetCounter = new AtomicInteger(0);
        final var targetTransport = MockTransportService.getInstance(target);
        String targetAction = randomFrom(
            PeerRecoveryTargetService.Actions.CLEAN_FILES,
            PeerRecoveryTargetService.Actions.FILE_CHUNK,
            PeerRecoveryTargetService.Actions.FILES_INFO,
            PeerRecoveryTargetService.Actions.FINALIZE,
            PeerRecoveryTargetService.Actions.PREPARE_TRANSLOG,
            PeerRecoveryTargetService.Actions.TRANSLOG_OPS,
            PeerRecoveryTargetService.Actions.HANDOFF_PRIMARY_CONTEXT
            // Excluding RESTORE_FILE_FROM_SNAPSHOT since we are not restoring from snapshot
        );
        targetTransport.addRequestHandlingBehavior(targetAction, (handler, request, channel, task) -> {
            if (targetCounter.incrementAndGet() == 1) {
                if (randomBoolean()) {
                    throw RETRY_CAUSE;
                } else {
                    channel.sendResponse(RETRY_CAUSE);
                    return;
                }
            }
            handler.messageReceived(request, channel, task);
        });
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
    /// - Count number of retry attempts [retryCounter]
    /// - Inject failures into recover path through [IndexEventListener] and [failureTarget] + [FailureTarget]
    /// - Concurrency control by injecting [Gate]s on shard creation/recovery path through [IndexEventListener]
    /// - Override retry semantics in production code by setting a custom [FailureStrategySelector] that retry if cause is [RETRY_CAUSE]
    public static class RetryRecoveryTestPlugin extends Plugin implements RecoveryFailureStrategySelectorPlugin {
        private static final AtomicReference<FailureTarget> failureTarget = new AtomicReference<>(null);
        private static final AtomicInteger recoveryCounter = new AtomicInteger();
        private static final AtomicInteger retryCounter = new AtomicInteger();

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
            retryCounter.set(0);
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
                case STATE_CHANGED_RECOVERING -> allGatesExcept(beforeIndexShardRecoveryGate, stateChangePostRecoveryGate);
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
                        maybeThrow(FailureTarget.BEFORE_INDEX_SHARD_RECOVERY);
                        listener.onResponse(null);
                    } finally {
                        beforeIndexShardRecoveryGate.exit();
                    }
                }

                @Override
                public void beforeIndexShardRecoveryRetry(ShardId shardId) {
                    // Called just before issuing retry attempt, under synchronized lock on IndicesClusterStateService
                    // Recovery attempt has already failed so no need to gate or throw here
                    retryCounter.incrementAndGet();
                }

                @Override
                public void afterIndexShardRecovery(IndexShard indexShard, ActionListener<Void> listener) {
                    maybeThrow(FailureTarget.AFTER_INDEX_SHARD_RECOVERY);
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
                        try {
                            maybeThrow(FailureTarget.STATE_CHANGED_RECOVERING);
                        } finally {
                            stateChangeRecoveringGate.exit();
                        }
                    }
                    if (currentState == IndexShardState.POST_RECOVERY) {
                        stateChangePostRecoveryGate.enter();
                        try {
                            maybeThrow(FailureTarget.STATE_CHANGED_POST_RECOVERY);
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

        @Override
        public FailureStrategySelector createFailureStrategySelector() {
            return (e, defaultStrategy) -> ExceptionsHelper.unwrapCausesAndSuppressed(e, t -> t.getMessage().contains(RETRY_MESSAGE))
                .isPresent() ? RETRY : defaultStrategy;
        }
    }

    /// Failure target describe different possible failure points during recovery.
    /// Typically on different calls to [IndexEventListener].
    enum FailureTarget {
        STATE_CHANGED_RECOVERING,
        BEFORE_INDEX_SHARD_RECOVERY,
        AFTER_INDEX_SHARD_RECOVERY,
        STATE_CHANGED_POST_RECOVERY
    }
}
