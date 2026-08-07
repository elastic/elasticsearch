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
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.action.admin.indices.ResizeIndexTestUtils;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
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
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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

        // Index will fail in recovery on first attempt
        RetryRecoveryTestPlugin.armRandomFailure();
        createIndex(indexName, indexSettings(1, 0).build());

        // Recovery should succeed, and we should have retried once
        ensureGreen(indexName);
        assertThat(RetryRecoveryTestPlugin.retryCounter.get(), equalTo(1));
        assertThat(RetryRecoveryTestPlugin.recoveryCounter.get(), equalTo(2));
    }

    public void testRetryOnFailureOnRecoveryFromExistingStore() {
        internalCluster().startNode();
        final var indexName = randomIndexName();

        // Create an existing store
        createIndex(indexName, indexSettings(1, 0).build());
        indexDoc(indexName, "1", "f", randomAlphaOfLength(10));
        flush(indexName);
        ensureGreen(indexName);
        assertAcked(indicesAdmin().prepareClose(indexName));

        // Fail next recovery attempt
        RetryRecoveryTestPlugin.reset();
        RetryRecoveryTestPlugin.armRandomFailure();

        // Recover from existing store
        assertAcked(indicesAdmin().prepareOpen(indexName).execute());

        // Recovery should succeed, and we should have retried once
        ensureGreen(indexName);
        assertThat(RetryRecoveryTestPlugin.retryCounter.get(), equalTo(1));
        assertThat(RetryRecoveryTestPlugin.recoveryCounter.get(), equalTo(2));
    }

    public void testRetryOnFailureOnRecoveryFromLocalShard() {
        internalCluster().startNode();
        final var sourceIndexName = randomIndexName();
        final var targetIndexName = randomIndexName();

        // Create an existing store
        createIndex(sourceIndexName, indexSettings(1, 0).build());
        indexDoc(sourceIndexName, "1", "f", randomAlphaOfLength(10));
        flush(sourceIndexName);
        ensureGreen(sourceIndexName);

        // Required for clone, make the source index read-only
        updateIndexSettings(Settings.builder().put("index.blocks.write", true), sourceIndexName);

        // Fail next recovery attempt
        RetryRecoveryTestPlugin.reset();
        RetryRecoveryTestPlugin.armRandomFailure();

        // Recover from local shard
        ResizeIndexTestUtils.executeResize(ResizeType.CLONE, sourceIndexName, targetIndexName, indexSettings(1, 0));

        // Recovery should succeed, and we should have retried once
        ensureGreen(targetIndexName);
        assertThat(RetryRecoveryTestPlugin.retryCounter.get(), equalTo(1));
        assertThat(RetryRecoveryTestPlugin.recoveryCounter.get(), equalTo(2));
    }

    public void testRetryOnFailureOnRecoveryFromSnapshot() {
        internalCluster().startNode();
        final var indexName = randomIndexName();
        final var repoName = "test-repo";

        // Create index to snapshot
        createIndex(indexName, indexSettings(1, 0).build());
        indexDoc(indexName, "1", "f", randomAlphaOfLength(10));
        flush(indexName);
        ensureGreen(indexName);

        // Snapshot the index
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName)
                .setType("fs")
                .setSettings(Settings.builder().put("location", randomRepoPath()))
        );
        clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap").setWaitForCompletion(true).get();

        // Delete the index
        assertAcked(indicesAdmin().prepareDelete(indexName));

        // Fail next recovery attempt
        RetryRecoveryTestPlugin.reset();
        RetryRecoveryTestPlugin.armRandomFailure();

        // Recover from snapshot
        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap").setWaitForCompletion(true).execute();

        // Recovery should succeed, and we should have retried once
        ensureGreen(indexName);
        assertThat(RetryRecoveryTestPlugin.retryCounter.get(), equalTo(1));
        assertThat(RetryRecoveryTestPlugin.recoveryCounter.get(), equalTo(2));
    }

    public void testRetryOnFailureOnRecoveryFromPeer() {
        String source = internalCluster().startNode();
        final var indexName = randomIndexName();

        // Create index on source
        createIndex(indexName, indexSettings(1, 0).build());
        indexDoc(indexName, "1", "f", randomAlphaOfLength(10));
        flush(indexName);
        ensureGreen(indexName);

        // Start target node
        String target = internalCluster().startNode();

        // Fail next recovery attempt
        // Target send error response back to source on data channel
        // Source sends an error response back on the coordination channel (as response to start_recovery request)
        // Target -> RecoveryResponseHandler -> failRecovery(..., RETRY) --> listener.onRecoveryFailure(..., RETRY)
        armRandomPeerRecoveryFailure(target);

        RetryRecoveryTestPlugin.reset();

        // Recover from peer
        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, 0, source, target));

        // Recovery should succeed, and we should have retried once
        ensureGreen(indexName);
        assertThat(RetryRecoveryTestPlugin.retryCounter.get(), equalTo(1));
        assertThat(RetryRecoveryTestPlugin.recoveryCounter.get(), equalTo(2));
    }

    public void testRetryOnFailureOnRecoveryFromEmptyStoreRaceWithIndexDeletion() throws Exception {
        internalCluster().startNode();
        String indexName = randomIndexName();

        // Index creation will block at some point during creation/recovery and then fail when released
        RetryRecoveryTestPlugin.armRandomFailure();
        Gate gate = RetryRecoveryTestPlugin.randomGateBeforeTargetFailure();
        gate.block();

        // Create index async
        prepareCreate(indexName, indexSettings(1, 0)).execute();

        // Wait for index creation/recovery attempt
        gate.await();

        // Delete index asynchronously
        indicesAdmin().prepareDelete(indexName).execute();

        // Release will make recovery and retry race with index deletion
        gate.release();

        // Let all tasks complete
        waitNoPendingTasksOnAll();

        // Index should not exist
        assertThat(indexExists(indexName), equalTo(false));
    }

    public void testRetryOnFailureOnRecoveryFromExistingStoreRaceWithIndexDeletion() throws Exception {
        internalCluster().startNode();
        String indexName = randomIndexName();

        // Create an existing store
        createIndex(indexName, indexSettings(1, 0).build());
        indexDoc(indexName, "1", "f", randomAlphaOfLength(10));
        flush(indexName);
        ensureGreen(indexName);
        assertAcked(indicesAdmin().prepareClose(indexName));

        // Open index will block at some point during creation/recovery and then fail when released
        RetryRecoveryTestPlugin.reset();
        RetryRecoveryTestPlugin.armRandomFailure();
        Gate gate = RetryRecoveryTestPlugin.randomGateBeforeTargetFailure();
        gate.block();

        // Recover from existing store async
        indicesAdmin().prepareOpen(indexName).execute();

        // Wait for index creation recovery attempt
        gate.await();

        // Delete index asynchronously
        indicesAdmin().prepareDelete(indexName).execute();

        // Release recovery will make retry mechanism race with index deletion
        gate.release();

        // Let all tasks complete
        waitNoPendingTasksOnAll();

        // Index should not exist
        assertThat(indexExists(indexName), equalTo(false));
    }

    public void testRetryOnFailureOnRecoveryFromLocalShardRaceWithIndexDeletion() throws Exception {
        internalCluster().startNode();
        final var sourceIndexName = randomIndexName();
        final var targetIndexName = randomIndexName();

        // Create an existing store
        createIndex(sourceIndexName, indexSettings(1, 0).build());
        indexDoc(sourceIndexName, "1", "f", randomAlphaOfLength(10));
        flush(sourceIndexName);
        ensureGreen(sourceIndexName);

        // Required for clone, make the source index read-only
        updateIndexSettings(Settings.builder().put("index.blocks.write", true), sourceIndexName);

        // Cloning index will block at some point during creation/recovery and then fail when released
        RetryRecoveryTestPlugin.armRandomFailure();
        Gate gate = RetryRecoveryTestPlugin.randomGateBeforeTargetFailure();
        gate.block();

        // Recover from local shard async
        ResizeIndexTestUtils.executeResize(ResizeType.CLONE, sourceIndexName, targetIndexName, indexSettings(1, 0));

        // Wait for index creation recovery attempt
        gate.await();

        // Delete index asynchronously
        indicesAdmin().prepareDelete(targetIndexName).execute();

        // Release recovery will make retry mechanism race with index deletion
        gate.release();

        // Let all tasks complete
        waitNoPendingTasksOnAll();

        // Index should not exist
        assertThat(indexExists(targetIndexName), equalTo(false));
    }

    public void testRetryOnFailureOnRecoveryFromSnapshotRaceWithIndexDeletion() throws Exception {
        internalCluster().startNode();
        final var indexName = randomIndexName();
        final var repoName = "test-repo";

        // Create index to snapshot
        createIndex(indexName, indexSettings(1, 0).build());
        indexDoc(indexName, "1", "f", randomAlphaOfLength(10));
        flush(indexName);
        ensureGreen(indexName);

        // Snapshot the index
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName)
                .setType("fs")
                .setSettings(Settings.builder().put("location", randomRepoPath()))
        );
        clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap").setWaitForCompletion(true).get();

        // Delete the index
        assertAcked(indicesAdmin().prepareDelete(indexName));

        // Index recovering from snapshot will block at some point during creation/recovery and then fail when released
        RetryRecoveryTestPlugin.reset();
        RetryRecoveryTestPlugin.armRandomFailure();
        Gate gate = RetryRecoveryTestPlugin.randomGateBeforeTargetFailure();
        gate.block();

        // Recover from snapshot async
        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap").setWaitForCompletion(false).execute();

        // Wait for index creation recovery attempt
        gate.await();

        // Delete index asynchronously
        indicesAdmin().prepareDelete(indexName).execute();

        // Release recovery will make retry mechanism race with index deletion
        gate.release();

        // Let all tasks complete
        waitNoPendingTasksOnAll();

        // Index should not exist
        assertThat(indexExists(indexName), equalTo(false));
    }

    public void testRetryOnFailureOnRecoveryFromPeerRaceWithIndexDeletion() throws Exception {
        String source = internalCluster().startNode();
        final var indexName = randomIndexName();

        // Create index on source
        createIndex(indexName, indexSettings(1, 0).build());
        indexDoc(indexName, "1", "f", randomAlphaOfLength(10));
        flush(indexName);
        ensureGreen(indexName);

        // Start target node
        String target = internalCluster().startNode();

        // Fail next recovery attempt
        // Target send error response back to source on data channel
        // Source sends an error response back on the coordination channel (as response to start_recovery request)
        // Target -> RecoveryResponseHandler -> failRecovery(..., RETRY) --> listener.onRecoveryFailure(..., RETRY)
        armRandomPeerRecoveryFailure(target);
        Gate gate = randomFrom(RetryRecoveryTestPlugin.allGatesExcept(RetryRecoveryTestPlugin.stateChangePostRecoveryGate));

        // Index recovering from peer will block at some point during creation/recovery and then fail when released
        gate.block();

        // Recover from peer async
        indicesAdmin().execute(
            TransportClusterRerouteAction.TYPE,
            new ClusterRerouteRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).setRetryFailed(false)
                .add(new MoveAllocationCommand(indexName, 0, source, target))
        );

        // Wait for peer recovery attempt
        gate.await();

        // Delete index asynchronously
        indicesAdmin().prepareDelete(indexName).execute();

        // Release recovery will make retry mechanism race with index deletion
        gate.release();

        // Let all tasks complete
        waitNoPendingTasksOnAll();

        // Index should not exist, and we should have reached retry step
        assertThat(indexExists(indexName), equalTo(false));
    }

    private void armRandomPeerRecoveryFailure(String target) {
        AtomicInteger targetCounter = new AtomicInteger(0);
        final var targetTransport = MockTransportService.getInstance(target);
        String targetOperation = randomFrom(
            PeerRecoveryTargetService.Actions.CLEAN_FILES,
            PeerRecoveryTargetService.Actions.FILE_CHUNK,
            PeerRecoveryTargetService.Actions.FILES_INFO,
            PeerRecoveryTargetService.Actions.FINALIZE,
            PeerRecoveryTargetService.Actions.PREPARE_TRANSLOG,
            PeerRecoveryTargetService.Actions.TRANSLOG_OPS,
            PeerRecoveryTargetService.Actions.HANDOFF_PRIMARY_CONTEXT
            // Excluding RESTORE_FILE_FROM_SNAPSHOT since we are not restoring from snapshot
        );
        targetTransport.addRequestHandlingBehavior(targetOperation, (handler, request, channel, task) -> {
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

    // Test matrix:
    // concurrent with index deletion
    // X EMPTY_STORE
    // X EXISTING_STORE
    // X LOCAL_SHARDS
    // X SNAPSHOT
    // X PEER
    // X currentRouting == null -> shard no longer assigned to node, e.g. index delete, allocation moved
    // - currentRouting.isSameAllocation(retryRouting) == false -> master unassigned then assigned back to same node
    // - currentRouting.initializing() == false -> see comment in IndexShard.updateShardState
    // - failedShardsCache.containsKey(shardId) ->
    // shard deletion
    // - EMPTY_STORE
    // - EXISTING_STORE
    // - LOCAL_SHARDS
    // - SNAPSHOT
    // - PEER
    // new allocation can't grab shard lock (shard moved away and then back while lock is held)
    // (note that this depends on createShardWhenLockAvailable use the same retry mechanism that rest of recovery does)
    // RESHARD_SPLIT depends on x-pack plugin, so implement inside StatelessReshardIT

    /// Think of a Gate as... well, a gate with a visitor and a guard.
    /// The visitor tries to [enter] the gate and when it leaves, [exit] the gate.
    /// The guard might prevent the visitor from entering by [block] the gate, then [await] for visitor to try to [enter],
    /// and finally [release] to let the visitor out again.
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
    /// // Do stuff while visitor is waiting
    /// gate.release();
    /// ```
    static class Gate {
        private final Semaphore gate = new Semaphore(1);
        private final Semaphore entered = new Semaphore(0);

        void reset() {
            gate.drainPermits();
            gate.release();
            entered.drainPermits();
        }

        /// Block someone from entering
        void block() {
            safeAcquire(gate);
        }

        /// Release block from someone entering
        public void release() {
            gate.release();
        }

        /// Wait for someone to enter()
        void await() {
            safeAcquire(entered);
            entered.release();
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
        private static final Gate beforeIndexShardCreatedGate = new Gate();
        private static final Gate onStoreCreatedGate = new Gate();
        private static final Gate afterIndexShardCreatedGate = new Gate();
        private static final Gate stateChangeRecoveringGate = new Gate();
        private static final Gate beforeIndexShardRecoveryGate = new Gate();
        private static final Gate stateChangePostRecoveryGate = new Gate();
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

        /// We will arm index event listener with a random
        public static void armRandomFailure() {
            failureTarget.set(randomFrom(FailureTarget.values()));
        }

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
            List<Gate> result = new ArrayList(allGates);
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

    // In the order they are invoked
    enum FailureTarget {
        STATE_CHANGED_RECOVERING,
        BEFORE_INDEX_SHARD_RECOVERY,
        AFTER_INDEX_SHARD_RECOVERY,
        STATE_CHANGED_POST_RECOVERY
    }
}
