/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.action.shard.ShardStateAction.SHARD_FAILED_ACTION_NAME;
import static org.elasticsearch.xpack.stateless.reshard.RetryReshardSplitRecoveryIT.FailureTarget.AFTER_INDEX_SHARD_RECOVERY;
import static org.elasticsearch.xpack.stateless.reshard.RetryReshardSplitRecoveryIT.FailureTarget.BEFORE_INDEX_SHARD_RECOVERY;
import static org.elasticsearch.xpack.stateless.reshard.RetryReshardSplitRecoveryIT.FailureTarget.STATE_CHANGED_POST_RECOVERY;
import static org.hamcrest.Matchers.equalTo;

/**
 * Local recovery retry coverage for {@code RESHARD_SPLIT} recoveries.
 * <p>
 * Reshard lives in the stateless plugin, so these cases cannot live next to
 * {@code RetryRecoveryIT} in server. The IndexEventListener failure-injection harness
 * mirrors that IT.
 */
public class RetryReshardSplitRecoveryIT extends AbstractStatelessPluginIntegTestCase {
    private static final String RETRY_MESSAGE = "RETRY_CAUSE";
    private static final RuntimeException RETRY_CAUSE = new RuntimeException(RETRY_MESSAGE);

    /** Target shard id created by a 1→2 reshard split. */
    private static final int SPLIT_TARGET_SHARD_ID = 1;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(RetryReshardSplitRecoveryTestPlugin.class);
        return plugins;
    }

    @After
    public void reset() {
        RetryReshardSplitRecoveryTestPlugin.reset();
    }

    public void testRetryOnFailureOnRecoveryFromReshardSplit() {
        String indexNode = startMasterAndIndexNode();
        ensureStableCluster(1);

        final String indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        MockTransportService transportService = MockTransportService.getInstance(indexNode);
        try {
            failTestIfReceiveShardFailure(transportService);

            RetryReshardSplitRecoveryTestPlugin.reset();
            RetryReshardSplitRecoveryTestPlugin.armFailure(BEFORE_INDEX_SHARD_RECOVERY);

            client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet(SAFE_AWAIT_TIMEOUT);
            ensureGreen(indexName);
            assertThat(RetryReshardSplitRecoveryTestPlugin.recoveryCounter.get(), equalTo(2));
        } finally {
            transportService.clearAllRules();
        }
    }

    public void testRetryOnFailureOnRecoveryFromReshardSplitRaceWithIndexDeletion() throws Exception {
        String indexNode = startMasterAndIndexNode();
        ensureStableCluster(1);

        final String indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        MockTransportService transportService = MockTransportService.getInstance(indexNode);
        try {
            failTestIfReceiveShardFailure(transportService);

            RetryReshardSplitRecoveryTestPlugin.reset();
            RetryReshardSplitRecoveryTestPlugin.armRandomFailure();
            Gate gate = RetryReshardSplitRecoveryTestPlugin.randomGateBeforeTargetFailure();
            gate.block();

            client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName));
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

    public void testRetryOnFailureOnRecoveryFromReshardSplitRaceWithNetworkDisruption() throws Exception {
        String master = startMasterOnlyNode();
        String indexNode = startIndexNode();

        final String indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        MockTransportService masterATransport = MockTransportService.getInstance(master);
        try {
            failTestIfReceiveShardFailure(masterATransport);

            RetryReshardSplitRecoveryTestPlugin.reset();
            RetryReshardSplitRecoveryTestPlugin.armRandomFailure();
            Gate gate = RetryReshardSplitRecoveryTestPlugin.randomGateBeforeTargetFailure();
            gate.block();

            client().execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName));
            gate.await();

            // Isolating indexNode will cause shards to go unassigned
            NetworkDisruption disruption = new NetworkDisruption(
                new NetworkDisruption.TwoPartitions(Set.of(indexNode), Set.of(master)),
                NetworkDisruption.DISCONNECT
            );
            internalCluster().setDisruptionScheme(disruption);
            disruption.startDisrupting();
            String indexNodeId = internalCluster().clusterService(indexNode).localNode().getId();
            awaitClusterState(master, state -> state.nodes().nodeExists(indexNodeId) == false);

            // Release recovery will make recovery/retry race with network disruption
            gate.release();
            disruption.stopDisrupting();

            waitNoPendingTasksOnAll();
            ensureGreen(indexName);
        } finally {
            masterATransport.clearAllRules();
        }
    }

    /// Local recovery retries avoid the round trip to master on a failed recovery.
    /// Master retries would mask bugs in the local retry path, so fail if shard-failed is sent.
    private static void failTestIfReceiveShardFailure(MockTransportService mockTransportService) {
        mockTransportService.addRequestHandlingBehavior(
            SHARD_FAILED_ACTION_NAME,
            (handler, request, channel, task) -> fail("should not send shard failure")
        );
    }

    /// Think of a Gate as a gate with a visitor and a guard.
    /// Visitor: {@code enter()} … work … {@code exit()}.
    /// Guard: {@code block()}, {@code await()} visitor, work, then {@code release()}.
    static class Gate {
        private final Semaphore gate = new Semaphore(1);
        private final Semaphore entered = new Semaphore(0);
        private final String name;

        Gate(String name) {
            this.name = name;
        }

        void reset() {
            gate.drainPermits();
            gate.release();
            entered.drainPermits();
        }

        void block() {
            safeAcquire(gate);
        }

        void await() {
            safeAcquire(entered);
            entered.release();
        }

        public void release() {
            gate.release();
        }

        void enter() {
            entered.release();
            safeAcquire(gate);
        }

        void exit() {
            gate.release();
            safeAcquire(entered);
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /// Counts recovery attempts, injects IndexEventListener failures, and gates recovery for races.
    /// Enables [IndicesClusterStateService#LOCAL_RECOVERY_RETRY].
    public static class RetryReshardSplitRecoveryTestPlugin extends Plugin {
        private static final AtomicReference<FailureTarget> failureTarget = new AtomicReference<>(null);
        private static final AtomicInteger recoveryCounter = new AtomicInteger();

        private static final Gate beforeIndexShardRecoveryGate = new Gate("beforeIndexShardRecoveryGate");
        private static final Gate stateChangeRecoveringGate = new Gate("stateChangeRecoveringGate");
        private static final Gate stateChangePostRecoveryGate = new Gate("stateChangePostRecoveryGate");
        private static final List<Gate> allGates = List.of(
            beforeIndexShardRecoveryGate,
            stateChangeRecoveringGate,
            stateChangePostRecoveryGate
        );

        public static void reset() {
            failureTarget.set(null);
            recoveryCounter.set(0);
            allGates.forEach(Gate::reset);
        }

        public static void armFailure(FailureTarget target) {
            failureTarget.set(target);
        }

        /// Arm index event listener with a random failure target.
        /// This will cause the next split-target recovery to fail with a [RETRY_CAUSE]
        /// exception when it reaches the [FailureTarget].
        public static void armRandomFailure() {
            failureTarget.set(randomFrom(FailureTarget.values()));
        }

        /// Returns a [Gate] that sits at some random point before the currently armed [FailureTarget].
        /// Used so recovery has started but not yet failed when we race against delete/disruption.
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
        public List<Setting<?>> getSettings() {
            // LOCAL_RECOVERY_RETRY is test-registered only (see IndicesClusterStateService)
            return List.of(IndicesClusterStateService.LOCAL_RECOVERY_RETRY);
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
                private boolean isSplitTarget(IndexShard indexShard) {
                    return indexShard.shardId().id() == SPLIT_TARGET_SHARD_ID;
                }

                @Override
                public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings, ActionListener<Void> listener) {
                    if (isSplitTarget(indexShard) == false) {
                        listener.onResponse(null);
                        return;
                    }
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
                    if (isSplitTarget(indexShard)) {
                        maybeThrow(AFTER_INDEX_SHARD_RECOVERY);
                    }
                    listener.onResponse(null);
                }

                @Override
                public void indexShardStateChanged(
                    IndexShard indexShard,
                    IndexShardState previousState,
                    IndexShardState currentState,
                    String reason
                ) {
                    if (isSplitTarget(indexShard) == false) {
                        return;
                    }
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

    enum FailureTarget {
        BEFORE_INDEX_SHARD_RECOVERY,
        AFTER_INDEX_SHARD_RECOVERY,
        STATE_CHANGED_POST_RECOVERY
    }
}
