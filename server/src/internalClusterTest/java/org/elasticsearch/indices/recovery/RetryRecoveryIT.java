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
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.indices.recovery.FailureStrategy.RETRY;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
@TestLogging(
    reason = "test investigation",
    value = "org.elasticsearch.indices.recovery.ThrottlingRecoveryService:TRACE,"
        + "org.elasticsearch.indices.cluster.IndicesClusterStateService:TRACE,"
        + "org.elasticsearch.index.shard.IndexShard:TRACE"
)
public class RetryRecoveryIT extends AbstractIndexRecoveryIntegTestCase {

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
        RetryRecoveryTestPlugin.armFailure();
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
        RetryRecoveryTestPlugin.armFailure();

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
        RetryRecoveryTestPlugin.armFailure();

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
        RetryRecoveryTestPlugin.armFailure();

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
        RetryRecoveryTestPlugin.reset();
        RetryRecoveryTestPlugin.armFailure();

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

        // Block index on the next recovery
        RetryRecoveryTestPlugin.stateChangePostRecovery.block();

        // Index creation will block in recovery and then fail immediately when unblocked
        RetryRecoveryTestPlugin.armFailure();
        prepareCreate(indexName, indexSettings(1, 0)).execute();

        // Wait for index creation recovery attempt
        RetryRecoveryTestPlugin.stateChangePostRecovery.await();

        // Delete index asynchronously while shard is blocked in recovery
        logger.info("--> prepareDelete");
        indicesAdmin().prepareDelete(indexName).execute();

        // Release recovery will make retry mechanism race with index deletion
        RetryRecoveryTestPlugin.stateChangePostRecovery.release();

        // Let all tasks complete
        waitNoPendingTasksOnAll();

        // Index should not exist, and we should have reached retry step
        assertThat(indexExists(indexName), equalTo(false));
        assertThat(RetryRecoveryTestPlugin.retryCounter.get(), equalTo(1));
    }

    // Test matrix:
    // no concurrent change
    // x EMPTY_STORE
    // x EXISTING_STORE
    // x LOCAL_SHARDS
    // x SNAPSHOT
    // x PEER
    // concurrent with index deletion
    // - EMPTY_STORE
    // - EXISTING_STORE
    // - LOCAL_SHARDS
    // - SNAPSHOT
    // - PEER
    // shard deletion
    // - EMPTY_STORE
    // - EXISTING_STORE
    // - LOCAL_SHARDS
    // - SNAPSHOT
    // - PEER
    // RESHARD_SPLIT depends on x-pack plugin, so implement inside StatelessReshardIT

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

    public static class RetryRecoveryTestPlugin extends Plugin implements RecoveryFailureStrategySelectorPlugin {
        private static final AtomicBoolean failNextRecovery = new AtomicBoolean(false);
        private static final AtomicInteger recoveryCounter = new AtomicInteger();
        private static final AtomicInteger retryCounter = new AtomicInteger();
        private static final RuntimeException RETRY_CAUSE = new RuntimeException("RETRY_CAUSE");

        private static final Gate stateChangePostRecovery = new Gate();

        public static void reset() {
            failNextRecovery.set(false);
            recoveryCounter.set(0);
            retryCounter.set(0);
            stateChangePostRecovery.reset();
        }

        public static void armFailure() {
            failNextRecovery.set(true);
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addIndexEventListener(new IndexEventListener() {
                @Override
                public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings, ActionListener<Void> listener) {
                    recoveryCounter.incrementAndGet();
                    listener.onResponse(null);
                }

                @Override
                public void beforeIndexShardRecoveryRetry(ShardId shardId) {
                    retryCounter.incrementAndGet();
                }

                @Override
                public void indexShardStateChanged(IndexShard indexShard, IndexShardState previousState, IndexShardState currentState, String reason) {
                    if (currentState == IndexShardState.POST_RECOVERY) {
                        stateChangePostRecovery.enter();
                        try {
                            if (failNextRecovery.getAndSet(false)) {
                                throw RETRY_CAUSE;
                            }
                        } finally {
                            stateChangePostRecovery.exit();
                        }
                    }
                }
            });
        }

        @Override
        public FailureStrategySelector createFailureStrategySelector() {
            return (e, defaultStrategy) -> ExceptionsHelper.unwrapCausesAndSuppressed(e, t -> t == RETRY_CAUSE).isPresent()
                ? RETRY
                : defaultStrategy;
        }
    }
}
