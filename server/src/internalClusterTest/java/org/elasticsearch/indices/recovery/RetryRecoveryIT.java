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
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.admin.indices.ResizeIndexTestUtils;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RecoveryFailureStrategySelectorPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.indices.recovery.FailureStrategy.RETRY;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
@TestLogging(reason = "test investigation", value = "org.elasticsearch.indices.recovery.ThrottlingRecoveryService:TRACE")
public class RetryRecoveryIT extends AbstractIndexRecoveryIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestRecoveryFailurePlugin.class);
        return plugins;
    }

    @After
    public void reset() {
        TestRecoveryFailurePlugin.reset();
    }

    public void testRetryOnFailureOnRecoveryFromEmptyStore() {
        internalCluster().startNode();
        String indexName = randomIndexName();

        // Index will fail in recovery on first attempt
        TestRecoveryFailurePlugin.armFailure();
        createIndex(indexName, indexSettings(1, 0).build());

        // Recovery should succeed, and we should have tried to recover twice
        ensureGreen(indexName);
        assertThat(TestRecoveryFailurePlugin.recoveryCounter.get(), equalTo(2));
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
        TestRecoveryFailurePlugin.reset();
        TestRecoveryFailurePlugin.armFailure();

        // Recover from existing store
        assertAcked(indicesAdmin().prepareOpen(indexName).execute());

        // Recovery should succeed, and we should have tried to recover twice
        ensureGreen(indexName);
        assertThat(TestRecoveryFailurePlugin.recoveryCounter.get(), equalTo(2));
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
        TestRecoveryFailurePlugin.reset();
        TestRecoveryFailurePlugin.armFailure();

        // Recover from local shard
        ResizeIndexTestUtils.executeResize(ResizeType.CLONE, sourceIndexName, targetIndexName, indexSettings(1, 0));

        // Recovery should succeed, and we should have tried to recover twice
        ensureGreen(targetIndexName);
        assertThat(TestRecoveryFailurePlugin.recoveryCounter.get(), equalTo(2));
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
        TestRecoveryFailurePlugin.reset();
        TestRecoveryFailurePlugin.armFailure();

        // Recover from snapshot
        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap").setWaitForCompletion(true).execute();

        // Recovery should succeed, and we should have tried to recover twice
        ensureGreen(indexName);
        assertThat(TestRecoveryFailurePlugin.recoveryCounter.get(), equalTo(2));
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
        TestRecoveryFailurePlugin.reset();
        TestRecoveryFailurePlugin.armFailure();

        // Recover from peer
        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, 0, source, target));

        // Recovery should succeed, and we should have tried to recover twice
        ensureGreen(indexName);
        assertThat(TestRecoveryFailurePlugin.recoveryCounter.get(), equalTo(2));
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

    public static class TestRecoveryFailurePlugin extends Plugin implements RecoveryFailureStrategySelectorPlugin {
        private static final AtomicBoolean failNextRecovery = new AtomicBoolean(false);
        private static final AtomicInteger recoveryCounter = new AtomicInteger();
        private static final RuntimeException RETRY_CAUSE = new RuntimeException("RETRY_CAUSE");

        public static void reset() {
            failNextRecovery.set(false);
            recoveryCounter.set(0);
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
                    if (failNextRecovery.getAndSet(false)) {
                        throw RETRY_CAUSE;
                    }
                    listener.onResponse(null);
                }
            });
        }

        @Override
        public FailureStrategySelector createFailureStrategySelector() {
            return (e, defaultStrategy) -> e.getCause() == RETRY_CAUSE ? RETRY : defaultStrategy;
        }
    }
}
