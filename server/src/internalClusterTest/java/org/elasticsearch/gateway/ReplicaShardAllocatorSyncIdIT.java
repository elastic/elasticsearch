/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gateway;

import org.apache.lucene.index.IndexWriter;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

/**
 * A legacy version of {@link ReplicaShardAllocatorIT#testPreferCopyCanPerformNoopRecovery()} verifying
 * that the {@link ReplicaShardAllocator} prefers copies with matching sync_id.
 * TODO: Remove this test in 9.0
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ReplicaShardAllocatorSyncIdIT extends ESIntegTestCase {

    private static final AtomicBoolean allowFlush = new AtomicBoolean();

    private static class SyncedFlushEngine extends InternalEngine {
        private volatile IndexWriter indexWriter;

        SyncedFlushEngine(EngineConfig engineConfig) {
            super(engineConfig);
        }

        @Override
        protected void commitIndexWriter(IndexWriter writer, Translog translog) throws IOException {
            if (allowFlush.get() == false) {
                throw new AssertionError("flush is not allowed:" +
                    "global checkpoint [" + getLastSyncedGlobalCheckpoint() + "] " +
                    "last commit [" + getLastCommittedSegmentInfos().userData + "]");
            }
            indexWriter = writer;
            super.commitIndexWriter(writer, translog);
        }

        void syncFlush(String syncId) throws IOException {
            // make sure that we have committed translog; otherwise, we can flush after relaying translog in store recovery
            flush(true, true);
            // make sure that background merges won't happen; otherwise, IndexWriter#hasUncommittedChanges can become true again
            forceMerge(false, 1, false, false, false, UUIDs.randomBase64UUID());
            assertNotNull(indexWriter);
            try (ReleasableLock ignored = writeLock.acquire()) {
                assertThat(getTranslogStats().getUncommittedOperations(), equalTo(0));
                Map<String, String> userData = new HashMap<>(getLastCommittedSegmentInfos().userData);
                SequenceNumbers.CommitInfo commitInfo = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(userData.entrySet());
                assertThat(commitInfo.localCheckpoint, equalTo(getLastSyncedGlobalCheckpoint()));
                assertThat(commitInfo.maxSeqNo, equalTo(getLastSyncedGlobalCheckpoint()));
                userData.put(Engine.SYNC_COMMIT_ID, syncId);
                indexWriter.setLiveCommitData(userData.entrySet());
                indexWriter.commit();
            }
        }
    }

    public static class SyncedFlushPlugin  extends Plugin implements EnginePlugin {
        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return Optional.of(SyncedFlushEngine::new);
        }
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, InternalSettingsPlugin.class, SyncedFlushPlugin.class);
    }

    @Before
    public void allowFlush() {
        allowFlush.set(true);
    }

    private void syncFlush(String index) throws IOException {
        String syncId = randomAlphaOfLength(10);
        final Set<String> nodes = internalCluster().nodesInclude(index);
        for (String node : nodes) {
            IndexService indexService = internalCluster().getInstance(IndicesService.class, node).indexServiceSafe(resolveIndex(index));
            for (IndexShard indexShard : indexService) {
                SyncedFlushEngine engine = (SyncedFlushEngine) IndexShardTestCase.getEngine(indexShard);
                engine.syncFlush(syncId);
            }
        }
        // Once we have synced flushed, we do not allow regular flush as it will destroy the sync_id.
        allowFlush.set(false);
    }

    public void testPreferCopyCanPerformNoopRecovery() throws Exception {
        String indexName = "test";
        String nodeWithPrimary = internalCluster().startNode();
        assertAcked(
            client().admin().indices().prepareCreate(indexName)
                .setSettings(Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING.getKey(), "1ms") // expire PRRLs quickly
                    .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                    .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                    .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "1ms")));
        String nodeWithReplica = internalCluster().startDataOnlyNode();
        Settings nodeWithReplicaSettings = internalCluster().dataPathSettings(nodeWithReplica);
        ensureGreen(indexName);
        indexRandom(randomBoolean(), randomBoolean(), randomBoolean(), IntStream.range(0, between(100, 500))
            .mapToObj(n -> client().prepareIndex(indexName).setSource("f", "v")).collect(Collectors.toList()));
        if (randomBoolean()) {
            client().admin().indices().prepareFlush(indexName).get();
        }
        ensureGlobalCheckpointAdvancedAndSynced(indexName);
        syncFlush(indexName);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeWithReplica));
        // Wait until the peer recovery retention leases of the offline node are expired
        assertBusy(() -> {
            for (ShardStats shardStats : client().admin().indices().prepareStats(indexName).get().getShards()) {
                assertThat(shardStats.getRetentionLeaseStats().retentionLeases().leases(), hasSize(1));
            }
        });
        CountDownLatch blockRecovery = new CountDownLatch(1);
        CountDownLatch recoveryStarted = new CountDownLatch(1);
        MockTransportService transportServiceOnPrimary
            = (MockTransportService) internalCluster().getInstance(TransportService.class, nodeWithPrimary);
        transportServiceOnPrimary.addSendBehavior((connection, requestId, action, request, options) -> {
            if (PeerRecoveryTargetService.Actions.FILES_INFO.equals(action)) {
                recoveryStarted.countDown();
                try {
                    blockRecovery.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });
        internalCluster().startDataOnlyNode();
        recoveryStarted.await();
        nodeWithReplica = internalCluster().startDataOnlyNode(nodeWithReplicaSettings);
        // AllocationService only calls GatewayAllocator if there're unassigned shards
        assertAcked(client().admin().indices().prepareCreate("dummy-index").setWaitForActiveShards(0));
        ensureGreen(indexName);
        assertThat(internalCluster().nodesInclude(indexName), hasItem(nodeWithReplica));
        assertNoOpRecoveries(indexName);
        blockRecovery.countDown();
        transportServiceOnPrimary.clearAllRules();
    }

    public void testFullClusterRestartPerformNoopRecovery() throws Exception {
        int numOfReplicas = randomIntBetween(1, 2);
        internalCluster().ensureAtLeastNumDataNodes(numOfReplicas + 2);
        String indexName = "test";
        assertAcked(
            client().admin().indices().prepareCreate(indexName)
                .setSettings(Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numOfReplicas)
                    .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), randomIntBetween(10, 100) + "kb")
                    .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING.getKey(), "1ms") // expire PRRLs quickly
                    .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                    .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "100ms")));
        ensureGreen(indexName);
        indexRandom(randomBoolean(), randomBoolean(), randomBoolean(), IntStream.range(0, between(200, 500))
            .mapToObj(n -> client().prepareIndex(indexName).setSource("f", "v")).collect(Collectors.toList()));
        if (randomBoolean()) {
            client().admin().indices().prepareFlush(indexName).get();
        }
        ensureGlobalCheckpointAdvancedAndSynced(indexName);
        syncFlush(indexName);
        assertAcked(client().admin().cluster().prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put("cluster.routing.allocation.enable", "primaries").build()));
        internalCluster().fullRestart();
        ensureYellow(indexName);
        // Wait until the peer recovery retention leases of the offline node are expired
        assertBusy(() -> {
            for (ShardStats shardStats : client().admin().indices().prepareStats(indexName).get().getShards()) {
                assertThat(shardStats.getRetentionLeaseStats().retentionLeases().leases(), hasSize(1));
            }
        });
        assertAcked(client().admin().cluster().prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().putNull("cluster.routing.allocation.enable").build()));
        ensureGreen(indexName);
        assertNoOpRecoveries(indexName);
    }

    private void assertNoOpRecoveries(String indexName) {
        for (RecoveryState recovery : client().admin().indices().prepareRecoveries(indexName).get().shardRecoveryStates().get(indexName)) {
            if (recovery.getPrimary() == false) {
                assertThat(recovery.getIndex().fileDetails(), empty());
                assertThat(recovery.getTranslog().totalLocal(), equalTo(recovery.getTranslog().totalOperations()));
            }
        }
    }

    private void ensureGlobalCheckpointAdvancedAndSynced(String indexName) throws Exception {
        assertBusy(() -> {
            Index index = resolveIndex(indexName);
            for (String node : internalCluster().nodesInclude(indexName)) {
                IndexService indexService = internalCluster().getInstance(IndicesService.class, node).indexService(index);
                if (indexService != null) {
                    for (IndexShard shard : indexService) {
                        assertThat(shard.getLastSyncedGlobalCheckpoint(), equalTo(shard.seqNoStats().getMaxSeqNo()));
                    }
                }
            }
        });
    }
}
