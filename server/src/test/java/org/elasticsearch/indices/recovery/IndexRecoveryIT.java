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

package org.elasticsearch.indices.recovery;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.PeerRecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.StoreRecoverySource;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState.Stage;
import org.elasticsearch.node.RecoverySettingsChunkSizePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.store.MockFSDirectoryService;
import org.elasticsearch.test.store.MockFSIndexStore;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.elasticsearch.node.RecoverySettingsChunkSizePlugin.CHUNK_SIZE_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class IndexRecoveryIT extends ESIntegTestCase {

    private static final String INDEX_NAME = "test-idx-1";
    private static final String INDEX_TYPE = "test-type-1";
    private static final String REPO_NAME = "test-repo-1";
    private static final String SNAP_NAME = "test-snap-1";

    private static final int MIN_DOC_COUNT = 500;
    private static final int MAX_DOC_COUNT = 1000;
    private static final int SHARD_COUNT = 1;
    private static final int REPLICA_COUNT = 0;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, MockFSIndexStore.TestPlugin.class,
                RecoverySettingsChunkSizePlugin.class);
    }

    private void assertRecoveryStateWithoutStage(RecoveryState state, int shardId, RecoverySource recoverySource, boolean primary,
                                                 String sourceNode, String targetNode) {
        assertThat(state.getShardId().getId(), equalTo(shardId));
        assertThat(state.getRecoverySource(), equalTo(recoverySource));
        assertThat(state.getPrimary(), equalTo(primary));
        if (sourceNode == null) {
            assertNull(state.getSourceNode());
        } else {
            assertNotNull(state.getSourceNode());
            assertThat(state.getSourceNode().getName(), equalTo(sourceNode));
        }
        if (targetNode == null) {
            assertNull(state.getTargetNode());
        } else {
            assertNotNull(state.getTargetNode());
            assertThat(state.getTargetNode().getName(), equalTo(targetNode));
        }
    }

    private void assertRecoveryState(RecoveryState state, int shardId, RecoverySource type, boolean primary, Stage stage,
                                     String sourceNode, String targetNode) {
        assertRecoveryStateWithoutStage(state, shardId, type, primary, sourceNode, targetNode);
        assertThat(state.getStage(), equalTo(stage));
    }

    private void assertOnGoingRecoveryState(RecoveryState state, int shardId, RecoverySource type, boolean primary,
                                            String sourceNode, String targetNode) {
        assertRecoveryStateWithoutStage(state, shardId, type, primary, sourceNode, targetNode);
        assertThat(state.getStage(), not(equalTo(Stage.DONE)));
    }

    private void slowDownRecovery(ByteSizeValue shardSize) {
        long chunkSize = Math.max(1, shardSize.getBytes() / 10);
        assertTrue(client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder()
                                // one chunk per sec..
                                .put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), chunkSize, ByteSizeUnit.BYTES)
                                // small chunks
                                .put(CHUNK_SIZE_SETTING.getKey(), new ByteSizeValue(chunkSize, ByteSizeUnit.BYTES))
                ).get().isAcknowledged());
    }

    private void restoreRecoverySpeed() {
        assertTrue(client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder()
                                .put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "20mb")
                                .put(CHUNK_SIZE_SETTING.getKey(), RecoverySettings.DEFAULT_CHUNK_SIZE)
                ).get().isAcknowledged());
    }

    public void testGatewayRecovery() throws Exception {
        logger.info("--> start nodes");
        String node = internalCluster().startNode();

        createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT, REPLICA_COUNT);

        logger.info("--> restarting cluster");
        internalCluster().fullRestart();
        ensureGreen();

        logger.info("--> request recoveries");
        RecoveryResponse response = client().admin().indices().prepareRecoveries(INDEX_NAME).execute().actionGet();
        assertThat(response.shardRecoveryStates().size(), equalTo(SHARD_COUNT));
        assertThat(response.shardRecoveryStates().get(INDEX_NAME).size(), equalTo(1));

        List<RecoveryState> recoveryStates = response.shardRecoveryStates().get(INDEX_NAME);
        assertThat(recoveryStates.size(), equalTo(1));

        RecoveryState recoveryState = recoveryStates.get(0);

        assertRecoveryState(recoveryState, 0, StoreRecoverySource.EXISTING_STORE_INSTANCE, true, Stage.DONE, null, node);

        validateIndexRecoveryState(recoveryState.getIndex());
    }

    public void testGatewayRecoveryTestActiveOnly() throws Exception {
        logger.info("--> start nodes");
        internalCluster().startNode();

        createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT, REPLICA_COUNT);

        logger.info("--> restarting cluster");
        internalCluster().fullRestart();
        ensureGreen();

        logger.info("--> request recoveries");
        RecoveryResponse response = client().admin().indices().prepareRecoveries(INDEX_NAME).setActiveOnly(true).execute().actionGet();

        List<RecoveryState> recoveryStates = response.shardRecoveryStates().get(INDEX_NAME);
        assertThat(recoveryStates.size(), equalTo(0));  // Should not expect any responses back
    }

    public void testReplicaRecovery() throws Exception {
        logger.info("--> start node A");
        String nodeA = internalCluster().startNode();

        logger.info("--> create index on node: {}", nodeA);
        createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT, REPLICA_COUNT);

        logger.info("--> start node B");
        String nodeB = internalCluster().startNode();
        ensureGreen();

        // force a shard recovery from nodeA to nodeB
        logger.info("--> bump replica count");
        client().admin().indices().prepareUpdateSettings(INDEX_NAME)
                .setSettings(Settings.builder().put("number_of_replicas", 1)).execute().actionGet();
        ensureGreen();

        logger.info("--> request recoveries");
        RecoveryResponse response = client().admin().indices().prepareRecoveries(INDEX_NAME).execute().actionGet();

        // we should now have two total shards, one primary and one replica
        List<RecoveryState> recoveryStates = response.shardRecoveryStates().get(INDEX_NAME);
        assertThat(recoveryStates.size(), equalTo(2));

        List<RecoveryState> nodeAResponses = findRecoveriesForTargetNode(nodeA, recoveryStates);
        assertThat(nodeAResponses.size(), equalTo(1));
        List<RecoveryState> nodeBResponses = findRecoveriesForTargetNode(nodeB, recoveryStates);
        assertThat(nodeBResponses.size(), equalTo(1));

        // validate node A recovery
        RecoveryState nodeARecoveryState = nodeAResponses.get(0);
        assertRecoveryState(nodeARecoveryState, 0, StoreRecoverySource.EMPTY_STORE_INSTANCE, true, Stage.DONE, null, nodeA);
        validateIndexRecoveryState(nodeARecoveryState.getIndex());

        // validate node B recovery
        RecoveryState nodeBRecoveryState = nodeBResponses.get(0);
        assertRecoveryState(nodeBRecoveryState, 0, PeerRecoverySource.INSTANCE, false, Stage.DONE, nodeA, nodeB);
        validateIndexRecoveryState(nodeBRecoveryState.getIndex());
    }

    @TestLogging(
            "_root:DEBUG,"
                    + "org.elasticsearch.cluster.service:TRACE,"
                    + "org.elasticsearch.indices.cluster:TRACE,"
                    + "org.elasticsearch.indices.recovery:TRACE,"
                    + "org.elasticsearch.index.shard:TRACE")
    public void testRerouteRecovery() throws Exception {
        logger.info("--> start node A");
        final String nodeA = internalCluster().startNode();

        logger.info("--> create index on node: {}", nodeA);
        ByteSizeValue shardSize = createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT, REPLICA_COUNT).getShards()[0].getStats().getStore().size();

        logger.info("--> start node B");
        final String nodeB = internalCluster().startNode();

        ensureGreen();

        logger.info("--> slowing down recoveries");
        slowDownRecovery(shardSize);

        logger.info("--> move shard from: {} to: {}", nodeA, nodeB);
        client().admin().cluster().prepareReroute()
                .add(new MoveAllocationCommand(INDEX_NAME, 0, nodeA, nodeB))
                .execute().actionGet().getState();

        logger.info("--> waiting for recovery to start both on source and target");
        final Index index = resolveIndex(INDEX_NAME);
        assertBusy(() -> {
            IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeA);
            assertThat(indicesService.indexServiceSafe(index).getShard(0).recoveryStats().currentAsSource(),
                    equalTo(1));
            indicesService = internalCluster().getInstance(IndicesService.class, nodeB);
            assertThat(indicesService.indexServiceSafe(index).getShard(0).recoveryStats().currentAsTarget(),
                    equalTo(1));
        });

        logger.info("--> request recoveries");
        RecoveryResponse response = client().admin().indices().prepareRecoveries(INDEX_NAME).execute().actionGet();

        List<RecoveryState> recoveryStates = response.shardRecoveryStates().get(INDEX_NAME);
        List<RecoveryState> nodeARecoveryStates = findRecoveriesForTargetNode(nodeA, recoveryStates);
        assertThat(nodeARecoveryStates.size(), equalTo(1));
        List<RecoveryState> nodeBRecoveryStates = findRecoveriesForTargetNode(nodeB, recoveryStates);
        assertThat(nodeBRecoveryStates.size(), equalTo(1));

        assertRecoveryState(nodeARecoveryStates.get(0), 0, StoreRecoverySource.EMPTY_STORE_INSTANCE, true, Stage.DONE, null, nodeA);
        validateIndexRecoveryState(nodeARecoveryStates.get(0).getIndex());

        assertOnGoingRecoveryState(nodeBRecoveryStates.get(0), 0, PeerRecoverySource.INSTANCE, true, nodeA, nodeB);
        validateIndexRecoveryState(nodeBRecoveryStates.get(0).getIndex());

        logger.info("--> request node recovery stats");
        NodesStatsResponse statsResponse = client().admin().cluster().prepareNodesStats().clear().setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Recovery)).get();
        long nodeAThrottling = Long.MAX_VALUE;
        long nodeBThrottling = Long.MAX_VALUE;
        for (NodeStats nodeStats : statsResponse.getNodes()) {
            final RecoveryStats recoveryStats = nodeStats.getIndices().getRecoveryStats();
            if (nodeStats.getNode().getName().equals(nodeA)) {
                assertThat("node A should have ongoing recovery as source", recoveryStats.currentAsSource(), equalTo(1));
                assertThat("node A should not have ongoing recovery as target", recoveryStats.currentAsTarget(), equalTo(0));
                nodeAThrottling = recoveryStats.throttleTime().millis();
            }
            if (nodeStats.getNode().getName().equals(nodeB)) {
                assertThat("node B should not have ongoing recovery as source", recoveryStats.currentAsSource(), equalTo(0));
                assertThat("node B should have ongoing recovery as target", recoveryStats.currentAsTarget(), equalTo(1));
                nodeBThrottling = recoveryStats.throttleTime().millis();
            }
        }

        logger.info("--> checking throttling increases");
        final long finalNodeAThrottling = nodeAThrottling;
        final long finalNodeBThrottling = nodeBThrottling;
        assertBusy(() -> {
            NodesStatsResponse statsResponse1 = client().admin().cluster().prepareNodesStats().clear().setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Recovery)).get();
            assertThat(statsResponse1.getNodes(), hasSize(2));
            for (NodeStats nodeStats : statsResponse1.getNodes()) {
                final RecoveryStats recoveryStats = nodeStats.getIndices().getRecoveryStats();
                if (nodeStats.getNode().getName().equals(nodeA)) {
                    assertThat("node A throttling should increase", recoveryStats.throttleTime().millis(), greaterThan(finalNodeAThrottling));
                }
                if (nodeStats.getNode().getName().equals(nodeB)) {
                    assertThat("node B throttling should increase", recoveryStats.throttleTime().millis(), greaterThan(finalNodeBThrottling));
                }
            }
        });


        logger.info("--> speeding up recoveries");
        restoreRecoverySpeed();

        // wait for it to be finished
        ensureGreen();

        response = client().admin().indices().prepareRecoveries(INDEX_NAME).execute().actionGet();

        recoveryStates = response.shardRecoveryStates().get(INDEX_NAME);
        assertThat(recoveryStates.size(), equalTo(1));

        assertRecoveryState(recoveryStates.get(0), 0, PeerRecoverySource.INSTANCE, true, Stage.DONE, nodeA, nodeB);
        validateIndexRecoveryState(recoveryStates.get(0).getIndex());
        Consumer<String> assertNodeHasThrottleTimeAndNoRecoveries = nodeName ->  {
            NodesStatsResponse nodesStatsResponse = client().admin().cluster().prepareNodesStats().setNodesIds(nodeName)
                .clear().setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Recovery)).get();
            assertThat(nodesStatsResponse.getNodes(), hasSize(1));
            NodeStats nodeStats = nodesStatsResponse.getNodes().get(0);
            final RecoveryStats recoveryStats = nodeStats.getIndices().getRecoveryStats();
            assertThat(recoveryStats.currentAsSource(), equalTo(0));
            assertThat(recoveryStats.currentAsTarget(), equalTo(0));
            assertThat(nodeName + " throttling should be >0", recoveryStats.throttleTime().millis(), greaterThan(0L));
        };
        // we have to use assertBusy as recovery counters are decremented only when the last reference to the RecoveryTarget
        // is decremented, which may happen after the recovery was done.
        assertBusy(() -> assertNodeHasThrottleTimeAndNoRecoveries.accept(nodeA));
        assertBusy(() -> assertNodeHasThrottleTimeAndNoRecoveries.accept(nodeB));

        logger.info("--> bump replica count");
        client().admin().indices().prepareUpdateSettings(INDEX_NAME)
                .setSettings(Settings.builder().put("number_of_replicas", 1)).execute().actionGet();
        ensureGreen();

        assertBusy(() -> assertNodeHasThrottleTimeAndNoRecoveries.accept(nodeA));
        assertBusy(() -> assertNodeHasThrottleTimeAndNoRecoveries.accept(nodeB));

        logger.info("--> start node C");
        String nodeC = internalCluster().startNode();
        assertFalse(client().admin().cluster().prepareHealth().setWaitForNodes("3").get().isTimedOut());

        logger.info("--> slowing down recoveries");
        slowDownRecovery(shardSize);

        logger.info("--> move replica shard from: {} to: {}", nodeA, nodeC);
        client().admin().cluster().prepareReroute()
                .add(new MoveAllocationCommand(INDEX_NAME, 0, nodeA, nodeC))
                .execute().actionGet().getState();

        response = client().admin().indices().prepareRecoveries(INDEX_NAME).execute().actionGet();
        recoveryStates = response.shardRecoveryStates().get(INDEX_NAME);

        nodeARecoveryStates = findRecoveriesForTargetNode(nodeA, recoveryStates);
        assertThat(nodeARecoveryStates.size(), equalTo(1));
        nodeBRecoveryStates = findRecoveriesForTargetNode(nodeB, recoveryStates);
        assertThat(nodeBRecoveryStates.size(), equalTo(1));
        List<RecoveryState> nodeCRecoveryStates = findRecoveriesForTargetNode(nodeC, recoveryStates);
        assertThat(nodeCRecoveryStates.size(), equalTo(1));

        assertRecoveryState(nodeARecoveryStates.get(0), 0, PeerRecoverySource.INSTANCE, false, Stage.DONE, nodeB, nodeA);
        validateIndexRecoveryState(nodeARecoveryStates.get(0).getIndex());

        assertRecoveryState(nodeBRecoveryStates.get(0), 0, PeerRecoverySource.INSTANCE, true, Stage.DONE, nodeA, nodeB);
        validateIndexRecoveryState(nodeBRecoveryStates.get(0).getIndex());

        // relocations of replicas are marked as REPLICA and the source node is the node holding the primary (B)
        assertOnGoingRecoveryState(nodeCRecoveryStates.get(0), 0, PeerRecoverySource.INSTANCE, false, nodeB, nodeC);
        validateIndexRecoveryState(nodeCRecoveryStates.get(0).getIndex());

        if (randomBoolean()) {
            // shutdown node with relocation source of replica shard and check if recovery continues
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeA));
            ensureStableCluster(2);

            response = client().admin().indices().prepareRecoveries(INDEX_NAME).execute().actionGet();
            recoveryStates = response.shardRecoveryStates().get(INDEX_NAME);

            nodeARecoveryStates = findRecoveriesForTargetNode(nodeA, recoveryStates);
            assertThat(nodeARecoveryStates.size(), equalTo(0));
            nodeBRecoveryStates = findRecoveriesForTargetNode(nodeB, recoveryStates);
            assertThat(nodeBRecoveryStates.size(), equalTo(1));
            nodeCRecoveryStates = findRecoveriesForTargetNode(nodeC, recoveryStates);
            assertThat(nodeCRecoveryStates.size(), equalTo(1));

            assertRecoveryState(nodeBRecoveryStates.get(0), 0, PeerRecoverySource.INSTANCE, true, Stage.DONE, nodeA, nodeB);
            validateIndexRecoveryState(nodeBRecoveryStates.get(0).getIndex());

            assertOnGoingRecoveryState(nodeCRecoveryStates.get(0), 0, PeerRecoverySource.INSTANCE, false, nodeB, nodeC);
            validateIndexRecoveryState(nodeCRecoveryStates.get(0).getIndex());
        }

        logger.info("--> speeding up recoveries");
        restoreRecoverySpeed();
        ensureGreen();

        response = client().admin().indices().prepareRecoveries(INDEX_NAME).execute().actionGet();
        recoveryStates = response.shardRecoveryStates().get(INDEX_NAME);

        nodeARecoveryStates = findRecoveriesForTargetNode(nodeA, recoveryStates);
        assertThat(nodeARecoveryStates.size(), equalTo(0));
        nodeBRecoveryStates = findRecoveriesForTargetNode(nodeB, recoveryStates);
        assertThat(nodeBRecoveryStates.size(), equalTo(1));
        nodeCRecoveryStates = findRecoveriesForTargetNode(nodeC, recoveryStates);
        assertThat(nodeCRecoveryStates.size(), equalTo(1));

        assertRecoveryState(nodeBRecoveryStates.get(0), 0, PeerRecoverySource.INSTANCE, true, Stage.DONE, nodeA, nodeB);
        validateIndexRecoveryState(nodeBRecoveryStates.get(0).getIndex());

        // relocations of replicas are marked as REPLICA and the source node is the node holding the primary (B)
        assertRecoveryState(nodeCRecoveryStates.get(0), 0, PeerRecoverySource.INSTANCE, false, Stage.DONE, nodeB, nodeC);
        validateIndexRecoveryState(nodeCRecoveryStates.get(0).getIndex());
    }

    public void testSnapshotRecovery() throws Exception {
        logger.info("--> start node A");
        String nodeA = internalCluster().startNode();

        logger.info("--> create repository");
        assertAcked(client().admin().cluster().preparePutRepository(REPO_NAME)
                .setType("fs").setSettings(Settings.builder()
                                .put("location", randomRepoPath())
                                .put("compress", false)
                ).get());

        ensureGreen();

        logger.info("--> create index on node: {}", nodeA);
        createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT, REPLICA_COUNT);

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot(REPO_NAME, SNAP_NAME)
                .setWaitForCompletion(true).setIndices(INDEX_NAME).get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        assertThat(client().admin().cluster().prepareGetSnapshots(REPO_NAME).setSnapshots(SNAP_NAME).get()
                .getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));

        client().admin().indices().prepareClose(INDEX_NAME).execute().actionGet();

        logger.info("--> restore");
        RestoreSnapshotResponse restoreSnapshotResponse = client().admin().cluster()
                .prepareRestoreSnapshot(REPO_NAME, SNAP_NAME).setWaitForCompletion(true).execute().actionGet();
        int totalShards = restoreSnapshotResponse.getRestoreInfo().totalShards();
        assertThat(totalShards, greaterThan(0));

        ensureGreen();

        logger.info("--> request recoveries");
        RecoveryResponse response = client().admin().indices().prepareRecoveries(INDEX_NAME).execute().actionGet();

        for (Map.Entry<String, List<RecoveryState>> indexRecoveryStates : response.shardRecoveryStates().entrySet()) {

            assertThat(indexRecoveryStates.getKey(), equalTo(INDEX_NAME));
            List<RecoveryState> recoveryStates = indexRecoveryStates.getValue();
            assertThat(recoveryStates.size(), equalTo(totalShards));

            for (RecoveryState recoveryState : recoveryStates) {
                SnapshotRecoverySource recoverySource = new SnapshotRecoverySource(
                    new Snapshot(REPO_NAME, createSnapshotResponse.getSnapshotInfo().snapshotId()),
                    Version.CURRENT, INDEX_NAME);
                assertRecoveryState(recoveryState, 0, recoverySource, true, Stage.DONE, null, nodeA);
                validateIndexRecoveryState(recoveryState.getIndex());
            }
        }
    }

    private List<RecoveryState> findRecoveriesForTargetNode(String nodeName, List<RecoveryState> recoveryStates) {
        List<RecoveryState> nodeResponses = new ArrayList<>();
        for (RecoveryState recoveryState : recoveryStates) {
            if (recoveryState.getTargetNode().getName().equals(nodeName)) {
                nodeResponses.add(recoveryState);
            }
        }
        return nodeResponses;
    }

    private IndicesStatsResponse createAndPopulateIndex(String name, int nodeCount, int shardCount, int replicaCount)
            throws ExecutionException, InterruptedException {

        logger.info("--> creating test index: {}", name);
        assertAcked(prepareCreate(name, nodeCount, Settings.builder().put("number_of_shards", shardCount)
                .put("number_of_replicas", replicaCount).put(Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), 0)));
        ensureGreen();

        logger.info("--> indexing sample data");
        final int numDocs = between(MIN_DOC_COUNT, MAX_DOC_COUNT);
        final IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];

        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex(name, INDEX_TYPE).
                    setSource("foo-int", randomInt(),
                            "foo-string", randomAlphaOfLength(32),
                            "foo-float", randomFloat());
        }

        indexRandom(true, docs);
        flush();
        assertThat(client().prepareSearch(name).setSize(0).get().getHits().getTotalHits(), equalTo((long) numDocs));
        return client().admin().indices().prepareStats(name).execute().actionGet();
    }

    private void validateIndexRecoveryState(RecoveryState.Index indexState) {
        assertThat(indexState.time(), greaterThanOrEqualTo(0L));
        assertThat(indexState.recoveredFilesPercent(), greaterThanOrEqualTo(0.0f));
        assertThat(indexState.recoveredFilesPercent(), lessThanOrEqualTo(100.0f));
        assertThat(indexState.recoveredBytesPercent(), greaterThanOrEqualTo(0.0f));
        assertThat(indexState.recoveredBytesPercent(), lessThanOrEqualTo(100.0f));
    }

    public void testDisconnectsWhileRecovering() throws Exception {
        final String indexName = "test";
        final Settings nodeSettings = Settings.builder()
                .put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.getKey(), "100ms")
                .put(RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING.getKey(), "1s")
                .put(MockFSDirectoryService.RANDOM_PREVENT_DOUBLE_WRITE_SETTING.getKey(), false) // restarted recoveries will delete temp files and write them again
                .build();
        // start a master node
        internalCluster().startNode(nodeSettings);

        final String blueNodeName = internalCluster().startNode(Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build());
        final String redNodeName = internalCluster().startNode(Settings.builder().put("node.attr.color", "red").put(nodeSettings).build());

        ClusterHealthResponse response = client().admin().cluster().prepareHealth().setWaitForNodes(">=3").get();
        assertThat(response.isTimedOut(), is(false));


        client().admin().indices().prepareCreate(indexName)
                .setSettings(
                        Settings.builder()
                                .put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "color", "blue")
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                ).get();

        List<IndexRequestBuilder> requests = new ArrayList<>();
        int numDocs = scaledRandomIntBetween(25, 250);
        for (int i = 0; i < numDocs; i++) {
            requests.add(client().prepareIndex(indexName, "type").setSource("{}", XContentType.JSON));
        }
        indexRandom(true, requests);
        ensureSearchable(indexName);

        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().get();
        final String blueNodeId = internalCluster().getInstance(ClusterService.class, blueNodeName).localNode().getId();

        assertFalse(stateResponse.getState().getRoutingNodes().node(blueNodeId).isEmpty());

        SearchResponse searchResponse = client().prepareSearch(indexName).get();
        assertHitCount(searchResponse, numDocs);

        String[] recoveryActions = new String[]{
                PeerRecoverySourceService.Actions.START_RECOVERY,
                PeerRecoveryTargetService.Actions.FILES_INFO,
                PeerRecoveryTargetService.Actions.FILE_CHUNK,
                PeerRecoveryTargetService.Actions.CLEAN_FILES,
                //RecoveryTarget.Actions.TRANSLOG_OPS, <-- may not be sent if already flushed
                PeerRecoveryTargetService.Actions.PREPARE_TRANSLOG,
                PeerRecoveryTargetService.Actions.FINALIZE
        };
        final String recoveryActionToBlock = randomFrom(recoveryActions);
        final boolean dropRequests = randomBoolean();
        logger.info("--> will {} between blue & red on [{}]", dropRequests ? "drop requests" : "break connection", recoveryActionToBlock);

        MockTransportService blueMockTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, blueNodeName);
        MockTransportService redMockTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, redNodeName);
        TransportService redTransportService = internalCluster().getInstance(TransportService.class, redNodeName);
        TransportService blueTransportService = internalCluster().getInstance(TransportService.class, blueNodeName);
        final CountDownLatch requestBlocked = new CountDownLatch(1);

        blueMockTransportService.addDelegate(redTransportService, new RecoveryActionBlocker(dropRequests, recoveryActionToBlock, blueMockTransportService.original(), requestBlocked));
        redMockTransportService.addDelegate(blueTransportService, new RecoveryActionBlocker(dropRequests, recoveryActionToBlock, redMockTransportService.original(), requestBlocked));

        logger.info("--> starting recovery from blue to red");
        client().admin().indices().prepareUpdateSettings(indexName).setSettings(
                Settings.builder()
                        .put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "color", "red,blue")
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
        ).get();

        requestBlocked.await();

        logger.info("--> stopping to block recovery");
        blueMockTransportService.clearAllRules();
        redMockTransportService.clearAllRules();

        ensureGreen();
        searchResponse = client(redNodeName).prepareSearch(indexName).setPreference("_local").get();
        assertHitCount(searchResponse, numDocs);

    }

    private class RecoveryActionBlocker extends MockTransportService.DelegateTransport {
        private final boolean dropRequests;
        private final String recoveryActionToBlock;
        private final CountDownLatch requestBlocked;

        RecoveryActionBlocker(boolean dropRequests, String recoveryActionToBlock, Transport delegate, CountDownLatch requestBlocked) {
            super(delegate);
            this.dropRequests = dropRequests;
            this.recoveryActionToBlock = recoveryActionToBlock;
            this.requestBlocked = requestBlocked;
        }

        @Override
        protected void sendRequest(Connection connection, long requestId, String action, TransportRequest request,
                                   TransportRequestOptions options) throws IOException {
            if (recoveryActionToBlock.equals(action) || requestBlocked.getCount() == 0) {
                logger.info("--> preventing {} request", action);
                requestBlocked.countDown();
                if (dropRequests) {
                    return;
                }
                throw new ConnectTransportException(connection.getNode(), "DISCONNECT: prevented " + action + " request");
            }
            super.sendRequest(connection, requestId, action, request, options);
        }
    }

    /**
     * Tests scenario where recovery target successfully sends recovery request to source but then the channel gets closed while
     * the source is working on the recovery process.
     */
    @TestLogging("_root:DEBUG,org.elasticsearch.indices.recovery:TRACE")
    public void testDisconnectsDuringRecovery() throws Exception {
        boolean primaryRelocation = randomBoolean();
        final String indexName = "test";
        final Settings nodeSettings = Settings.builder()
            .put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.getKey(), TimeValue.timeValueMillis(randomIntBetween(0, 100)))
            .build();
        TimeValue disconnectAfterDelay = TimeValue.timeValueMillis(randomIntBetween(0, 100));
        // start a master node
        String masterNodeName = internalCluster().startMasterOnlyNode(nodeSettings);

        final String blueNodeName = internalCluster().startNode(Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build());
        final String redNodeName = internalCluster().startNode(Settings.builder().put("node.attr.color", "red").put(nodeSettings).build());

        client().admin().indices().prepareCreate(indexName)
            .setSettings(
                Settings.builder()
                    .put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "color", "blue")
                    .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            ).get();

        List<IndexRequestBuilder> requests = new ArrayList<>();
        int numDocs = scaledRandomIntBetween(25, 250);
        for (int i = 0; i < numDocs; i++) {
            requests.add(client().prepareIndex(indexName, "type").setSource("{}", XContentType.JSON));
        }
        indexRandom(true, requests);
        ensureSearchable(indexName);
        assertHitCount(client().prepareSearch(indexName).get(), numDocs);

        MockTransportService masterTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, masterNodeName);
        MockTransportService blueMockTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, blueNodeName);
        MockTransportService redMockTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, redNodeName);

        redMockTransportService.addDelegate(blueMockTransportService, new MockTransportService.DelegateTransport(redMockTransportService.original()) {
            private final AtomicInteger count = new AtomicInteger();

            @Override
            protected void sendRequest(Connection connection, long requestId, String action, TransportRequest request,
                                       TransportRequestOptions options) throws IOException {
                logger.info("--> sending request {} on {}", action, connection.getNode());
                if (PeerRecoverySourceService.Actions.START_RECOVERY.equals(action) && count.incrementAndGet() == 1) {
                    // ensures that it's considered as valid recovery attempt by source
                    try {
                        awaitBusy(() -> client(blueNodeName).admin().cluster().prepareState().setLocal(true).get()
                            .getState().getRoutingTable().index("test").shard(0).getAllInitializingShards().isEmpty() == false);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    super.sendRequest(connection, requestId, action, request, options);
                    try {
                        Thread.sleep(disconnectAfterDelay.millis());
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    throw new ConnectTransportException(connection.getNode(), "DISCONNECT: simulation disconnect after successfully sending " + action + " request");
                } else {
                    super.sendRequest(connection, requestId, action, request, options);
                }
            }
        });

        final AtomicBoolean finalized = new AtomicBoolean();
        blueMockTransportService.addDelegate(redMockTransportService, new MockTransportService.DelegateTransport(blueMockTransportService.original()) {
            @Override
            protected void sendRequest(Connection connection, long requestId, String action, TransportRequest request,
                                       TransportRequestOptions options) throws IOException {
                logger.info("--> sending request {} on {}", action, connection.getNode());
                if (action.equals(PeerRecoveryTargetService.Actions.FINALIZE)) {
                    finalized.set(true);
                }
                super.sendRequest(connection, requestId, action, request, options);
            }
        });

        for (MockTransportService mockTransportService : Arrays.asList(redMockTransportService, blueMockTransportService)) {
            mockTransportService.addDelegate(masterTransportService, new MockTransportService.DelegateTransport(mockTransportService.original()) {
                @Override
                protected void sendRequest(Connection connection, long requestId, String action, TransportRequest request,
                                           TransportRequestOptions options) throws IOException {
                    logger.info("--> sending request {} on {}", action, connection.getNode());
                    if ((primaryRelocation && finalized.get()) == false) {
                        assertNotEquals(action, ShardStateAction.SHARD_FAILED_ACTION_NAME);
                    }
                    super.sendRequest(connection, requestId, action, request, options);
                }
            });
        }

        if (primaryRelocation) {
            logger.info("--> starting primary relocation recovery from blue to red");
            client().admin().indices().prepareUpdateSettings(indexName).setSettings(
                Settings.builder()
                    .put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "color", "red")
            ).get();

            ensureGreen(); // also waits for relocation / recovery to complete
            // if a primary relocation fails after the source shard has been marked as relocated, both source and target are failed. If the
            // source shard is moved back to started because the target fails first, it's possible that there is a cluster state where the
            // shard is marked as started again (and ensureGreen returns), but while applying the cluster state the primary is failed and
            // will be reallocated. The cluster will thus become green, then red, then green again. Triggering a refresh here before
            // searching helps, as in contrast to search actions, refresh waits for the closed shard to be reallocated.
            client().admin().indices().prepareRefresh(indexName).get();
        } else {
            logger.info("--> starting replica recovery from blue to red");
            client().admin().indices().prepareUpdateSettings(indexName).setSettings(
                Settings.builder()
                    .put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "color", "red,blue")
                    .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
            ).get();

            ensureGreen();
        }

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch(indexName).get(), numDocs);
        }
    }
}
