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

import com.carrotsearch.randomizedtesting.LifecycleScope;
import com.google.common.util.concurrent.ListenableFuture;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.recovery.ShardRecoveryResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.discovery.DiscoveryService;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState.Stage;
import org.elasticsearch.indices.recovery.RecoveryState.Type;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.store.MockDirectoryHelper;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.*;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.*;

/**
 *
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class IndexRecoveryTests extends ElasticsearchIntegrationTest {

    private static final String INDEX_NAME = "test-idx-1";
    private static final String INDEX_TYPE = "test-type-1";
    private static final String REPO_NAME = "test-repo-1";
    private static final String SNAP_NAME = "test-snap-1";

    private static final int MIN_DOC_COUNT = 500;
    private static final int MAX_DOC_COUNT = 1000;
    private static final int SHARD_COUNT = 1;
    private static final int REPLICA_COUNT = 0;


    private void assertRecoveryStateWithoutStage(RecoveryState state, int shardId, Type type,
                                                 String sourceNode, String targetNode, boolean hasRestoreSource) {
        assertThat(state.getShardId().getId(), equalTo(shardId));
        assertThat(state.getType(), equalTo(type));
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
        if (hasRestoreSource) {
            assertNotNull(state.getRestoreSource());
        } else {
            assertNull(state.getRestoreSource());
        }

    }

    private void assertRecoveryState(RecoveryState state, int shardId, Type type, Stage stage,
                                     String sourceNode, String targetNode, boolean hasRestoreSource) {
        assertRecoveryStateWithoutStage(state, shardId, type, sourceNode, targetNode, hasRestoreSource);
        assertThat(state.getStage(), equalTo(stage));
    }

    private void assertOnGoingRecoveryState(RecoveryState state, int shardId, Type type,
                                            String sourceNode, String targetNode, boolean hasRestoreSource) {
        assertRecoveryStateWithoutStage(state, shardId, type, sourceNode, targetNode, hasRestoreSource);
        assertThat(state.getStage(), not(equalTo(Stage.DONE)));
    }

    private void slowDownRecovery(ByteSizeValue shardSize) {
        long chunkSize = shardSize.bytes() / 10;
        assertTrue(client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(ImmutableSettings.builder()
                                // one chunk per sec..
                                .put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC, chunkSize)
                                .put(RecoverySettings.INDICES_RECOVERY_FILE_CHUNK_SIZE, chunkSize)
                )
                .get().isAcknowledged());
    }

    private void restoreRecoverySpeed() {
        assertTrue(client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(ImmutableSettings.builder()
                                .put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC, "20mb")
                                .put(RecoverySettings.INDICES_RECOVERY_FILE_CHUNK_SIZE, "512kb")
                )
                .get().isAcknowledged());
    }

    @Test
    public void gatewayRecoveryTest() throws Exception {
        logger.info("--> start nodes");
        String node = internalCluster().startNode();

        createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT, REPLICA_COUNT);

        logger.info("--> restarting cluster");
        internalCluster().fullRestart();
        ensureGreen();

        logger.info("--> request recoveries");
        RecoveryResponse response = client().admin().indices().prepareRecoveries(INDEX_NAME).execute().actionGet();
        assertThat(response.shardResponses().size(), equalTo(SHARD_COUNT));
        assertThat(response.shardResponses().get(INDEX_NAME).size(), equalTo(1));

        List<ShardRecoveryResponse> shardResponses = response.shardResponses().get(INDEX_NAME);
        assertThat(shardResponses.size(), equalTo(1));

        ShardRecoveryResponse shardResponse = shardResponses.get(0);
        RecoveryState state = shardResponse.recoveryState();

        assertRecoveryState(state, 0, Type.GATEWAY, Stage.DONE, node, node, false);

        validateIndexRecoveryState(state.getIndex());
    }

    @Test
    public void gatewayRecoveryTestActiveOnly() throws Exception {
        logger.info("--> start nodes");
        internalCluster().startNode();

        createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT, REPLICA_COUNT);

        logger.info("--> restarting cluster");
        internalCluster().fullRestart();
        ensureGreen();

        logger.info("--> request recoveries");
        RecoveryResponse response = client().admin().indices().prepareRecoveries(INDEX_NAME).setActiveOnly(true).execute().actionGet();

        List<ShardRecoveryResponse> shardResponses = response.shardResponses().get(INDEX_NAME);
        assertThat(shardResponses.size(), equalTo(0));  // Should not expect any responses back
    }

    @Test
    public void replicaRecoveryTest() throws Exception {
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
                .setSettings(settingsBuilder().put("number_of_replicas", 1)).execute().actionGet();
        ensureGreen();

        logger.info("--> request recoveries");
        RecoveryResponse response = client().admin().indices().prepareRecoveries(INDEX_NAME).execute().actionGet();

        // we should now have two total shards, one primary and one replica
        List<ShardRecoveryResponse> shardResponses = response.shardResponses().get(INDEX_NAME);
        assertThat(shardResponses.size(), equalTo(2));

        List<ShardRecoveryResponse> nodeAResponses = findRecoveriesForTargetNode(nodeA, shardResponses);
        assertThat(nodeAResponses.size(), equalTo(1));
        List<ShardRecoveryResponse> nodeBResponses = findRecoveriesForTargetNode(nodeB, shardResponses);
        assertThat(nodeBResponses.size(), equalTo(1));

        // validate node A recovery
        ShardRecoveryResponse nodeAShardResponse = nodeAResponses.get(0);
        assertRecoveryState(nodeAShardResponse.recoveryState(), 0, Type.GATEWAY, Stage.DONE, nodeA, nodeA, false);
        validateIndexRecoveryState(nodeAShardResponse.recoveryState().getIndex());

        // validate node B recovery
        ShardRecoveryResponse nodeBShardResponse = nodeBResponses.get(0);
        assertRecoveryState(nodeBShardResponse.recoveryState(), 0, Type.REPLICA, Stage.DONE, nodeA, nodeB, false);
        validateIndexRecoveryState(nodeBShardResponse.recoveryState().getIndex());
    }

    @Test
    @TestLogging("indices.recovery:TRACE")
    public void rerouteRecoveryTest() throws Exception {
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
                .add(new MoveAllocationCommand(new ShardId(INDEX_NAME, 0), nodeA, nodeB))
                .execute().actionGet().getState();

        logger.info("--> waiting for recovery to start both on source and target");
        assertBusy(new Runnable() {
            @Override
            public void run() {
                IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeA);
                assertThat(indicesService.indexServiceSafe(INDEX_NAME).shardSafe(0).recoveryStats().currentAsSource(),
                        equalTo(1));
                indicesService = internalCluster().getInstance(IndicesService.class, nodeB);
                assertThat(indicesService.indexServiceSafe(INDEX_NAME).shardSafe(0).recoveryStats().currentAsTarget(),
                        equalTo(1));
            }
        });

        logger.info("--> request recoveries");
        RecoveryResponse response = client().admin().indices().prepareRecoveries(INDEX_NAME).execute().actionGet();

        List<ShardRecoveryResponse> shardResponses = response.shardResponses().get(INDEX_NAME);
        List<ShardRecoveryResponse> nodeAResponses = findRecoveriesForTargetNode(nodeA, shardResponses);
        assertThat(nodeAResponses.size(), equalTo(1));
        List<ShardRecoveryResponse> nodeBResponses = findRecoveriesForTargetNode(nodeB, shardResponses);
        assertThat(nodeBResponses.size(), equalTo(1));

        assertRecoveryState(nodeAResponses.get(0).recoveryState(), 0, Type.GATEWAY, Stage.DONE, nodeA, nodeA, false);
        validateIndexRecoveryState(nodeAResponses.get(0).recoveryState().getIndex());

        assertOnGoingRecoveryState(nodeBResponses.get(0).recoveryState(), 0, Type.RELOCATION, nodeA, nodeB, false);
        validateIndexRecoveryState(nodeBResponses.get(0).recoveryState().getIndex());

        logger.info("--> request node recovery stats");
        NodesStatsResponse statsResponse = client().admin().cluster().prepareNodesStats().clear().setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Recovery)).get();
        long nodeAThrottling = Long.MAX_VALUE;
        long nodeBThrottling = Long.MAX_VALUE;
        for (NodeStats nodeStats : statsResponse.getNodes()) {
            final RecoveryStats recoveryStats = nodeStats.getIndices().getRecoveryStats();
            if (nodeStats.getNode().name().equals(nodeA)) {
                assertThat("node A should have ongoing recovery as source", recoveryStats.currentAsSource(), equalTo(1));
                assertThat("node A should not have ongoing recovery as target", recoveryStats.currentAsTarget(), equalTo(0));
                nodeAThrottling = recoveryStats.throttleTime().millis();
            }
            if (nodeStats.getNode().name().equals(nodeB)) {
                assertThat("node B should not have ongoing recovery as source", recoveryStats.currentAsSource(), equalTo(0));
                assertThat("node B should have ongoing recovery as target", recoveryStats.currentAsTarget(), equalTo(1));
                nodeBThrottling = recoveryStats.throttleTime().millis();
            }
        }

        logger.info("--> checking throttling increases");
        final long finalNodeAThrottling = nodeAThrottling;
        final long finalNodeBThrottling = nodeBThrottling;
        assertBusy(new Runnable() {
            @Override
            public void run() {
                NodesStatsResponse statsResponse = client().admin().cluster().prepareNodesStats().clear().setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Recovery)).get();
                assertThat(statsResponse.getNodes(), arrayWithSize(2));
                for (NodeStats nodeStats : statsResponse.getNodes()) {
                    final RecoveryStats recoveryStats = nodeStats.getIndices().getRecoveryStats();
                    if (nodeStats.getNode().name().equals(nodeA)) {
                        assertThat("node A throttling should increase", recoveryStats.throttleTime().millis(), greaterThan(finalNodeAThrottling));
                    }
                    if (nodeStats.getNode().name().equals(nodeB)) {
                        assertThat("node B throttling should increase", recoveryStats.throttleTime().millis(), greaterThan(finalNodeBThrottling));
                    }
                }
            }
        });


        logger.info("--> speeding up recoveries");
        restoreRecoverySpeed();

        // wait for it to be finished
        ensureGreen();

        response = client().admin().indices().prepareRecoveries(INDEX_NAME).execute().actionGet();

        shardResponses = response.shardResponses().get(INDEX_NAME);
        assertThat(shardResponses.size(), equalTo(1));

        assertRecoveryState(shardResponses.get(0).recoveryState(), 0, Type.RELOCATION, Stage.DONE, nodeA, nodeB, false);
        validateIndexRecoveryState(shardResponses.get(0).recoveryState().getIndex());

        statsResponse = client().admin().cluster().prepareNodesStats().clear().setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Recovery)).get();
        assertThat(statsResponse.getNodes(), arrayWithSize(2));
        for (NodeStats nodeStats : statsResponse.getNodes()) {
            final RecoveryStats recoveryStats = nodeStats.getIndices().getRecoveryStats();
            assertThat(recoveryStats.currentAsSource(), equalTo(0));
            assertThat(recoveryStats.currentAsTarget(), equalTo(0));
            if (nodeStats.getNode().name().equals(nodeA)) {
                assertThat("node A throttling should be >0", recoveryStats.throttleTime().millis(), greaterThan(0l));
            }
            if (nodeStats.getNode().name().equals(nodeB)) {
                assertThat("node B throttling should be >0 ", recoveryStats.throttleTime().millis(), greaterThan(0l));
            }
        }

        logger.info("--> bump replica count");
        client().admin().indices().prepareUpdateSettings(INDEX_NAME)
                .setSettings(settingsBuilder().put("number_of_replicas", 1)).execute().actionGet();
        ensureGreen();

        statsResponse = client().admin().cluster().prepareNodesStats().clear().setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Recovery)).get();
        assertThat(statsResponse.getNodes(), arrayWithSize(2));
        for (NodeStats nodeStats : statsResponse.getNodes()) {
            final RecoveryStats recoveryStats = nodeStats.getIndices().getRecoveryStats();
            assertThat(recoveryStats.currentAsSource(), equalTo(0));
            assertThat(recoveryStats.currentAsTarget(), equalTo(0));
            if (nodeStats.getNode().name().equals(nodeA)) {
                assertThat("node A throttling should be >0", recoveryStats.throttleTime().millis(), greaterThan(0l));
            }
            if (nodeStats.getNode().name().equals(nodeB)) {
                assertThat("node B throttling should be >0 ", recoveryStats.throttleTime().millis(), greaterThan(0l));
            }
        }

        logger.info("--> start node C");
        String nodeC = internalCluster().startNode();
        assertFalse(client().admin().cluster().prepareHealth().setWaitForNodes("3").get().isTimedOut());

        logger.info("--> slowing down recoveries");
        slowDownRecovery(shardSize);

        logger.info("--> move replica shard from: {} to: {}", nodeA, nodeC);
        client().admin().cluster().prepareReroute()
                .add(new MoveAllocationCommand(new ShardId(INDEX_NAME, 0), nodeA, nodeC))
                .execute().actionGet().getState();

        response = client().admin().indices().prepareRecoveries(INDEX_NAME).execute().actionGet();
        shardResponses = response.shardResponses().get(INDEX_NAME);

        nodeAResponses = findRecoveriesForTargetNode(nodeA, shardResponses);
        assertThat(nodeAResponses.size(), equalTo(1));
        nodeBResponses = findRecoveriesForTargetNode(nodeB, shardResponses);
        assertThat(nodeBResponses.size(), equalTo(1));
        List<ShardRecoveryResponse> nodeCResponses = findRecoveriesForTargetNode(nodeC, shardResponses);
        assertThat(nodeCResponses.size(), equalTo(1));

        assertRecoveryState(nodeAResponses.get(0).recoveryState(), 0, Type.REPLICA, Stage.DONE, nodeB, nodeA, false);
        validateIndexRecoveryState(nodeAResponses.get(0).recoveryState().getIndex());

        assertRecoveryState(nodeBResponses.get(0).recoveryState(), 0, Type.RELOCATION, Stage.DONE, nodeA, nodeB, false);
        validateIndexRecoveryState(nodeBResponses.get(0).recoveryState().getIndex());

        // relocations of replicas are marked as REPLICA and the source node is the node holding the primary (B)
        assertOnGoingRecoveryState(nodeCResponses.get(0).recoveryState(), 0, Type.REPLICA, nodeB, nodeC, false);
        validateIndexRecoveryState(nodeCResponses.get(0).recoveryState().getIndex());

        logger.info("--> speeding up recoveries");
        restoreRecoverySpeed();
        ensureGreen();

        response = client().admin().indices().prepareRecoveries(INDEX_NAME).execute().actionGet();
        shardResponses = response.shardResponses().get(INDEX_NAME);

        nodeAResponses = findRecoveriesForTargetNode(nodeA, shardResponses);
        assertThat(nodeAResponses.size(), equalTo(0));
        nodeBResponses = findRecoveriesForTargetNode(nodeB, shardResponses);
        assertThat(nodeBResponses.size(), equalTo(1));
        nodeCResponses = findRecoveriesForTargetNode(nodeC, shardResponses);
        assertThat(nodeCResponses.size(), equalTo(1));

        assertRecoveryState(nodeBResponses.get(0).recoveryState(), 0, Type.RELOCATION, Stage.DONE, nodeA, nodeB, false);
        validateIndexRecoveryState(nodeBResponses.get(0).recoveryState().getIndex());

        // relocations of replicas are marked as REPLICA and the source node is the node holding the primary (B)
        assertRecoveryState(nodeCResponses.get(0).recoveryState(), 0, Type.REPLICA, Stage.DONE, nodeB, nodeC, false);
        validateIndexRecoveryState(nodeCResponses.get(0).recoveryState().getIndex());
    }

    @Test
    public void snapshotRecoveryTest() throws Exception {
        logger.info("--> start node A");
        String nodeA = internalCluster().startNode();

        logger.info("--> create repository");
        assertAcked(client().admin().cluster().preparePutRepository(REPO_NAME)
                .setType("fs").setSettings(ImmutableSettings.settingsBuilder()
                                .put("location", createTempDir())
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

        for (Map.Entry<String, List<ShardRecoveryResponse>> shardRecoveryResponse : response.shardResponses().entrySet()) {

            assertThat(shardRecoveryResponse.getKey(), equalTo(INDEX_NAME));
            List<ShardRecoveryResponse> shardRecoveryResponses = shardRecoveryResponse.getValue();
            assertThat(shardRecoveryResponses.size(), equalTo(totalShards));

            for (ShardRecoveryResponse shardResponse : shardRecoveryResponses) {
                assertRecoveryState(shardResponse.recoveryState(), 0, Type.SNAPSHOT, Stage.DONE, null, nodeA, true);
                validateIndexRecoveryState(shardResponse.recoveryState().getIndex());
            }
        }
    }

    private List<ShardRecoveryResponse> findRecoveriesForTargetNode(String nodeName, List<ShardRecoveryResponse> responses) {
        List<ShardRecoveryResponse> nodeResponses = new ArrayList<>();
        for (ShardRecoveryResponse response : responses) {
            if (response.recoveryState().getTargetNode().getName().equals(nodeName)) {
                nodeResponses.add(response);
            }
        }
        return nodeResponses;
    }

    private IndicesStatsResponse createAndPopulateIndex(String name, int nodeCount, int shardCount, int replicaCount)
            throws ExecutionException, InterruptedException {

        logger.info("--> creating test index: {}", name);
        assertAcked(prepareCreate(name, nodeCount, settingsBuilder().put("number_of_shards", shardCount)
                .put("number_of_replicas", replicaCount).put(Store.INDEX_STORE_STATS_REFRESH_INTERVAL, 0)));
        ensureGreen();

        logger.info("--> indexing sample data");
        final int numDocs = between(MIN_DOC_COUNT, MAX_DOC_COUNT);
        final IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];

        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex(INDEX_NAME, INDEX_TYPE).
                    setSource("foo-int", randomInt(),
                            "foo-string", randomAsciiOfLength(32),
                            "foo-float", randomFloat());
        }

        indexRandom(true, docs);
        flush();
        assertThat(client().prepareCount(INDEX_NAME).get().getCount(), equalTo((long) numDocs));
        return client().admin().indices().prepareStats(INDEX_NAME).execute().actionGet();
    }

    private void validateIndexRecoveryState(RecoveryState.Index indexState) {
        assertThat(indexState.time(), greaterThanOrEqualTo(0L));
        assertThat(indexState.recoveredFilesPercent(), greaterThanOrEqualTo(0.0f));
        assertThat(indexState.recoveredFilesPercent(), lessThanOrEqualTo(100.0f));
        assertThat(indexState.recoveredBytesPercent(), greaterThanOrEqualTo(0.0f));
        assertThat(indexState.recoveredBytesPercent(), lessThanOrEqualTo(100.0f));
    }

    @Test
    public void disconnectsWhileRecoveringTest() throws Exception {
        final String indexName = "test";
        final Settings nodeSettings = ImmutableSettings.builder()
                .put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK, "100ms")
                .put(RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT, "1s")
                .put("cluster.routing.schedule", "100ms") // aggressive reroute post shard failures
                .put(TransportModule.TRANSPORT_SERVICE_TYPE_KEY, MockTransportService.class.getName())
                .put(MockDirectoryHelper.RANDOM_PREVENT_DOUBLE_WRITE, false) // restarted recoveries will delete temp files and write them again
                .build();
        // start a master node
        internalCluster().startNode(nodeSettings);

        ListenableFuture<String> blueFuture = internalCluster().startNodeAsync(ImmutableSettings.builder().put("node.color", "blue").put(nodeSettings).build());
        ListenableFuture<String> redFuture = internalCluster().startNodeAsync(ImmutableSettings.builder().put("node.color", "red").put(nodeSettings).build());
        final String blueNodeName = blueFuture.get();
        final String redNodeName = redFuture.get();

        ClusterHealthResponse response = client().admin().cluster().prepareHealth().setWaitForNodes(">=3").get();
        assertThat(response.isTimedOut(), is(false));


        client().admin().indices().prepareCreate(indexName)
                .setSettings(
                        ImmutableSettings.builder()
                                .put(FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "color", "blue")
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                ).get();

        List<IndexRequestBuilder> requests = new ArrayList<>();
        int numDocs = scaledRandomIntBetween(25, 250);
        for (int i = 0; i < numDocs; i++) {
            requests.add(client().prepareIndex(indexName, "type").setCreate(true).setSource("{}"));
        }
        indexRandom(true, requests);
        ensureSearchable(indexName);

        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().get();
        final String blueNodeId = internalCluster().getInstance(DiscoveryService.class, blueNodeName).localNode().id();

        assertFalse(stateResponse.getState().readOnlyRoutingNodes().node(blueNodeId).isEmpty());

        SearchResponse searchResponse = client().prepareSearch(indexName).get();
        assertHitCount(searchResponse, numDocs);

        String[] recoveryActions = new String[]{
                RecoverySource.Actions.START_RECOVERY,
                RecoveryTarget.Actions.FILES_INFO,
                RecoveryTarget.Actions.FILE_CHUNK,
                RecoveryTarget.Actions.CLEAN_FILES,
                //RecoveryTarget.Actions.TRANSLOG_OPS, <-- may not be sent if already flushed
                RecoveryTarget.Actions.PREPARE_TRANSLOG,
                RecoveryTarget.Actions.FINALIZE
        };
        final String recoveryActionToBlock = randomFrom(recoveryActions);
        final boolean dropRequests = randomBoolean();
        logger.info("--> will {} between blue & red on [{}]", dropRequests ? "drop requests" : "break connection", recoveryActionToBlock);

        MockTransportService blueMockTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, blueNodeName);
        MockTransportService redMockTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, redNodeName);
        DiscoveryNode redDiscoNode = internalCluster().getInstance(ClusterService.class, redNodeName).localNode();
        DiscoveryNode blueDiscoNode = internalCluster().getInstance(ClusterService.class, blueNodeName).localNode();
        final CountDownLatch requestBlocked = new CountDownLatch(1);

        blueMockTransportService.addDelegate(redDiscoNode, new RecoveryActionBlocker(dropRequests, recoveryActionToBlock, blueMockTransportService.original(), requestBlocked));
        redMockTransportService.addDelegate(blueDiscoNode, new RecoveryActionBlocker(dropRequests, recoveryActionToBlock, redMockTransportService.original(), requestBlocked));

        logger.info("--> starting recovery from blue to red");
        client().admin().indices().prepareUpdateSettings(indexName).setSettings(
                ImmutableSettings.builder()
                        .put(FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "color", "red,blue")
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

        public RecoveryActionBlocker(boolean dropRequests, String recoveryActionToBlock, Transport delegate, CountDownLatch requestBlocked) {
            super(delegate);
            this.dropRequests = dropRequests;
            this.recoveryActionToBlock = recoveryActionToBlock;
            this.requestBlocked = requestBlocked;
        }

        @Override
        public void sendRequest(DiscoveryNode node, long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException, TransportException {
            if (recoveryActionToBlock.equals(action) || requestBlocked.getCount() == 0) {
                logger.info("--> preventing {} request", action);
                requestBlocked.countDown();
                if (dropRequests) {
                    return;
                }
                throw new ConnectTransportException(node, "DISCONNECT: prevented " + action + " request");
            }
            transport.sendRequest(node, requestId, action, request, options);
        }
    }
}
