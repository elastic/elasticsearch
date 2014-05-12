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
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.recovery.ShardRecoveryResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
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

    @Test
    public void gatewayRecoveryTest() throws Exception {
        logger.info("--> start nodes");
        String node = cluster().startNode(settingsBuilder().put("gateway.type", "local"));

        createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT, REPLICA_COUNT);

        logger.info("--> restarting cluster");
        cluster().fullRestart();
        ensureGreen();

        logger.info("--> request recoveries");
        RecoveryResponse response = client().admin().indices().prepareRecoveries(INDEX_NAME).execute().actionGet();
        assertThat(response.shardResponses().size(), equalTo(SHARD_COUNT));
        assertThat(response.shardResponses().get(INDEX_NAME).size(), equalTo(1));

        List<ShardRecoveryResponse> shardResponses = response.shardResponses().get(INDEX_NAME);
        assertThat(shardResponses.size(), equalTo(1));

        ShardRecoveryResponse shardResponse = shardResponses.get(0);
        RecoveryState state = shardResponse.recoveryState();

        assertThat(state.getType(), equalTo(RecoveryState.Type.GATEWAY));
        assertThat(state.getStage(), equalTo(RecoveryState.Stage.DONE));
        assertThat(node, equalTo(state.getSourceNode().getName()));
        assertThat(node, equalTo(state.getTargetNode().getName()));
        assertNull(state.getRestoreSource());

        validateIndexRecoveryState(state.getIndex());
    }

    @Test
    public void gatewayRecoveryTestActiveOnly() throws Exception {
        logger.info("--> start nodes");
        cluster().startNode(settingsBuilder().put("gateway.type", "local"));

        createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT, REPLICA_COUNT);

        logger.info("--> restarting cluster");
        cluster().fullRestart();
        ensureGreen();

        logger.info("--> request recoveries");
        RecoveryResponse response = client().admin().indices().prepareRecoveries(INDEX_NAME).setActiveOnly(true).execute().actionGet();

        List<ShardRecoveryResponse> shardResponses = response.shardResponses().get(INDEX_NAME);
        assertThat(shardResponses.size(), equalTo(0));  // Should not expect any responses back
    }

    @Test
    public void replicaRecoveryTest() throws Exception {
        logger.info("--> start node A");
        String nodeA = cluster().startNode(settingsBuilder().put("gateway.type", "local"));

        logger.info("--> create index on node: {}", nodeA);
        createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT, REPLICA_COUNT);

        logger.info("--> start node B");
        String nodeB = cluster().startNode(settingsBuilder().put("gateway.type", "local"));
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
        assertThat(nodeAShardResponse.recoveryState().getShardId().id(), equalTo(0));
        assertThat(nodeAShardResponse.recoveryState().getSourceNode().getName(), equalTo(nodeA));
        assertThat(nodeAShardResponse.recoveryState().getTargetNode().getName(), equalTo(nodeA));
        assertThat(nodeAShardResponse.recoveryState().getType(), equalTo(RecoveryState.Type.GATEWAY));
        assertThat(nodeAShardResponse.recoveryState().getStage(), equalTo(RecoveryState.Stage.DONE));
        validateIndexRecoveryState(nodeAShardResponse.recoveryState().getIndex());

        // validate node B recovery
        ShardRecoveryResponse nodeBShardResponse = nodeBResponses.get(0);
        assertThat(nodeBShardResponse.recoveryState().getShardId().id(), equalTo(0));
        assertThat(nodeBShardResponse.recoveryState().getSourceNode().getName(), equalTo(nodeA));
        assertThat(nodeBShardResponse.recoveryState().getTargetNode().getName(), equalTo(nodeB));
        assertThat(nodeBShardResponse.recoveryState().getType(), equalTo(RecoveryState.Type.REPLICA));
        assertThat(nodeBShardResponse.recoveryState().getStage(), equalTo(RecoveryState.Stage.DONE));
        validateIndexRecoveryState(nodeBShardResponse.recoveryState().getIndex());
    }

    @Test
    public void rerouteRecoveryTest() throws Exception {
        logger.info("--> start node A");
        String nodeA = cluster().startNode(settingsBuilder().put("gateway.type", "local"));

        logger.info("--> create index on node: {}", nodeA);
        createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT, REPLICA_COUNT);

        logger.info("--> start node B");
        String nodeB = cluster().startNode(settingsBuilder().put("gateway.type", "local"));
        ensureGreen();

        logger.info("--> move shard from: {} to: {}", nodeA, nodeB);
        client().admin().cluster().prepareReroute()
                .add(new MoveAllocationCommand(new ShardId(INDEX_NAME, 0), nodeA, nodeB))
                .execute().actionGet().getState();

        ensureGreen();

        logger.info("--> request recoveries");
        RecoveryResponse response = client().admin().indices().prepareRecoveries(INDEX_NAME).execute().actionGet();

        List<ShardRecoveryResponse> shardResponses = response.shardResponses().get(INDEX_NAME);
        assertThat(shardResponses.size(), equalTo(1));

        ShardRecoveryResponse shardResponse = shardResponses.get(0);
        RecoveryState state = shardResponse.recoveryState();

        assertThat(state.getType(), equalTo(RecoveryState.Type.RELOCATION));
        assertThat(state.getStage(), equalTo(RecoveryState.Stage.DONE));
        assertThat(nodeA, equalTo(state.getSourceNode().getName()));
        assertThat(nodeB, equalTo(state.getTargetNode().getName()));
        assertNull(state.getRestoreSource());
        validateIndexRecoveryState(state.getIndex());
    }

    @Test
    public void snapshotRecoveryTest() throws Exception {
        logger.info("--> start node A");
        String nodeA = cluster().startNode(settingsBuilder().put("gateway.type", "local"));

        logger.info("--> create repository");
        assertAcked(client().admin().cluster().preparePutRepository(REPO_NAME)
                .setType("fs").setSettings(ImmutableSettings.settingsBuilder()
                                .put("location", newTempDir(LifecycleScope.SUITE))
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

                assertThat(shardResponse.recoveryState().getType(), equalTo(RecoveryState.Type.SNAPSHOT));
                assertThat(shardResponse.recoveryState().getStage(), equalTo(RecoveryState.Stage.DONE));
                assertNotNull(shardResponse.recoveryState().getRestoreSource());
                assertThat(shardResponse.recoveryState().getTargetNode().getName(), equalTo(nodeA));
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
                .put("number_of_replicas", replicaCount)));
        ensureGreen();

        logger.info("--> indexing sample data");
        final int numDocs = between(MIN_DOC_COUNT, MAX_DOC_COUNT);
        final IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];

        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex(INDEX_NAME, INDEX_TYPE).
                    setSource("foo-int-" + i, randomInt(),
                              "foo-string-" + i, randomAsciiOfLength(32),
                              "foo-float-" + i, randomFloat());
        }

        indexRandom(true, docs);
        flush();
        assertThat(client().prepareCount(INDEX_NAME).get().getCount(), equalTo((long) numDocs));
        return client().admin().indices().prepareStats(INDEX_NAME).execute().actionGet();
    }

    private void validateIndexRecoveryState(RecoveryState.Index indexState) {
        assertThat(indexState.time(), greaterThanOrEqualTo(0L));
        assertThat(indexState.percentFilesRecovered(), greaterThanOrEqualTo(0.0f));
        assertThat(indexState.percentFilesRecovered(), lessThanOrEqualTo(100.0f));
        assertThat(indexState.percentBytesRecovered(), greaterThanOrEqualTo(0.0f));
        assertThat(indexState.percentBytesRecovered(), lessThanOrEqualTo(100.0f));
    }
}
