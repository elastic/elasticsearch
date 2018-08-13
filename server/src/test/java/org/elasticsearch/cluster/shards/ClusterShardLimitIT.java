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


package org.elasticsearch.cluster.shards;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.List;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class ClusterShardLimitIT extends ESIntegTestCase {
    private static final String shardsPerNodeKey = MetaData.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey();

    public void testSettingClusterMaxShards() {
        int shardsPerNode = between(1, 500_000);
        try {
            ClusterUpdateSettingsResponse response = client().admin().cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(shardsPerNodeKey, shardsPerNode).build())
                .get();
            assertEquals(shardsPerNode, response.getTransientSettings().getAsInt(shardsPerNodeKey, -1).intValue());
        } catch (IllegalArgumentException ex) {
            fail(ex.getMessage());
        }
    }

    public void testIndexCreationOverLimit() {
        int dataNodes = client().admin().cluster().prepareState().get().getState().getNodes().getDataNodes().size();

        ShardCounts counts = ShardCounts.forDataNodeCount(dataNodes);

        setShardsPerNode(counts.getShardsPerNode());

        // Create an index that will bring us up to the limit
        createIndex("test", Settings.builder().put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, counts.getFirstIndexShards())
            .put(SETTING_NUMBER_OF_REPLICAS, counts.getFirstIndexReplicas()).build());

        try {
            prepareCreate("should-fail", Settings.builder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_SHARDS, counts.getFailingIndexShards())
                .put(SETTING_NUMBER_OF_REPLICAS, counts.getFailingIndexReplicas())).get();
            fail("Should not have been able to go over the limit");
        } catch (IllegalArgumentException e) {
            int totalShards = counts.getFailingIndexShards() * (1 + counts.getFailingIndexReplicas());
            int currentShards = counts.getFirstIndexShards() * (1 + counts.getFirstIndexReplicas());
            int maxShards = counts.getShardsPerNode() * dataNodes;
            String expectedError = "Validation Failed: 1: creating [should-fail] would create " + totalShards
                + " total shards, but this cluster currently has " + currentShards + "/" + maxShards + " maximum shards open;";
            assertEquals(expectedError, e.getMessage());
        }
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        assertFalse(clusterState.getMetaData().hasIndex("should-fail"));
    }

    public void testRestoreSnapshotOverLimit() {
        Client client = client();

        logger.info("-->  creating repository");
        Settings.Builder repoSettings = Settings.builder();
        repoSettings.put("location", randomRepoPath());
        repoSettings.put("compress", randomBoolean());
        repoSettings.put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES);

        assertAcked(client.admin().cluster().preparePutRepository("test-repo").setType("fs").setSettings(repoSettings.build()));

        int dataNodes = client().admin().cluster().prepareState().get().getState().getNodes().getDataNodes().size();
        ShardCounts counts = ShardCounts.forDataNodeCount(dataNodes);
        createIndex("snapshot-index", Settings.builder().put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, counts.getFailingIndexShards())
            .put(SETTING_NUMBER_OF_REPLICAS, counts.getFailingIndexReplicas()).build());
        ensureGreen();

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster()
            .prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIndices("snapshot-index").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        List<SnapshotInfo> snapshotInfos = client.admin().cluster().prepareGetSnapshots("test-repo")
            .setSnapshots("test-snap").get().getSnapshots();
        assertThat(snapshotInfos.size(), equalTo(1));
        SnapshotInfo snapshotInfo = snapshotInfos.get(0);
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.version(), equalTo(Version.CURRENT));

        // Test restore after index deletion
        logger.info("--> delete indices");
        cluster().wipeIndices("snapshot-index");

        // Reduce the shard limit and fill it up
        setShardsPerNode(counts.getShardsPerNode());
        createIndex("test-fill", Settings.builder().put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, counts.getFirstIndexShards())
            .put(SETTING_NUMBER_OF_REPLICAS, counts.getFirstIndexReplicas()).build());

        logger.info("--> restore one index after deletion");
        try {
            RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster()
                .prepareRestoreSnapshot("test-repo", "test-snap")
                .setWaitForCompletion(true).setIndices("snapshot-index").execute().actionGet();
            fail("Should not have been able to restore snapshot in full cluster");
        } catch (IllegalArgumentException e) {
            int totalShards = counts.getFailingIndexShards() * (1 + counts.getFailingIndexReplicas());
            int currentShards = counts.getFirstIndexShards() * (1 + counts.getFirstIndexReplicas());
            int maxShards = counts.getShardsPerNode() * dataNodes;
            String expectedError = "Validation Failed: 1: creating [snapshot-index] would create " + totalShards
                + " total shards, but this cluster currently has " + currentShards + "/" + maxShards + " maximum shards open;";
            assertEquals(expectedError, e.getMessage());
        }
        ensureGreen();
        ClusterState clusterState = client.admin().cluster().prepareState().get().getState();
        assertFalse(clusterState.getMetaData().hasIndex("snapshot-index"));
    }

    public void testOpenIndexOverLimit() {
        Client client = client();
        int dataNodes = client().admin().cluster().prepareState().get().getState().getNodes().getDataNodes().size();
        ShardCounts counts = ShardCounts.forDataNodeCount(dataNodes);

        createIndex("test-index-1", Settings.builder().put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, counts.getFailingIndexShards())
            .put(SETTING_NUMBER_OF_REPLICAS, counts.getFailingIndexReplicas()).build());

        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertFalse(healthResponse.isTimedOut());

        CloseIndexResponse closeIndexResponse = client.admin().indices().prepareClose("test-index-1").execute().actionGet();
        assertTrue(closeIndexResponse.isAcknowledged());

        // Fill up the cluster
        setShardsPerNode(counts.getShardsPerNode());
        createIndex("test-fill", Settings.builder().put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, counts.getFirstIndexShards())
            .put(SETTING_NUMBER_OF_REPLICAS, counts.getFirstIndexReplicas()).build());


        try {
            client.admin().indices().prepareOpen("test-index-1").execute().actionGet();
            fail("should not have been able to open index");
        } catch (IllegalArgumentException e) {
            int totalShards = counts.getFailingIndexShards() * (1 + counts.getFailingIndexReplicas());
            int currentShards = counts.getFirstIndexShards() * (1 + counts.getFirstIndexReplicas());
            int maxShards = counts.getShardsPerNode() * dataNodes;
            String expectedError = "Validation Failed: 1: opening [test-index-1] would open " + totalShards
                + " total shards, but this cluster currently has " + currentShards + "/" + maxShards + " maximum shards open;";
            assertEquals(expectedError, e.getMessage());
        }
        ClusterState clusterState = client.admin().cluster().prepareState().get().getState();
        assertFalse(clusterState.getMetaData().hasIndex("snapshot-index"));
    }

    private void setShardsPerNode(int shardsPerNode) {
        try {
            ClusterUpdateSettingsResponse response = client().admin().cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(shardsPerNodeKey, shardsPerNode).build())
                .get();
            assertEquals(shardsPerNode, response.getTransientSettings().getAsInt(shardsPerNodeKey, -1).intValue());
        } catch (IllegalArgumentException ex) {
            fail(ex.getMessage());
        }
    }

    private static class ShardCounts {
        private final int shardsPerNode;

        private final int firstIndexShards;
        private final int firstIndexReplicas;

        private final int failingIndexShards;
        private final int failingIndexReplicas;

        private ShardCounts(int shardsPerNode,
                            int firstIndexShards,
                            int firstIndexReplicas,
                            int failingIndexShards,
                            int failingIndexReplicas) {
            this.shardsPerNode = shardsPerNode;
            this.firstIndexShards = firstIndexShards;
            this.firstIndexReplicas = firstIndexReplicas;
            this.failingIndexShards = failingIndexShards;
            this.failingIndexReplicas = failingIndexReplicas;
        }

        static ShardCounts forDataNodeCount(int dataNodes) {
            int mainIndexReplicas = between(0, dataNodes - 1);
            int mainIndexShards = between(1, 10);
            int totalShardsInIndex = (mainIndexReplicas + 1) * mainIndexShards;
            int shardsPerNode = (int) Math.ceil((double) totalShardsInIndex / dataNodes);
            int totalCap = shardsPerNode * dataNodes;

            int failingIndexShards;
            int failingIndexReplicas;
            if (dataNodes > 1 && frequently()) {
                failingIndexShards = Math.max(1, totalCap - totalShardsInIndex);
                failingIndexReplicas = between(1, dataNodes - 1);
            } else {
                failingIndexShards = totalCap - totalShardsInIndex + between(1, 10);
                failingIndexReplicas = 0;
            }

            return new ShardCounts(shardsPerNode, mainIndexShards, mainIndexReplicas, failingIndexShards, failingIndexReplicas);
        }

        int getShardsPerNode() {
            return shardsPerNode;
        }

        int getFirstIndexShards() {
            return firstIndexShards;
        }

        int getFirstIndexReplicas() {
            return firstIndexReplicas;
        }

        int getFailingIndexShards() {
            return failingIndexShards;
        }

        int getFailingIndexReplicas() {
            return failingIndexReplicas;
        }
    }
}
