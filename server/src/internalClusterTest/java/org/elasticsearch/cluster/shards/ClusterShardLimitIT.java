/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.shards;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.NodeRoles.dataNode;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class ClusterShardLimitIT extends ESIntegTestCase {
    private static final String shardsPerNodeKey = ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey();

    public void testSettingClusterMaxShards() {
        int shardsPerNode = between(1, 500_000);
        setShardsPerNode(shardsPerNode);
    }

    public void testMinimumPerNode() {
        int negativeShardsPerNode = between(-50_000, 0);
        try {
            if (frequently()) {
                clusterAdmin().prepareUpdateSettings()
                    .setPersistentSettings(Settings.builder().put(shardsPerNodeKey, negativeShardsPerNode).build())
                    .get();
            } else {
                clusterAdmin().prepareUpdateSettings()
                    .setPersistentSettings(Settings.builder().put(shardsPerNodeKey, negativeShardsPerNode).build())
                    .get();
            }
            fail("should not be able to set negative shards per node");
        } catch (IllegalArgumentException ex) {
            assertEquals(
                "Failed to parse value [" + negativeShardsPerNode + "] for setting [cluster.max_shards_per_node] must be >= 1",
                ex.getMessage()
            );
        }
    }

    public void testIndexCreationOverLimit() {
        int dataNodes = clusterAdmin().prepareState().get().getState().getNodes().getDataNodes().size();

        ShardCounts counts = ShardCounts.forDataNodeCount(dataNodes);

        setShardsPerNode(counts.getShardsPerNode());

        // Create an index that will bring us up to the limit
        createIndex(
            "test",
            Settings.builder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_SHARDS, counts.getFirstIndexShards())
                .put(SETTING_NUMBER_OF_REPLICAS, counts.getFirstIndexReplicas())
                .build()
        );

        try {
            prepareCreate(
                "should-fail",
                Settings.builder()
                    .put(indexSettings())
                    .put(SETTING_NUMBER_OF_SHARDS, counts.getFailingIndexShards())
                    .put(SETTING_NUMBER_OF_REPLICAS, counts.getFailingIndexReplicas())
            ).get();
            fail("Should not have been able to go over the limit");
        } catch (IllegalArgumentException e) {
            verifyException(dataNodes, counts, e);
        }
        ClusterState clusterState = clusterAdmin().prepareState().get().getState();
        assertFalse(clusterState.getMetadata().hasIndex("should-fail"));
    }

    public void testIndexCreationOverLimitFromTemplate() {
        int dataNodes = clusterAdmin().prepareState().get().getState().getNodes().getDataNodes().size();

        final ShardCounts counts = ShardCounts.forDataNodeCount(dataNodes);

        setShardsPerNode(counts.getShardsPerNode());

        if (counts.getFirstIndexShards() > 0) {
            createIndex(
                "test",
                Settings.builder()
                    .put(indexSettings())
                    .put(SETTING_NUMBER_OF_SHARDS, counts.getFirstIndexShards())
                    .put(SETTING_NUMBER_OF_REPLICAS, counts.getFirstIndexReplicas())
                    .build()
            );
        }

        assertAcked(
            indicesAdmin().preparePutTemplate("should-fail")
                .setPatterns(Collections.singletonList("should-fail"))
                .setOrder(1)
                .setSettings(indexSettings(counts.getFailingIndexShards(), counts.getFailingIndexReplicas()))
                .get()
        );

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> indicesAdmin().prepareCreate("should-fail").get()
        );
        verifyException(dataNodes, counts, e);
        ClusterState clusterState = clusterAdmin().prepareState().get().getState();
        assertFalse(clusterState.getMetadata().hasIndex("should-fail"));
    }

    public void testIncreaseReplicasOverLimit() {
        int dataNodes = clusterAdmin().prepareState().get().getState().getNodes().getDataNodes().size();

        dataNodes = ensureMultipleDataNodes(dataNodes);

        int firstShardCount = between(2, 10);
        int shardsPerNode = firstShardCount - 1;
        setShardsPerNode(shardsPerNode);

        prepareCreate("growing-should-fail", indexSettings(firstShardCount, 0)).get();

        try {
            indicesAdmin().prepareUpdateSettings("growing-should-fail")
                .setSettings(Settings.builder().put("number_of_replicas", dataNodes))
                .get();
            fail("shouldn't be able to increase the number of replicas");
        } catch (IllegalArgumentException e) {
            String expectedError = "Validation Failed: 1: this action would add ["
                + (dataNodes * firstShardCount)
                + "] shards, but this cluster currently has ["
                + firstShardCount
                + "]/["
                + dataNodes * shardsPerNode
                + "] maximum normal shards open;";
            assertEquals(expectedError, e.getMessage());
        }
        Metadata clusterState = clusterAdmin().prepareState().get().getState().metadata();
        assertEquals(0, clusterState.index("growing-should-fail").getNumberOfReplicas());
    }

    public void testChangingMultipleIndicesOverLimit() {
        int dataNodes = clusterAdmin().prepareState().get().getState().getNodes().getDataNodes().size();

        dataNodes = ensureMultipleDataNodes(dataNodes);

        // Create two indexes: One that ends up with fewer shards, and one
        // that ends up with more to verify that we check the _total_ number of
        // shards the operation would add.

        int firstIndexFactor = between(5, 10);
        int firstIndexShards = firstIndexFactor * dataNodes;
        int firstIndexReplicas = 0;

        int secondIndexFactor = between(1, 3);
        int secondIndexShards = secondIndexFactor * dataNodes;
        int secondIndexReplicas = dataNodes;

        int shardsPerNode = firstIndexFactor + (secondIndexFactor * (1 + secondIndexReplicas));
        setShardsPerNode(shardsPerNode);

        createIndex(
            "test-1-index",
            Settings.builder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_SHARDS, firstIndexShards)
                .put(SETTING_NUMBER_OF_REPLICAS, firstIndexReplicas)
                .build()
        );
        createIndex(
            "test-2-index",
            Settings.builder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_SHARDS, secondIndexShards)
                .put(SETTING_NUMBER_OF_REPLICAS, secondIndexReplicas)
                .build()
        );
        try {
            indicesAdmin().prepareUpdateSettings(randomFrom("_all", "test-*", "*-index"))
                .setSettings(Settings.builder().put("number_of_replicas", dataNodes - 1))
                .get();
            fail("should not have been able to increase shards above limit");
        } catch (IllegalArgumentException e) {
            int totalShardsBefore = (firstIndexShards * (1 + firstIndexReplicas)) + (secondIndexShards * (1 + secondIndexReplicas));
            int totalShardsAfter = (dataNodes) * (firstIndexShards + secondIndexShards);
            int difference = totalShardsAfter - totalShardsBefore;

            String expectedError = "Validation Failed: 1: this action would add ["
                + difference
                + "] shards, but this cluster currently has ["
                + totalShardsBefore
                + "]/["
                + dataNodes * shardsPerNode
                + "] maximum normal shards open;";
            assertEquals(expectedError, e.getMessage());
        }
        Metadata clusterState = clusterAdmin().prepareState().get().getState().metadata();
        assertEquals(firstIndexReplicas, clusterState.index("test-1-index").getNumberOfReplicas());
        assertEquals(secondIndexReplicas, clusterState.index("test-2-index").getNumberOfReplicas());
    }

    public void testPreserveExistingSkipsCheck() {
        int dataNodes = clusterAdmin().prepareState().get().getState().getNodes().getDataNodes().size();

        dataNodes = ensureMultipleDataNodes(dataNodes);

        int firstShardCount = between(2, 10);
        int shardsPerNode = firstShardCount - 1;
        setShardsPerNode(shardsPerNode);

        prepareCreate(
            "test-index",
            Settings.builder().put(indexSettings()).put(SETTING_NUMBER_OF_SHARDS, firstShardCount).put(SETTING_NUMBER_OF_REPLICAS, 0)
        ).get();

        // Since a request with preserve_existing can't change the number of
        // replicas, we should never get an error here.
        assertAcked(
            indicesAdmin().prepareUpdateSettings("test-index")
                .setPreserveExisting(true)
                .setSettings(Settings.builder().put("number_of_replicas", dataNodes))
                .get()
        );
        ClusterState clusterState = clusterAdmin().prepareState().get().getState();
        assertEquals(0, clusterState.getMetadata().index("test-index").getNumberOfReplicas());
    }

    public void testRestoreSnapshotOverLimit() {
        Client client = client();

        logger.info("-->  creating repository");
        Settings.Builder repoSettings = Settings.builder();
        repoSettings.put("location", randomRepoPath());
        repoSettings.put("compress", randomBoolean());
        repoSettings.put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES);

        assertAcked(client.admin().cluster().preparePutRepository("test-repo").setType("fs").setSettings(repoSettings.build()));

        int dataNodes = clusterAdmin().prepareState().get().getState().getNodes().getDataNodes().size();
        ShardCounts counts = ShardCounts.forDataNodeCount(dataNodes);
        createIndex(
            "snapshot-index",
            Settings.builder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_SHARDS, counts.getFailingIndexShards())
                .put(SETTING_NUMBER_OF_REPLICAS, counts.getFailingIndexReplicas())
                .build()
        );
        ensureGreen();

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIndices("snapshot-index")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        List<SnapshotInfo> snapshotInfos = client.admin()
            .cluster()
            .prepareGetSnapshots("test-repo")
            .setSnapshots("test-snap")
            .get()
            .getSnapshots();
        assertThat(snapshotInfos.size(), equalTo(1));
        SnapshotInfo snapshotInfo = snapshotInfos.get(0);
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.version(), equalTo(IndexVersion.current()));

        // Test restore after index deletion
        logger.info("--> delete indices");
        cluster().wipeIndices("snapshot-index");

        // Reduce the shard limit and fill it up
        setShardsPerNode(counts.getShardsPerNode());
        createIndex(
            "test-fill",
            Settings.builder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_SHARDS, counts.getFirstIndexShards())
                .put(SETTING_NUMBER_OF_REPLICAS, counts.getFirstIndexReplicas())
                .build()
        );

        logger.info("--> restore one index after deletion");
        try {
            RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
                .cluster()
                .prepareRestoreSnapshot("test-repo", "test-snap")
                .setWaitForCompletion(true)
                .setIndices("snapshot-index")
                .execute()
                .actionGet();
            fail("Should not have been able to restore snapshot in full cluster");
        } catch (IllegalArgumentException e) {
            verifyException(dataNodes, counts, e);
        }
        ensureGreen();
        ClusterState clusterState = client.admin().cluster().prepareState().get().getState();
        assertFalse(clusterState.getMetadata().hasIndex("snapshot-index"));
    }

    public void testOpenIndexOverLimit() {
        Client client = client();
        int dataNodes = clusterAdmin().prepareState().get().getState().getNodes().getDataNodes().size();
        ShardCounts counts = ShardCounts.forDataNodeCount(dataNodes);

        createIndex(
            "test-index-1",
            Settings.builder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_SHARDS, counts.getFailingIndexShards())
                .put(SETTING_NUMBER_OF_REPLICAS, counts.getFailingIndexReplicas())
                .build()
        );

        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertFalse(healthResponse.isTimedOut());

        AcknowledgedResponse closeIndexResponse = client.admin().indices().prepareClose("test-index-1").execute().actionGet();
        assertTrue(closeIndexResponse.isAcknowledged());

        // Fill up the cluster
        setShardsPerNode(counts.getShardsPerNode());
        createIndex(
            "test-fill",
            Settings.builder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_SHARDS, counts.getFirstIndexShards())
                .put(SETTING_NUMBER_OF_REPLICAS, counts.getFirstIndexReplicas())
                .build()
        );

        try {
            client.admin().indices().prepareOpen("test-index-1").execute().actionGet();
            fail("should not have been able to open index");
        } catch (IllegalArgumentException e) {
            verifyException(dataNodes, counts, e);
        }
        ClusterState clusterState = client.admin().cluster().prepareState().get().getState();
        assertFalse(clusterState.getMetadata().hasIndex("snapshot-index"));
    }

    private int ensureMultipleDataNodes(int dataNodes) {
        if (dataNodes == 1) {
            internalCluster().startNode(dataNode());
            assertThat(
                clusterAdmin().prepareHealth()
                    .setWaitForEvents(Priority.LANGUID)
                    .setWaitForNodes(">=2")
                    .setLocal(true)
                    .execute()
                    .actionGet()
                    .isTimedOut(),
                equalTo(false)
            );
            dataNodes = clusterAdmin().prepareState().get().getState().getNodes().getDataNodes().size();
        }
        return dataNodes;
    }

    private void setShardsPerNode(int shardsPerNode) {
        try {
            ClusterUpdateSettingsResponse response = clusterAdmin().prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put(shardsPerNodeKey, shardsPerNode).build())
                .get();
            assertEquals(shardsPerNode, response.getPersistentSettings().getAsInt(shardsPerNodeKey, -1).intValue());
        } catch (IllegalArgumentException ex) {
            fail(ex.getMessage());
        }
    }

    private void verifyException(int dataNodes, ShardCounts counts, IllegalArgumentException e) {
        int totalShards = counts.getFailingIndexShards() * (1 + counts.getFailingIndexReplicas());
        int currentShards = counts.getFirstIndexShards() * (1 + counts.getFirstIndexReplicas());
        int maxShards = counts.getShardsPerNode() * dataNodes;
        String expectedError = "Validation Failed: 1: this action would add ["
            + totalShards
            + "] shards, but this cluster currently has ["
            + currentShards
            + "]/["
            + maxShards
            + "] maximum normal shards open;";
        assertEquals(expectedError, e.getMessage());
    }

}
