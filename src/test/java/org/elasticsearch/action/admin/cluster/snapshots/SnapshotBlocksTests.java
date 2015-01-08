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
package org.elasticsearch.action.admin.cluster.snapshots;

import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.junit.Before;
import org.junit.Test;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * This class tests that snapshot operations (Create, Delete, Restore) are blocked when the cluster is read-only.
 *
 * The @ClusterScope TEST is needed because this class updates the cluster setting "cluster.blocks.read_only".
 */
@ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST)
public class SnapshotBlocksTests extends ElasticsearchIntegrationTest {

    protected static final String INDEX_NAME = "test-blocks";
    protected static final String REPOSITORY_NAME = "repo-" + INDEX_NAME;
    protected static final String SNAPSHOT_NAME = "snapshot-0";

    @Before
    protected void setUpRepository() throws Exception {
        createIndex(INDEX_NAME);

        int docs = between(10, 100);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(INDEX_NAME, "type").setSource("test", "init").execute().actionGet();
        }

        logger.info("--> register a repository");
        assertAcked(client().admin().cluster().preparePutRepository(REPOSITORY_NAME)
                .setType("fs")
                .setSettings(ImmutableSettings.settingsBuilder().put("location",  createTempDir())));

        logger.info("--> verify the repository");
        VerifyRepositoryResponse verifyResponse = client().admin().cluster().prepareVerifyRepository(REPOSITORY_NAME).get();
        assertThat(verifyResponse.getNodes().length, equalTo(cluster().numDataAndMasterNodes()));

        logger.info("--> create a snapshot");
        CreateSnapshotResponse snapshotResponse = client().admin().cluster().prepareCreateSnapshot(REPOSITORY_NAME, SNAPSHOT_NAME)
                                                                            .setIncludeGlobalState(true)
                                                                            .setWaitForCompletion(true)
                                                                            .execute().actionGet();
        assertThat(snapshotResponse.status(), equalTo(RestStatus.OK));
        ensureSearchable();
    }

    @Test
    public void testCreateSnapshotWithBlocks() {
        logger.info("-->  creating a snapshot is blocked when the cluster is read only");
        try {
            setClusterReadOnly(true);
            assertBlocked(client().admin().cluster().prepareCreateSnapshot(REPOSITORY_NAME, "snapshot-1"), MetaData.CLUSTER_READ_ONLY_BLOCK);
        } finally {
            setClusterReadOnly(false);
        }

        logger.info("-->  creating a snapshot is allowed when the cluster is not read only");
        CreateSnapshotResponse response = client().admin().cluster().prepareCreateSnapshot(REPOSITORY_NAME, "snapshot-1")
                .setWaitForCompletion(true)
                .execute().actionGet();
        assertThat(response.status(), equalTo(RestStatus.OK));
    }

    @Test
    public void testDeleteSnapshotWithBlocks() {
        logger.info("-->  deleting a snapshot is blocked when the cluster is read only");
        try {
            setClusterReadOnly(true);
            assertBlocked(client().admin().cluster().prepareDeleteSnapshot(REPOSITORY_NAME, SNAPSHOT_NAME), MetaData.CLUSTER_READ_ONLY_BLOCK);
        } finally {
            setClusterReadOnly(false);
        }

        logger.info("-->  deleting a snapshot is allowed when the cluster is not read only");
        DeleteSnapshotResponse response = client().admin().cluster().prepareDeleteSnapshot(REPOSITORY_NAME, SNAPSHOT_NAME).execute().actionGet();
        assertThat(response.isAcknowledged(), equalTo(true));
    }

    @Test
    public void testRestoreSnapshotWithBlocks() {
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME));
        assertFalse(client().admin().indices().prepareExists(INDEX_NAME).get().isExists());

        logger.info("-->  restoring a snapshot is blocked when the cluster is read only");
        try {
            setClusterReadOnly(true);
            assertBlocked(client().admin().cluster().prepareRestoreSnapshot(REPOSITORY_NAME, SNAPSHOT_NAME), MetaData.CLUSTER_READ_ONLY_BLOCK);
        } finally {
            setClusterReadOnly(false);
        }

        logger.info("-->  creating a snapshot is allowed when the cluster is not read only");
        RestoreSnapshotResponse response = client().admin().cluster().prepareRestoreSnapshot(REPOSITORY_NAME, SNAPSHOT_NAME)
                .setWaitForCompletion(true)
                .execute().actionGet();
        assertThat(response.status(), equalTo(RestStatus.OK));
        assertTrue(client().admin().indices().prepareExists(INDEX_NAME).get().isExists());
    }

    @Test
    public void testGetSnapshotWithBlocks() {
        // This test checks that the Get Snapshot operation is never blocked, even if the cluster is read only.
        try {
            setClusterReadOnly(true);
            GetSnapshotsResponse response = client().admin().cluster().prepareGetSnapshots(REPOSITORY_NAME).execute().actionGet();
            assertThat(response.getSnapshots(), hasSize(1));
            assertThat(response.getSnapshots().get(0).name(), equalTo(SNAPSHOT_NAME));
        } finally {
            setClusterReadOnly(false);
        }
    }

    @Test
    public void testSnapshotStatusWithBlocks() {
        // This test checks that the Snapshot Status operation is never blocked, even if the cluster is read only.
        try {
            setClusterReadOnly(true);
            SnapshotsStatusResponse response = client().admin().cluster().prepareSnapshotStatus(REPOSITORY_NAME)
                    .setSnapshots(SNAPSHOT_NAME)
                    .execute().actionGet();
            assertThat(response.getSnapshots(), hasSize(1));
            assertThat(response.getSnapshots().get(0).getState().completed(), equalTo(true));
        } finally {
            setClusterReadOnly(false);
        }
    }
}
