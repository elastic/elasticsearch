/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.snapshots;

import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.junit.Before;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_READ;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * This class tests that snapshot operations (Create, Delete, Restore) are blocked when the cluster is read-only.
 *
 * The @NodeScope TEST is needed because this class updates the cluster setting "cluster.blocks.read_only".
 */
@ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class SnapshotBlocksIT extends ESIntegTestCase {

    protected static final String INDEX_NAME = "test-blocks-1";
    protected static final String OTHER_INDEX_NAME = "test-blocks-2";
    protected static final String COMMON_INDEX_NAME_MASK = "test-blocks-*";
    protected static final String REPOSITORY_NAME = "repo-" + INDEX_NAME;
    protected static final String SNAPSHOT_NAME = "snapshot-0";

    @Before
    protected void setUpRepository() throws Exception {
        createIndex(INDEX_NAME, OTHER_INDEX_NAME);

        int docs = between(10, 100);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(INDEX_NAME).setSource("test", "init").execute().actionGet();
        }
        docs = between(10, 100);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(OTHER_INDEX_NAME).setSource("test", "init").execute().actionGet();
        }

        logger.info("--> register a repository");

        assertAcked(
            clusterAdmin().preparePutRepository(REPOSITORY_NAME)
                .setType("fs")
                .setSettings(Settings.builder().put("location", randomRepoPath()))
        );

        logger.info("--> verify the repository");
        VerifyRepositoryResponse verifyResponse = clusterAdmin().prepareVerifyRepository(REPOSITORY_NAME).get();
        assertThat(verifyResponse.getNodes().size(), equalTo(cluster().numDataAndMasterNodes()));

        logger.info("--> create a snapshot");
        CreateSnapshotResponse snapshotResponse = clusterAdmin().prepareCreateSnapshot(REPOSITORY_NAME, SNAPSHOT_NAME)
            .setIncludeGlobalState(true)
            .setWaitForCompletion(true)
            .execute()
            .actionGet();
        assertThat(snapshotResponse.status(), equalTo(RestStatus.OK));
        ensureSearchable();
    }

    public void testCreateSnapshotWithBlocks() {
        logger.info("-->  creating a snapshot is allowed when the cluster is read only");
        try {
            setClusterReadOnly(true);
            assertThat(
                clusterAdmin().prepareCreateSnapshot(REPOSITORY_NAME, "snapshot-1").setWaitForCompletion(true).get().status(),
                equalTo(RestStatus.OK)
            );
        } finally {
            setClusterReadOnly(false);
        }

        logger.info("-->  creating a snapshot is allowed when the cluster is not read only");
        CreateSnapshotResponse response = clusterAdmin().prepareCreateSnapshot(REPOSITORY_NAME, "snapshot-2")
            .setWaitForCompletion(true)
            .execute()
            .actionGet();
        assertThat(response.status(), equalTo(RestStatus.OK));
    }

    public void testCreateSnapshotWithIndexBlocks() {
        logger.info("-->  creating a snapshot is not blocked when an index is read only");
        try {
            enableIndexBlock(INDEX_NAME, SETTING_READ_ONLY);
            assertThat(
                clusterAdmin().prepareCreateSnapshot(REPOSITORY_NAME, "snapshot-1")
                    .setIndices(COMMON_INDEX_NAME_MASK)
                    .setWaitForCompletion(true)
                    .get()
                    .status(),
                equalTo(RestStatus.OK)
            );
        } finally {
            disableIndexBlock(INDEX_NAME, SETTING_READ_ONLY);
        }

        logger.info("-->  creating a snapshot is blocked when an index is blocked for reads");
        try {
            enableIndexBlock(INDEX_NAME, SETTING_BLOCKS_READ);
            assertThat(
                clusterAdmin().prepareCreateSnapshot(REPOSITORY_NAME, "snapshot-2")
                    .setIndices(COMMON_INDEX_NAME_MASK)
                    .setWaitForCompletion(true)
                    .get()
                    .status(),
                equalTo(RestStatus.OK)
            );
        } finally {
            disableIndexBlock(INDEX_NAME, SETTING_BLOCKS_READ);
        }
    }

    public void testDeleteSnapshotWithBlocks() {
        logger.info("-->  deleting a snapshot is allowed when the cluster is read only");
        try {
            setClusterReadOnly(true);
            assertTrue(clusterAdmin().prepareDeleteSnapshot(REPOSITORY_NAME, SNAPSHOT_NAME).get().isAcknowledged());
        } finally {
            setClusterReadOnly(false);
        }
    }

    public void testRestoreSnapshotWithBlocks() {
        assertAcked(indicesAdmin().prepareDelete(INDEX_NAME, OTHER_INDEX_NAME));
        assertFalse(indexExists(INDEX_NAME));
        assertFalse(indexExists(OTHER_INDEX_NAME));

        logger.info("-->  restoring a snapshot is blocked when the cluster is read only");
        try {
            setClusterReadOnly(true);
            assertBlocked(clusterAdmin().prepareRestoreSnapshot(REPOSITORY_NAME, SNAPSHOT_NAME), Metadata.CLUSTER_READ_ONLY_BLOCK);
        } finally {
            setClusterReadOnly(false);
        }

        logger.info("-->  creating a snapshot is allowed when the cluster is not read only");
        RestoreSnapshotResponse response = clusterAdmin().prepareRestoreSnapshot(REPOSITORY_NAME, SNAPSHOT_NAME)
            .setWaitForCompletion(true)
            .execute()
            .actionGet();
        assertThat(response.status(), equalTo(RestStatus.OK));
        assertTrue(indexExists(INDEX_NAME));
        assertTrue(indexExists(OTHER_INDEX_NAME));
    }

    public void testGetSnapshotWithBlocks() {
        // This test checks that the Get Snapshot operation is never blocked, even if the cluster is read only.
        try {
            setClusterReadOnly(true);
            GetSnapshotsResponse response = clusterAdmin().prepareGetSnapshots(REPOSITORY_NAME).execute().actionGet();
            assertThat(response.getSnapshots(), hasSize(1));
            assertThat(response.getSnapshots().get(0).snapshotId().getName(), equalTo(SNAPSHOT_NAME));
        } finally {
            setClusterReadOnly(false);
        }
    }

    public void testSnapshotStatusWithBlocks() {
        // This test checks that the Snapshot Status operation is never blocked, even if the cluster is read only.
        try {
            setClusterReadOnly(true);
            SnapshotsStatusResponse response = clusterAdmin().prepareSnapshotStatus(REPOSITORY_NAME)
                .setSnapshots(SNAPSHOT_NAME)
                .execute()
                .actionGet();
            assertThat(response.getSnapshots(), hasSize(1));
            assertThat(response.getSnapshots().get(0).getState().completed(), equalTo(true));
        } finally {
            setClusterReadOnly(false);
        }
    }
}
