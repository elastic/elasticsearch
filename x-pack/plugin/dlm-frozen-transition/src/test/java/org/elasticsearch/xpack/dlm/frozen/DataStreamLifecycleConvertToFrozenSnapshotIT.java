/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.xcontent.XContentType;

import java.time.Clock;
import java.util.List;

import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.WRITE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class DataStreamLifecycleConvertToFrozenSnapshotIT extends AbstractSnapshotIntegTestCase {

    private static final String REPO_NAME = "test-repo";

    private XPackLicenseState licenseState;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        licenseState = new XPackLicenseState(
            System::currentTimeMillis,
            new XPackLicenseStatus(License.OperationMode.ENTERPRISE, true, null)
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(RepositoriesService.DEFAULT_REPOSITORY_SETTING.getKey(), REPO_NAME)
            .build();
    }

    public void testMaybeTakeSnapshotCreatesNewSnapshot() throws Exception {
        createRepository(REPO_NAME, "fs");
        String indexName = "test-index";
        createIndex(
            indexName,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        indexDoc(indexName);
        flushAndRefresh(indexName);

        // Add write block to make the index read-only (as required before snapshot)
        AcknowledgedResponse blockResp = client().admin().indices().addBlock(new AddIndexBlockRequest(WRITE, indexName)).actionGet();
        assertTrue(blockResp.isAcknowledged());

        ProjectState projectState = getProjectState();
        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(
            indexName,
            client(),
            projectState,
            licenseState,
            Clock.systemUTC()
        );

        converter.maybeTakeSnapshot(indexName);

        // Verify the snapshot was created in the repository
        String expectedSnapshotName = DataStreamLifecycleConvertToFrozen.snapshotName(indexName);
        GetSnapshotsResponse getSnapshotsResponse = clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, REPO_NAME)
            .setSnapshots(expectedSnapshotName)
            .get();

        List<SnapshotInfo> snapshots = getSnapshotsResponse.getSnapshots();
        assertThat(snapshots.size(), is(1));
        SnapshotInfo snapshotInfo = snapshots.get(0);
        assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.failedShards(), is(0));
        assertThat(snapshotInfo.snapshotId().getName(), is(expectedSnapshotName));
        assertTrue(snapshotInfo.indices().contains(indexName));
    }

    public void testMaybeTakeSnapshotSkipsWhenValidSnapshotAlreadyExists() throws Exception {
        createRepository(REPO_NAME, "fs");
        String indexName = "test-index-skip";
        createIndex(
            indexName,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        indexDoc(indexName);
        flushAndRefresh(indexName);

        AcknowledgedResponse blockResp = client().admin().indices().addBlock(new AddIndexBlockRequest(WRITE, indexName)).actionGet();
        assertTrue(blockResp.isAcknowledged());

        // First call — creates the snapshot
        ProjectState projectState = getProjectState();
        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(
            indexName,
            client(),
            projectState,
            licenseState,
            Clock.systemUTC()
        );
        converter.maybeTakeSnapshot(indexName);

        // Second call — should skip since the snapshot already exists and is valid
        // Get a fresh project state
        ProjectState projectState2 = getProjectState();
        DataStreamLifecycleConvertToFrozen converter2 = new DataStreamLifecycleConvertToFrozen(
            indexName,
            client(),
            projectState2,
            licenseState,
            Clock.systemUTC()
        );
        converter2.maybeTakeSnapshot(indexName);

        // Verify still only one snapshot exists
        String expectedSnapshotName = DataStreamLifecycleConvertToFrozen.snapshotName(indexName);
        GetSnapshotsResponse getSnapshotsResponse = clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, REPO_NAME)
            .setSnapshots(expectedSnapshotName)
            .get();
        assertThat(getSnapshotsResponse.getSnapshots().size(), is(1));
    }

    public void testMaybeTakeSnapshotWithMissingRepoThrows() {
        // Don't create a repository — configure a default repo that doesn't exist
        String indexName = "test-index-no-repo";
        createIndex(
            indexName,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );

        ProjectState projectState = getProjectState();
        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(
            indexName,
            client(),
            projectState,
            licenseState,
            Clock.systemUTC()
        );

        // The snapshot should fail because the repo doesn't exist
        expectThrows(Exception.class, () -> converter.maybeTakeSnapshot(indexName));
    }

    public void testSnapshotNameIsConsistent() {
        String indexName = "my-test-index";
        String snapshotName = DataStreamLifecycleConvertToFrozen.snapshotName(indexName);
        assertThat(snapshotName, is("dlm-frozen-my-test-index"));

        // Calling again should produce the same name (idempotent)
        assertThat(DataStreamLifecycleConvertToFrozen.snapshotName(indexName), equalTo(snapshotName));
    }

    public void testSnapshotContainsCorrectMetadata() throws Exception {
        createRepository(REPO_NAME, "fs");
        String indexName = "test-index-metadata";
        createIndex(
            indexName,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        indexDoc(indexName);
        flushAndRefresh(indexName);

        AcknowledgedResponse blockResp = client().admin().indices().addBlock(new AddIndexBlockRequest(WRITE, indexName)).actionGet();
        assertTrue(blockResp.isAcknowledged());

        ProjectState projectState = getProjectState();
        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(
            indexName,
            client(),
            projectState,
            licenseState,
            Clock.systemUTC()
        );
        converter.maybeTakeSnapshot(indexName);

        String expectedSnapshotName = DataStreamLifecycleConvertToFrozen.snapshotName(indexName);
        GetSnapshotsResponse response = clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, REPO_NAME)
            .setSnapshots(expectedSnapshotName)
            .get();

        SnapshotInfo snapshotInfo = response.getSnapshots().get(0);
        // Verify the snapshot does not include global state
        assertThat(snapshotInfo.includeGlobalState(), is(false));
        // Verify user metadata
        assertThat(snapshotInfo.userMetadata(), is(notNullValue()));
        assertThat(snapshotInfo.userMetadata().get("dlm-managed"), is(true));
    }

    private ProjectState getProjectState() {
        ClusterState clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        return clusterState.projectState(Metadata.DEFAULT_PROJECT_ID);
    }

    private void indexDoc(String indexName) {
        client().index(new IndexRequest(indexName).source("{\"field\": \"value\"}", XContentType.JSON)).actionGet();
    }
}
