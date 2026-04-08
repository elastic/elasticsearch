/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.blobcache.BlobCachePlugin;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.action.support.master.MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT;
import static org.elasticsearch.test.NodeRoles.addRoles;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for {@link DLMConvertToFrozen#maybeMountSearchableSnapshot(String)}.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DLMConvertToFrozenMountSnapshotIT extends ESIntegTestCase {

    private static final String INDEX_NAME = "test-convert-to-frozen-mount";
    private static final String REPO_NAME = "test-repo";
    private XPackLicenseState licenseState;

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected boolean forceSingleDataPath() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(BlobCachePlugin.class);
        plugins.add(LocalStateSearchableSnapshots.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final Settings initialSettings = super.nodeSettings(nodeOrdinal, otherSettings);
        final Settings settings;
        if (DiscoveryNode.canContainData(otherSettings)) {
            settings = addRoles(initialSettings, Set.of(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE));
        } else {
            settings = initialSettings;
        }
        return Settings.builder()
            .put(settings)
            .put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.of(10, ByteSizeUnit.MB))
            .build();
    }

    @Before
    public void setupLicense() {
        licenseState = new XPackLicenseState(
            System::currentTimeMillis,
            new XPackLicenseStatus(License.OperationMode.ENTERPRISE, true, null)
        );
    }

    /**
     * Tests that maybeMountSearchableSnapshot successfully mounts a snapshot that was
     * previously taken, creating the expected mounted index.
     */
    public void testMountSearchableSnapshotSuccessfully() throws Exception {
        internalCluster().startNode();
        createIndex(
            INDEX_NAME,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen(INDEX_NAME);
        prepareIndex(INDEX_NAME).setSource("field", "value").get();
        refresh(INDEX_NAME);

        setupRepoAndIndexMetadata();

        // Take a snapshot with the expected DLM naming convention
        String snapshotName = DLMConvertToFrozen.snapshotName(INDEX_NAME);
        clusterAdmin().prepareCreateSnapshot(INFINITE_MASTER_NODE_TIMEOUT, REPO_NAME, snapshotName)
            .setIndices(INDEX_NAME)
            .setWaitForCompletion(true)
            .setIncludeGlobalState(false)
            .get();

        // Verify the snapshot was created successfully
        List<SnapshotInfo> snapshots = clusterAdmin().prepareGetSnapshots(INFINITE_MASTER_NODE_TIMEOUT, REPO_NAME)
            .setSnapshots(snapshotName)
            .get()
            .getSnapshots();
        assertThat(snapshots.size(), equalTo(1));
        assertThat(snapshots.getFirst().state(), equalTo(SnapshotState.SUCCESS));

        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            INDEX_NAME,
            Metadata.DEFAULT_PROJECT_ID,
            client(),
            internalCluster().clusterService(),
            licenseState,
            Clock.systemUTC()
        );

        converter.maybeMountSearchableSnapshot(INDEX_NAME);

        // Verify the mounted index exists and is green
        ClusterState clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        IndexMetadata mountedIndexMetadata = clusterState.projectState(Metadata.DEFAULT_PROJECT_ID).metadata().index(snapshotName);
        assertThat("Mounted index [" + snapshotName + "] should exist", mountedIndexMetadata, is(notNullValue()));
        ensureGreen(snapshotName);
    }

    /**
     * Tests that maybeMountSearchableSnapshot is idempotent: calling it a second time
     * after a successful mount should detect the already-mounted snapshot and skip without error.
     */
    public void testMountSearchableSnapshotIdempotent() throws Exception {
        internalCluster().startNode();
        createIndex(
            INDEX_NAME,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen(INDEX_NAME);
        prepareIndex(INDEX_NAME).setSource("field", "value").get();
        refresh(INDEX_NAME);

        setupRepoAndIndexMetadata();

        String snapshotName = DLMConvertToFrozen.snapshotName(INDEX_NAME);
        clusterAdmin().prepareCreateSnapshot(INFINITE_MASTER_NODE_TIMEOUT, REPO_NAME, snapshotName)
            .setIndices(INDEX_NAME)
            .setWaitForCompletion(true)
            .setIncludeGlobalState(false)
            .get();

        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            INDEX_NAME,
            Metadata.DEFAULT_PROJECT_ID,
            client(),
            internalCluster().clusterService(),
            licenseState,
            Clock.systemUTC()
        );

        // First call: mounts the snapshot
        converter.maybeMountSearchableSnapshot(INDEX_NAME);
        ensureGreen(snapshotName);

        // Second call: should detect the snapshot is already mounted and skip
        converter.maybeMountSearchableSnapshot(INDEX_NAME);

        // Verify the mounted index still exists
        ClusterState clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        IndexMetadata mountedIndexMetadata = clusterState.projectState(Metadata.DEFAULT_PROJECT_ID).metadata().index(snapshotName);
        assertThat("Mounted index [" + snapshotName + "] should still exist", mountedIndexMetadata, is(notNullValue()));
    }

    /**
     * Tests that maybeMountSearchableSnapshot throws when the original index has been deleted
     * (checkIfEligibleForConvertToFrozen fails).
     */
    public void testMountSearchableSnapshotThrowsWhenIndexDeleted() {
        internalCluster().startNode();
        createIndex(
            INDEX_NAME,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen(INDEX_NAME);

        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            INDEX_NAME,
            Metadata.DEFAULT_PROJECT_ID,
            client(),
            internalCluster().clusterService(),
            licenseState,
            Clock.systemUTC()
        );

        // Delete the index
        assertAcked(indicesAdmin().prepareDelete(INDEX_NAME));

        IndexNotFoundException exception = expectThrows(
            IndexNotFoundException.class,
            () -> converter.maybeMountSearchableSnapshot(INDEX_NAME)
        );
        assertThat(exception.getMessage(), containsString("no such index"));
    }

    /**
     * Sets up the snapshot repository and adds the required custom index metadata
     * so that {@link DLMConvertToFrozen#checkIfEligibleForConvertToFrozen()} passes.
     */
    private void setupRepoAndIndexMetadata() {
        // Create a snapshot repository
        Path repoPath = randomRepoPath();
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, REPO_NAME)
                .setType("fs")
                .setSettings(Settings.builder().put("location", repoPath))
        );

        // Add the required custom metadata to the index via a cluster state update
        ClusterService masterClusterService = internalCluster().clusterService(internalCluster().getMasterName());
        ClusterState currentState = masterClusterService.state();
        ProjectMetadata projectMetadata = currentState.metadata().getProject(Metadata.DEFAULT_PROJECT_ID);
        IndexMetadata indexMetadata = projectMetadata.index(INDEX_NAME);
        IndexMetadata updatedMetadata = IndexMetadata.builder(indexMetadata)
            .putCustom(
                DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY,
                Map.of(DataStreamLifecycleService.FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY, REPO_NAME)
            )
            .build();
        ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(projectMetadata).put(updatedMetadata, true);
        ClusterState newState = ClusterState.builder(currentState).putProjectMetadata(projectBuilder.build()).build();
        ClusterServiceUtils.setState(masterClusterService.getMasterService(), newState);
    }
}
