/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockRequest;
import org.elasticsearch.action.admin.indices.readonly.TransportAddIndexBlockAction;
import org.elasticsearch.action.admin.indices.readonly.TransportVerifyShardIndexBlockAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.Before;

import java.nio.file.Path;
import java.time.Clock;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.READ;
import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.WRITE;
import static org.elasticsearch.cluster.metadata.MetadataIndexStateService.VERIFIED_READ_ONLY_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 * Integration tests for {@link DLMConvertToFrozen#maybeMarkIndexReadOnly()}.
 * These tests run against a real internal cluster and verify that the write block is properly
 * added to (or skipped for) real indices, including under node failure scenarios.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DLMConvertToFrozenMarkReadOnlyIT extends ESIntegTestCase {

    private static final String INDEX_NAME = "test-convert-to-frozen-mark-readonly";
    private static final String REPO_NAME = "test-repo";
    private XPackLicenseState licenseState;

    private static void assertIndexWriteBlock(boolean expected) {
        ClusterState clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        ProjectId projectId = Metadata.DEFAULT_PROJECT_ID;
        ClusterBlock writeBlock = WRITE.getBlock();
        String expectation = expected ? "should have" : "should not have";
        assertThat(
            "Index [" + INDEX_NAME + "] " + expectation + " WRITE block",
            clusterState.blocks().hasIndexBlock(projectId, INDEX_NAME, writeBlock),
            is(expected)
        );
    }

    private static void assertIndexVerifiedReadOnly() {
        ClusterState clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        IndexMetadata idxMeta = clusterState.projectState(Metadata.DEFAULT_PROJECT_ID).metadata().index(INDEX_NAME);
        assertThat(
            "Index [" + INDEX_NAME + "] " + "should be verified read-only",
            VERIFIED_READ_ONLY_SETTING.get(idxMeta.getSettings()),
            is(true)
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockTransportService.TestPlugin.class);
    }

    @Before
    public void setupLicense() {
        licenseState = new XPackLicenseState(
            System::currentTimeMillis,
            new XPackLicenseStatus(License.OperationMode.ENTERPRISE, true, null)
        );
    }

    /**
     * Tests that calling maybeMarkIndexReadOnly on an index without a write block
     * successfully adds the WRITE block to the index.
     */
    public void testMarkIndexReadOnlyAddsWriteBlock() throws Exception {
        internalCluster().startNode();
        createIndex(INDEX_NAME, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build());
        ensureGreen(INDEX_NAME);
        setupRepoAndIndexMetadata();

        // verify the index does not have a WRITE block before calling the method
        assertIndexWriteBlock(false);

        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            INDEX_NAME,
            Metadata.DEFAULT_PROJECT_ID,
            client(),
            internalCluster().clusterService(),
            licenseState,
            Clock.systemUTC()
        );

        converter.maybeMarkIndexReadOnly();

        // Verify the WRITE block is now present on the index
        assertIndexWriteBlock(true);
        assertIndexVerifiedReadOnly();
    }

    /**
     * Tests that an index with a non-WRITE block (e.g., READ) still gets a WRITE block
     * added by maybeMarkIndexReadOnly, since only WRITE blocks are checked.
     */
    public void testMarkIndexReadOnlyAddsWriteBlockEvenWhenReadBlockExists() throws Exception {
        internalCluster().startNode();
        createIndex(INDEX_NAME, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build());
        ensureGreen(INDEX_NAME);
        setupRepoAndIndexMetadata();

        // Add a READ block (not WRITE)
        AddIndexBlockRequest addReadBlockRequest = new AddIndexBlockRequest(READ, INDEX_NAME);
        assertAcked(client().execute(TransportAddIndexBlockAction.TYPE, addReadBlockRequest).actionGet());
        assertIndexWriteBlock(false);

        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            INDEX_NAME,
            Metadata.DEFAULT_PROJECT_ID,
            client(),
            internalCluster().clusterService(),
            licenseState,
            Clock.systemUTC()
        );

        // Should still add the WRITE block since the existing block is READ, not WRITE
        converter.maybeMarkIndexReadOnly();

        // Verify the WRITE block is now present on the index
        assertIndexWriteBlock(true);
        assertIndexVerifiedReadOnly();
    }

    /**
     * Tests that calling maybeMarkIndexReadOnly on an index that was created with
     * multiple shards successfully adds the WRITE block.
     */
    public void testMarkIndexReadOnlyOnMultiShardIndex() throws Exception {
        internalCluster().startNode();
        int numShards = randomIntBetween(2, 5);
        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
        ensureGreen(INDEX_NAME);
        setupRepoAndIndexMetadata();
        assertIndexWriteBlock(false);

        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            INDEX_NAME,
            Metadata.DEFAULT_PROJECT_ID,
            client(),
            internalCluster().clusterService(),
            licenseState,
            Clock.systemUTC()
        );

        converter.maybeMarkIndexReadOnly();

        // Verify the WRITE block is now present on the index
        assertIndexWriteBlock(true);
        assertIndexVerifiedReadOnly();
    }

    /**
     * Tests that calling maybeMarkIndexReadOnly twice in succession does not fail.
     * The first call adds the block, and the second call detects it and skips without
     * making any cluster state changes (verified by checking that the index metadata
     * settings version and cluster state version remain unchanged after the second call).
     */
    public void testMarkIndexReadOnlyCalledTwiceSuccessfully() throws Exception {
        internalCluster().startNode();
        createIndex(INDEX_NAME, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build());
        ensureGreen(INDEX_NAME);
        setupRepoAndIndexMetadata();

        ClusterService clusterService = internalCluster().clusterService();

        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            INDEX_NAME,
            Metadata.DEFAULT_PROJECT_ID,
            client(),
            clusterService,
            licenseState,
            Clock.systemUTC()
        );

        // First call adds the block
        converter.maybeMarkIndexReadOnly();
        assertIndexWriteBlock(true);
        assertIndexVerifiedReadOnly();

        // Capture the index metadata state after the first call to verify the second call is a no-op
        ClusterState stateAfterFirstCall = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        IndexMetadata idxMetaAfterFirstCall = stateAfterFirstCall.projectState(Metadata.DEFAULT_PROJECT_ID).metadata().index(INDEX_NAME);
        long settingsVersionAfterFirstCall = idxMetaAfterFirstCall.getSettingsVersion();
        long clusterStateVersionAfterFirstCall = stateAfterFirstCall.version();

        // Second converter uses the same ClusterService which always returns the latest state
        DLMConvertToFrozen converter2 = new DLMConvertToFrozen(
            INDEX_NAME,
            Metadata.DEFAULT_PROJECT_ID,
            client(),
            clusterService,
            licenseState,
            Clock.systemUTC()
        );

        // Second call should be idempotent (skips since block is already present)
        converter2.maybeMarkIndexReadOnly();
        assertIndexWriteBlock(true);
        assertIndexVerifiedReadOnly();

        // Verify the second call did not cause any cluster state changes
        ClusterState stateAfterSecondCall = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        IndexMetadata idxMetaAfterSecondCall = stateAfterSecondCall.projectState(Metadata.DEFAULT_PROJECT_ID).metadata().index(INDEX_NAME);
        assertThat(
            "Settings version should not change on second idempotent call",
            idxMetaAfterSecondCall.getSettingsVersion(),
            is(settingsVersionAfterFirstCall)
        );
        assertThat(
            "Cluster state version should not change on second idempotent call",
            stateAfterSecondCall.version(),
            is(clusterStateVersionAfterFirstCall)
        );
    }

    /**
     * Tests that maybeMarkIndexReadOnly throws an ElasticsearchException when the
     * shard-level block verification step fails due to a transport-level failure.
     * Uses MockTransportService to inject an exception at the shard verification action.
     */
    public void testMarkIndexReadOnlyThrowsOnShardVerificationFailure() throws Exception {
        internalCluster().startMasterOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        createIndex(
            INDEX_NAME,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen(INDEX_NAME);
        setupRepoAndIndexMetadata();

        // Inject a transport-level failure on the shard verification step
        MockTransportService dataNodeTransport = MockTransportService.getInstance(dataNode);
        dataNodeTransport.addRequestHandlingBehavior(
            TransportVerifyShardIndexBlockAction.TYPE.name(),
            (handler, request, channel, task) -> channel.sendResponse(new ElasticsearchException("simulated shard verification failure"))
        );

        try {
            DLMConvertToFrozen converter = new DLMConvertToFrozen(
                INDEX_NAME,
                Metadata.DEFAULT_PROJECT_ID,
                client(),
                internalCluster().clusterService(),
                licenseState,
                Clock.systemUTC()
            );

            expectThrows(ElasticsearchException.class, converter::maybeMarkIndexReadOnly);
        } finally {
            dataNodeTransport.clearAllRules();
        }
    }

    /**
     * Tests that maybeMarkIndexReadOnly throws an ElasticsearchException when the index
     * is deleted after the converter is created but before maybeMarkIndexReadOnly is called.
     * This simulates a race condition where the index disappears mid-run.
     */
    public void testMarkIndexReadOnlyThrowsWhenIndexDeletedBeforeCall() {
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

        // Delete the index after the converter is constructed but before calling maybeMarkIndexReadOnly
        assertAcked(indicesAdmin().prepareDelete(INDEX_NAME));

        // checkIfEligibleForConvertToFrozen detects the missing index and throws IndexNotFoundException
        ElasticsearchException exception = expectThrows(ElasticsearchException.class, converter::maybeMarkIndexReadOnly);
        assertThat(exception.getMessage(), containsString("no such index"));
    }

    /**
     * Tests that the WRITE block set by maybeMarkIndexReadOnly survives a master node
     * failover. This ensures the block is part of the persisted cluster state and is
     * not lost when a new master is elected.
     */
    public void testWriteBlockPersistsAfterMasterFailover() throws Exception {
        // Start 3 master-eligible nodes so we can lose one and still have quorum
        internalCluster().startMasterOnlyNodes(3);
        internalCluster().startDataOnlyNode();
        ensureStableCluster(4);

        createIndex(
            INDEX_NAME,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen(INDEX_NAME);
        setupRepoAndIndexMetadata();
        assertIndexWriteBlock(false);

        DLMConvertToFrozen converter = new DLMConvertToFrozen(
            INDEX_NAME,
            Metadata.DEFAULT_PROJECT_ID,
            client(),
            internalCluster().clusterService(),
            licenseState,
            Clock.systemUTC()
        );

        converter.maybeMarkIndexReadOnly();
        assertIndexWriteBlock(true);
        assertIndexVerifiedReadOnly();

        // Stop the current master to trigger a failover
        internalCluster().stopCurrentMasterNode();
        ensureStableCluster(3);
        ensureGreen(INDEX_NAME);

        // Verify the WRITE block survived the master failover
        assertIndexWriteBlock(true);
        assertIndexVerifiedReadOnly();
    }

    /**
     * Sets up the snapshot repository and adds the required custom index metadata
     * ({@code data_stream_lifecycle -> dlm_freeze_with -> REPO_NAME}) so that
     * {@link DLMConvertToFrozen#checkIfEligibleForConvertToFrozen()} passes.
     */
    private void setupRepoAndIndexMetadata() {
        // Create a snapshot repository
        Path repoPath = randomRepoPath();
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, REPO_NAME)
                .setType("fs")
                .setSettings(Settings.builder().put("location", repoPath))
        );

        // Add the required custom metadata to the index via a cluster state update published through the master service
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
