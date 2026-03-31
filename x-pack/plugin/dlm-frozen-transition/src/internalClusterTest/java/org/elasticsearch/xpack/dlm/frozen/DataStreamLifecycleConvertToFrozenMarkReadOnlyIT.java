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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.Before;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.READ;
import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.WRITE;
import static org.elasticsearch.cluster.metadata.MetadataIndexStateService.VERIFIED_READ_ONLY_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 * Integration tests for {@link DataStreamLifecycleConvertToFrozen#maybeMarkIndexReadOnly()}.
 * These tests run against a real internal cluster and verify that the write block is properly
 * added to (or skipped for) real indices, including under node failure scenarios.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DataStreamLifecycleConvertToFrozenMarkReadOnlyIT extends ESIntegTestCase {

    private static final String INDEX_NAME = "test-convert-to-frozen-mark-readonly";
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
    public void testMarkIndexReadOnlyAddsWriteBlock() {
        internalCluster().startNode();
        createIndex(INDEX_NAME, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build());
        ensureGreen(INDEX_NAME);

        // verify the index does not have a WRITE block before calling the method
        assertIndexWriteBlock(false);

        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(
            INDEX_NAME,
            Metadata.DEFAULT_PROJECT_ID,
            client(),
            internalCluster().clusterService(),
            licenseState
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
    public void testMarkIndexReadOnlyAddsWriteBlockEvenWhenReadBlockExists() {
        internalCluster().startNode();
        createIndex(INDEX_NAME, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build());
        ensureGreen(INDEX_NAME);

        // Add a READ block (not WRITE)
        AddIndexBlockRequest addReadBlockRequest = new AddIndexBlockRequest(READ, INDEX_NAME);
        assertAcked(client().execute(TransportAddIndexBlockAction.TYPE, addReadBlockRequest).actionGet());
        assertIndexWriteBlock(false);

        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(
            INDEX_NAME,
            Metadata.DEFAULT_PROJECT_ID,
            client(),
            internalCluster().clusterService(),
            licenseState
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
    public void testMarkIndexReadOnlyOnMultiShardIndex() {
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
        assertIndexWriteBlock(false);

        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(
            INDEX_NAME,
            Metadata.DEFAULT_PROJECT_ID,
            client(),
            internalCluster().clusterService(),
            licenseState
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
    public void testMarkIndexReadOnlyCalledTwiceSuccessfully() {
        internalCluster().startNode();
        createIndex(INDEX_NAME, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build());
        ensureGreen(INDEX_NAME);

        ClusterService clusterService = internalCluster().clusterService();

        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(
            INDEX_NAME,
            Metadata.DEFAULT_PROJECT_ID,
            client(),
            clusterService,
            licenseState
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
        DataStreamLifecycleConvertToFrozen converter2 = new DataStreamLifecycleConvertToFrozen(
            INDEX_NAME,
            Metadata.DEFAULT_PROJECT_ID,
            client(),
            clusterService,
            licenseState
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
    public void testMarkIndexReadOnlyThrowsOnShardVerificationFailure() {
        internalCluster().startMasterOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        createIndex(
            INDEX_NAME,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen(INDEX_NAME);

        // Inject a transport-level failure on the shard verification step
        MockTransportService dataNodeTransport = MockTransportService.getInstance(dataNode);
        dataNodeTransport.addRequestHandlingBehavior(
            TransportVerifyShardIndexBlockAction.TYPE.name(),
            (handler, request, channel, task) -> {
                channel.sendResponse(new ElasticsearchException("simulated shard verification failure"));
            }
        );

        try {
            DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(
                INDEX_NAME,
                Metadata.DEFAULT_PROJECT_ID,
                client(),
                internalCluster().clusterService(),
                licenseState
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

        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(
            INDEX_NAME,
            Metadata.DEFAULT_PROJECT_ID,
            client(),
            internalCluster().clusterService(),
            licenseState
        );

        // Delete the index after the converter is constructed but before calling maybeMarkIndexReadOnly
        assertAcked(indicesAdmin().prepareDelete(INDEX_NAME));

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, converter::maybeMarkIndexReadOnly);
        assertThat(exception.getMessage(), containsString("DLM unable to mark index"));
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
        assertIndexWriteBlock(false);

        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(
            INDEX_NAME,
            Metadata.DEFAULT_PROJECT_ID,
            client(),
            internalCluster().clusterService(),
            licenseState
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
}
