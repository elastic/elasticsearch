/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.datastreams;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockRequest;
import org.elasticsearch.action.admin.indices.readonly.TransportAddIndexBlockAction;
import org.elasticsearch.action.admin.indices.readonly.TransportVerifyShardIndexBlockAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
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
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
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

        ProjectState projectState = getCurrentProjectState();

        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(
            INDEX_NAME,
            client(),
            projectState,
            licenseState
        );

        converter.maybeMarkIndexReadOnly();

        // Verify the WRITE block is now present on the index
        assertIndexWriteBlock(true);
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

        ProjectState projectState = getCurrentProjectState();

        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(
            INDEX_NAME,
            client(),
            projectState,
            licenseState
        );

        // Should still add the WRITE block since the existing block is READ, not WRITE
        converter.maybeMarkIndexReadOnly();

        // Verify the WRITE block is now present on the index
        assertIndexWriteBlock(true);
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

        ProjectState projectState = getCurrentProjectState();

        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(
            INDEX_NAME,
            client(),
            projectState,
            licenseState
        );

        converter.maybeMarkIndexReadOnly();

        // Verify the WRITE block is now present on the index
        assertIndexWriteBlock(true);
    }

    /**
     * Tests that calling maybeMarkIndexReadOnly twice in succession does not fail.
     * The first call adds the block, and the second call detects it and skips.
     */
    public void testMarkIndexReadOnlyCalledTwiceSuccessfully() {
        internalCluster().startNode();
        createIndex(INDEX_NAME, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build());
        ensureGreen(INDEX_NAME);

        ProjectState projectState = getCurrentProjectState();

        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(
            INDEX_NAME,
            client(),
            projectState,
            licenseState
        );

        // First call adds the block
        converter.maybeMarkIndexReadOnly();
        assertIndexWriteBlock(true);

        // Get fresh project state with the block now present
        ProjectState updatedProjectState = getCurrentProjectState();
        DataStreamLifecycleConvertToFrozen converter2 = new DataStreamLifecycleConvertToFrozen(
            INDEX_NAME,
            client(),
            updatedProjectState,
            licenseState
        );

        // Second call should be idempotent (skips since block is already present)
        converter2.maybeMarkIndexReadOnly();
        assertIndexWriteBlock(true);
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
                throw new ElasticsearchException("simulated shard verification failure");
            }
        );

        try {
            ProjectState projectState = getCurrentProjectState();
            DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(
                INDEX_NAME,
                client(),
                projectState,
                licenseState
            );

            expectThrows(ElasticsearchException.class, converter::maybeMarkIndexReadOnly);
        } finally {
            dataNodeTransport.clearAllRules();
        }
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

        ProjectState projectState = getCurrentProjectState();
        DataStreamLifecycleConvertToFrozen converter = new DataStreamLifecycleConvertToFrozen(
            INDEX_NAME,
            client(),
            projectState,
            licenseState
        );

        converter.maybeMarkIndexReadOnly();
        assertIndexWriteBlock(true);

        // Stop the current master to trigger a failover
        internalCluster().stopCurrentMasterNode();
        ensureStableCluster(3);
        ensureGreen(INDEX_NAME);

        // Verify the WRITE block survived the master failover
        assertIndexWriteBlock(true);
    }

    private ProjectState getCurrentProjectState() {
        ClusterState clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        return clusterState.projectState(Metadata.DEFAULT_PROJECT_ID);
    }

    private static void assertIndexWriteBlock(boolean expected) {
        ClusterState clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        ProjectId projectId = Metadata.DEFAULT_PROJECT_ID;
        ClusterBlock writeBlock = WRITE.getBlock();
        assertThat(
            "Index [" + DataStreamLifecycleConvertToFrozenMarkReadOnlyIT.INDEX_NAME + "] should have WRITE block",
            clusterState.blocks().hasIndexBlock(projectId, DataStreamLifecycleConvertToFrozenMarkReadOnlyIT.INDEX_NAME, writeBlock),
            is(expected)
        );
    }
}
