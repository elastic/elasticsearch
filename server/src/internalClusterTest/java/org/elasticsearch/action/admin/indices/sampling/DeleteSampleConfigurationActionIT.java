/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.ingest.SamplingService;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Integration tests for {@link DeleteSampleConfigurationAction}.
 * <p>
 * These tests verify the delete sample configuration functionality in a real cluster environment,
 * including proper metadata updates, error handling, and interaction with existing configurations.
 * </p>
 */
public class DeleteSampleConfigurationActionIT extends ESIntegTestCase {

    /**
     * Tests successful deletion of an existing sampling configuration.
     */
    public void testDeleteSampleConfiguration() throws Exception {
        assumeTrue("Requires the sampling feature flag to be enabled", SamplingService.RANDOM_SAMPLING_FEATURE_FLAG);

        String indexName = randomIdentifier();
        createIndex(indexName);

        // First create a sampling configuration
        SamplingConfiguration config = new SamplingConfiguration(0.5, 50, ByteSizeValue.ofMb(10), TimeValue.timeValueHours(1), null);
        putSamplingConfiguration(indexName, config);
        assertSamplingConfigurationExists(indexName, config);

        // Now delete the sampling configuration
        DeleteSampleConfigurationAction.Request deleteRequest = new DeleteSampleConfigurationAction.Request(
            TimeValue.timeValueSeconds(30),
            TimeValue.timeValueSeconds(10)
        );
        deleteRequest.indices(indexName);

        AcknowledgedResponse deleteResponse = client().execute(DeleteSampleConfigurationAction.INSTANCE, deleteRequest).actionGet();
        assertTrue("Delete sampling configuration should be acknowledged", deleteResponse.isAcknowledged());

        // Verify the configuration was removed
        assertSamplingConfigurationNotExists(indexName);
    }

    /**
     * Tests deletion of one configuration while leaving others intact.
     */
    public void testDeleteSampleConfigurationLeavesOthersIntact() throws Exception {
        assumeTrue("Requires the sampling feature flag to be enabled", SamplingService.RANDOM_SAMPLING_FEATURE_FLAG);

        String indexName1 = randomIdentifier();
        String indexName2 = randomIdentifier();
        createIndex(indexName1);
        createIndex(indexName2);

        // Create sampling configurations for both indices
        SamplingConfiguration config1 = new SamplingConfiguration(0.3d, 30, ByteSizeValue.ofMb(5), TimeValue.timeValueMinutes(30), null);
        SamplingConfiguration config2 = new SamplingConfiguration(
            0.8d,
            80,
            ByteSizeValue.ofMb(20),
            TimeValue.timeValueHours(2),
            "ctx?.field == 'sample'"
        );

        putSamplingConfiguration(indexName1, config1);
        putSamplingConfiguration(indexName2, config2);
        assertSamplingConfigurationExists(indexName1, config1);
        assertSamplingConfigurationExists(indexName2, config2);

        // Delete configuration for index1 only
        DeleteSampleConfigurationAction.Request deleteRequest = new DeleteSampleConfigurationAction.Request(
            TimeValue.timeValueSeconds(30),
            TimeValue.timeValueSeconds(10)
        );
        deleteRequest.indices(indexName1);

        AcknowledgedResponse deleteResponse = client().execute(DeleteSampleConfigurationAction.INSTANCE, deleteRequest).actionGet();
        assertTrue("Delete sampling configuration should be acknowledged", deleteResponse.isAcknowledged());

        // Verify index1 configuration was removed but index2 configuration remains
        assertSamplingConfigurationNotExists(indexName1);
        assertSamplingConfigurationExists(indexName2, config2);
    }

    /**
     * Tests deletion attempt when no sampling configuration exists for the index.
     */
    public void testDeleteSampleConfigurationNotExists() throws Exception {
        assumeTrue("Requires the sampling feature flag to be enabled", SamplingService.RANDOM_SAMPLING_FEATURE_FLAG);

        String indexName = randomIdentifier();
        createIndex(indexName);

        // Verify no configuration exists initially
        assertSamplingConfigurationNotExists(indexName);

        // Attempt to delete non-existent configuration
        DeleteSampleConfigurationAction.Request deleteRequest = new DeleteSampleConfigurationAction.Request(
            TimeValue.timeValueSeconds(30),
            TimeValue.timeValueSeconds(10)
        );
        deleteRequest.indices(indexName);

        ResourceNotFoundException exception = expectThrows(
            ResourceNotFoundException.class,
            () -> client().execute(DeleteSampleConfigurationAction.INSTANCE, deleteRequest).actionGet()
        );

        assertThat(exception.getMessage(), containsString("provided index [" + indexName + "] has no sampling configuration"));
    }

    /**
     * Tests deletion attempt when no sampling metadata exists at all.
     */
    public void testDeleteSampleConfigurationNoSamplingMetadata() throws Exception {
        assumeTrue("Requires the sampling feature flag to be enabled", SamplingService.RANDOM_SAMPLING_FEATURE_FLAG);

        String indexName = randomIdentifier();
        createIndex(indexName);

        // Ensure no sampling metadata exists by checking cluster state
        assertSamplingConfigurationNotExists(indexName);

        // Attempt to delete configuration when no sampling metadata exists
        DeleteSampleConfigurationAction.Request deleteRequest = new DeleteSampleConfigurationAction.Request(
            TimeValue.timeValueSeconds(30),
            TimeValue.timeValueSeconds(10)
        );
        deleteRequest.indices(indexName);

        ResourceNotFoundException exception = expectThrows(
            ResourceNotFoundException.class,
            () -> client().execute(DeleteSampleConfigurationAction.INSTANCE, deleteRequest).actionGet()
        );

        assertThat(exception.getMessage(), containsString("provided index [" + indexName + "] has no sampling configuration"));
    }

    /**
     * Tests deletion attempt for non-existent index.
     */
    public void testDeleteSampleConfigurationNonExistentIndex() throws Exception {
        assumeTrue("Requires the sampling feature flag to be enabled", SamplingService.RANDOM_SAMPLING_FEATURE_FLAG);

        String nonExistentIndex = randomIdentifier();

        // Don't create the index - test that we cannot delete sampling config for non-existent indices
        DeleteSampleConfigurationAction.Request deleteRequest = new DeleteSampleConfigurationAction.Request(
            TimeValue.timeValueSeconds(30),
            TimeValue.timeValueSeconds(10)
        );
        deleteRequest.indices(nonExistentIndex);

        // This should fail - sampling configs cannot be deleted for non-existent indices
        IndexNotFoundException exception = expectThrows(
            IndexNotFoundException.class,
            () -> client().execute(DeleteSampleConfigurationAction.INSTANCE, deleteRequest).actionGet()
        );

        assertThat(exception.getIndex().getName(), equalTo(nonExistentIndex));
    }

    /**
     * Tests that deletion persists across cluster state updates.
     */
    public void testDeleteSampleConfigurationPersistsAcrossClusterStateUpdates() throws Exception {
        assumeTrue("Requires the sampling feature flag to be enabled", SamplingService.RANDOM_SAMPLING_FEATURE_FLAG);

        String indexName = randomIdentifier();
        createIndex(indexName);

        // Create and verify sampling configuration
        SamplingConfiguration config = new SamplingConfiguration(0.9d, 90, ByteSizeValue.ofMb(25), TimeValue.timeValueHours(4), null);
        putSamplingConfiguration(indexName, config);
        assertSamplingConfigurationExists(indexName, config);

        // Delete the configuration
        DeleteSampleConfigurationAction.Request deleteRequest = new DeleteSampleConfigurationAction.Request(
            TimeValue.timeValueSeconds(30),
            TimeValue.timeValueSeconds(10)
        );
        deleteRequest.indices(indexName);

        AcknowledgedResponse deleteResponse = client().execute(DeleteSampleConfigurationAction.INSTANCE, deleteRequest).actionGet();
        assertTrue("Delete sampling configuration should be acknowledged", deleteResponse.isAcknowledged());
        assertSamplingConfigurationNotExists(indexName);

        // Trigger cluster state updates by creating additional indices
        for (int i = 0; i < 3; i++) {
            createIndex("dummy-index-" + i);
        }

        // Verify sampling configuration remains deleted after cluster state changes
        assertSamplingConfigurationNotExists(indexName);
    }

    /**
     * Tests complete workflow: create, verify, delete, verify deletion, re-create.
     */
    public void testDeleteSampleConfigurationCompleteWorkflow() throws Exception {
        assumeTrue("Requires the sampling feature flag to be enabled", SamplingService.RANDOM_SAMPLING_FEATURE_FLAG);

        String indexName = randomIdentifier();
        createIndex(indexName);

        // Step 1: Create initial configuration
        SamplingConfiguration initialConfig = new SamplingConfiguration(
            0.4d,
            40,
            ByteSizeValue.ofMb(8),
            TimeValue.timeValueMinutes(45),
            "ctx?.status == 'active'"
        );
        putSamplingConfiguration(indexName, initialConfig);
        assertSamplingConfigurationExists(indexName, initialConfig);

        // Step 2: Delete the configuration
        DeleteSampleConfigurationAction.Request deleteRequest = new DeleteSampleConfigurationAction.Request(
            TimeValue.timeValueSeconds(30),
            TimeValue.timeValueSeconds(10)
        );
        deleteRequest.indices(indexName);

        AcknowledgedResponse deleteResponse = client().execute(DeleteSampleConfigurationAction.INSTANCE, deleteRequest).actionGet();
        assertTrue("Delete sampling configuration should be acknowledged", deleteResponse.isAcknowledged());
        assertSamplingConfigurationNotExists(indexName);

        // Step 3: Attempt to delete again (should fail)
        ResourceNotFoundException exception = expectThrows(
            ResourceNotFoundException.class,
            () -> client().execute(DeleteSampleConfigurationAction.INSTANCE, deleteRequest).actionGet()
        );
        assertThat(exception.getMessage(), containsString("provided index [" + indexName + "] has no sampling configuration"));

        // Step 4: Re-create with different configuration
        SamplingConfiguration newConfig = new SamplingConfiguration(0.7d, 70, ByteSizeValue.ofMb(15), TimeValue.timeValueHours(3), null);
        putSamplingConfiguration(indexName, newConfig);
        assertSamplingConfigurationExists(indexName, newConfig);

        // Step 5: Verify we can delete the new configuration
        AcknowledgedResponse secondDeleteResponse = client().execute(DeleteSampleConfigurationAction.INSTANCE, deleteRequest).actionGet();
        assertTrue("Second delete sampling configuration should be acknowledged", secondDeleteResponse.isAcknowledged());
        assertSamplingConfigurationNotExists(indexName);
    }

    /**
     * Tests deletion with different timeout values.
     */
    public void testDeleteSampleConfigurationWithTimeouts() throws Exception {
        assumeTrue("Requires the sampling feature flag to be enabled", SamplingService.RANDOM_SAMPLING_FEATURE_FLAG);

        String indexName = randomIdentifier();
        createIndex(indexName);

        // Create sampling configuration
        SamplingConfiguration config = new SamplingConfiguration(0.6d, 60, ByteSizeValue.ofMb(12), TimeValue.timeValueMinutes(60), null);
        putSamplingConfiguration(indexName, config);
        assertSamplingConfigurationExists(indexName, config);

        // Delete with custom timeouts
        DeleteSampleConfigurationAction.Request deleteRequest = new DeleteSampleConfigurationAction.Request(
            TimeValue.timeValueMinutes(2), // masterNodeTimeout
            TimeValue.timeValueSeconds(45)  // ackTimeout
        );
        deleteRequest.indices(indexName);

        AcknowledgedResponse deleteResponse = client().execute(DeleteSampleConfigurationAction.INSTANCE, deleteRequest).actionGet();
        assertTrue("Delete sampling configuration with custom timeouts should be acknowledged", deleteResponse.isAcknowledged());
        assertSamplingConfigurationNotExists(indexName);
    }

    /**
     * Helper method to create a sampling configuration for an index.
     */
    private void putSamplingConfiguration(String indexName, SamplingConfiguration config) throws Exception {
        PutSampleConfigurationAction.Request putRequest = new PutSampleConfigurationAction.Request(
            config,
            TimeValue.timeValueSeconds(30),
            TimeValue.timeValueSeconds(10)
        );
        putRequest.indices(indexName);

        AcknowledgedResponse putResponse = client().execute(PutSampleConfigurationAction.INSTANCE, putRequest).actionGet();
        assertTrue("Put sampling configuration should be acknowledged", putResponse.isAcknowledged());
    }

    /**
     * Helper method to assert that a sampling configuration exists for an index with expected values.
     */
    private void assertSamplingConfigurationExists(String indexName, SamplingConfiguration expectedConfig) {
        ClusterService clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        ClusterState clusterState = clusterService.state();
        ProjectMetadata projectMetadata = clusterState.metadata().getProject(ProjectId.DEFAULT);
        assertThat("Project metadata should exist", projectMetadata, notNullValue());

        SamplingMetadata samplingMetadata = projectMetadata.custom(SamplingMetadata.TYPE);
        assertThat("Sampling metadata should exist", samplingMetadata, notNullValue());

        Map<String, SamplingConfiguration> configMap = samplingMetadata.getIndexToSamplingConfigMap();
        assertTrue("Configuration should exist for index " + indexName, configMap.containsKey(indexName));

        SamplingConfiguration actualConfig = configMap.get(indexName);
        assertThat("Configuration should match expected for index " + indexName, actualConfig, equalTo(expectedConfig));
    }

    /**
     * Helper method to assert that no sampling configuration exists for an index.
     */
    private void assertSamplingConfigurationNotExists(String indexName) {
        ClusterService clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        ClusterState clusterState = clusterService.state();
        ProjectMetadata projectMetadata = clusterState.metadata().getProject(ProjectId.DEFAULT);

        if (projectMetadata == null) {
            return; // No project metadata means no sampling config
        }

        SamplingMetadata samplingMetadata = projectMetadata.custom(SamplingMetadata.TYPE);
        if (samplingMetadata == null) {
            return; // No sampling metadata means no sampling config
        }

        Map<String, SamplingConfiguration> configMap = samplingMetadata.getIndexToSamplingConfigMap();
        assertThat("Configuration should not exist for index " + indexName, configMap.get(indexName), nullValue());
    }
}
