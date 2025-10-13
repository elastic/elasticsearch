/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.ingest.SamplingService;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PutSampleConfigurationActionIT extends ESIntegTestCase {

    public void testPutSampleConfiguration() throws Exception {
        assumeTrue("Requires the sampling feature flag to be enabled", SamplingService.RANDOM_SAMPLING_FEATURE_FLAG);

        String indexName = randomIdentifier();
        createIndex(indexName);

        // Initially no sampling configuration should exist
        assertSamplingConfigurationNotExists(indexName);

        // Create a sampling configuration
        SamplingConfiguration config = new SamplingConfiguration(0.5d, 50, ByteSizeValue.ofMb(10), TimeValue.timeValueHours(1), null);
        PutSampleConfigurationAction.Request request = new PutSampleConfigurationAction.Request(
            config,
            TimeValue.timeValueSeconds(30),
            TimeValue.timeValueSeconds(10)
        );
        request.indices(indexName);

        AcknowledgedResponse response = client().execute(PutSampleConfigurationAction.INSTANCE, request).actionGet();
        assertTrue("Put sampling configuration should be acknowledged", response.isAcknowledged());

        // Verify the configuration was stored
        assertSamplingConfigurationExists(indexName, config);
    }

    public void testPutSampleConfigurationOverwritesExisting() throws Exception {
        assumeTrue("Requires the sampling feature flag to be enabled", SamplingService.RANDOM_SAMPLING_FEATURE_FLAG);

        String indexName = randomIdentifier();
        createIndex(indexName);

        // Create initial configuration
        SamplingConfiguration initialConfig = new SamplingConfiguration(
            0.3d,
            30,
            ByteSizeValue.ofMb(5),
            TimeValue.timeValueMinutes(30),
            null
        );
        PutSampleConfigurationAction.Request initialRequest = new PutSampleConfigurationAction.Request(
            initialConfig,
            TimeValue.timeValueSeconds(30),
            TimeValue.timeValueSeconds(10)
        );
        initialRequest.indices(indexName);

        AcknowledgedResponse initialResponse = client().execute(PutSampleConfigurationAction.INSTANCE, initialRequest).actionGet();
        assertTrue("Initial put should be acknowledged", initialResponse.isAcknowledged());
        assertSamplingConfigurationExists(indexName, initialConfig);

        // Overwrite with new configuration
        SamplingConfiguration newConfig = new SamplingConfiguration(0.8d, 80, ByteSizeValue.ofMb(20), TimeValue.timeValueHours(2), null);
        PutSampleConfigurationAction.Request updateRequest = new PutSampleConfigurationAction.Request(
            newConfig,
            TimeValue.timeValueSeconds(30),
            TimeValue.timeValueSeconds(10)
        );
        updateRequest.indices(indexName);

        AcknowledgedResponse updateResponse = client().execute(PutSampleConfigurationAction.INSTANCE, updateRequest).actionGet();
        assertTrue("Update put should be acknowledged", updateResponse.isAcknowledged());

        // Verify the configuration was overwritten
        assertSamplingConfigurationExists(indexName, newConfig);
    }

    public void testPutSampleConfigurationWithCondition() throws Exception {
        assumeTrue("Requires the sampling feature flag to be enabled", SamplingService.RANDOM_SAMPLING_FEATURE_FLAG);

        String indexName = randomIdentifier();
        createIndex(indexName);

        // Create configuration with condition
        String condition = "ctx?.field == 'sample_me'";
        SamplingConfiguration configWithCondition = new SamplingConfiguration(
            1.0d,
            100,
            ByteSizeValue.ofMb(15),
            TimeValue.timeValueHours(3),
            condition
        );
        PutSampleConfigurationAction.Request request = new PutSampleConfigurationAction.Request(
            configWithCondition,
            TimeValue.timeValueSeconds(30),
            TimeValue.timeValueSeconds(10)
        );
        request.indices(indexName);

        AcknowledgedResponse response = client().execute(PutSampleConfigurationAction.INSTANCE, request).actionGet();
        assertTrue("Put sampling configuration with condition should be acknowledged", response.isAcknowledged());

        // Verify the configuration with condition was stored
        assertSamplingConfigurationExists(indexName, configWithCondition);
    }

    public void testPutSampleConfigurationNonExistentIndex() throws Exception {
        assumeTrue("Requires the sampling feature flag to be enabled", SamplingService.RANDOM_SAMPLING_FEATURE_FLAG);

        String nonExistentIndex = randomIdentifier();

        // Don't create the index - test that we cannot set sampling config for non-existent indices
        SamplingConfiguration config = new SamplingConfiguration(0.6d, 60, ByteSizeValue.ofMb(8), TimeValue.timeValueMinutes(45), null);
        PutSampleConfigurationAction.Request request = new PutSampleConfigurationAction.Request(
            config,
            TimeValue.timeValueSeconds(30),
            TimeValue.timeValueSeconds(10)
        );
        request.indices(nonExistentIndex);

        // This should now fail - sampling configs cannot be set for non-existent indices
        expectThrows(Exception.class, () -> { client().execute(PutSampleConfigurationAction.INSTANCE, request).actionGet(); });

        // Verify no configuration was stored
        assertSamplingConfigurationNotExists(nonExistentIndex);

        // Now create the index and verify the config can be set
        createIndex(nonExistentIndex);

        AcknowledgedResponse response = client().execute(PutSampleConfigurationAction.INSTANCE, request).actionGet();
        assertTrue("Put sampling configuration should be acknowledged for existing index", response.isAcknowledged());
        assertSamplingConfigurationExists(nonExistentIndex, config);
    }

    public void testPutSampleConfigurationPersistsAcrossClusterStateUpdates() throws Exception {
        assumeTrue("Requires the sampling feature flag to be enabled", SamplingService.RANDOM_SAMPLING_FEATURE_FLAG);

        String indexName = randomIdentifier();
        createIndex(indexName);

        // Store sampling configuration
        SamplingConfiguration config = new SamplingConfiguration(0.9d, 90, ByteSizeValue.ofMb(25), TimeValue.timeValueHours(4), null);
        PutSampleConfigurationAction.Request request = new PutSampleConfigurationAction.Request(
            config,
            TimeValue.timeValueSeconds(30),
            TimeValue.timeValueSeconds(10)
        );
        request.indices(indexName);

        AcknowledgedResponse response = client().execute(PutSampleConfigurationAction.INSTANCE, request).actionGet();
        assertTrue("Put sampling configuration should be acknowledged", response.isAcknowledged());
        assertSamplingConfigurationExists(indexName, config);

        // Trigger cluster state updates by creating additional indices
        for (int i = 0; i < 3; i++) {
            createIndex("dummy-index-" + i);
        }

        // Verify sampling configuration still exists after cluster state changes
        assertSamplingConfigurationExists(indexName, config);
    }

    public void testPutSampleConfigurationWithSamplingIntegration() throws Exception {
        assumeTrue("Requires the sampling feature flag to be enabled", SamplingService.RANDOM_SAMPLING_FEATURE_FLAG);

        String indexName = randomIdentifier();
        createIndex(indexName);

        // Put a sampling configuration that samples everything
        SamplingConfiguration config = new SamplingConfiguration(1.0d, 100, ByteSizeValue.ofMb(50), TimeValue.timeValueDays(1), null);
        PutSampleConfigurationAction.Request putRequest = new PutSampleConfigurationAction.Request(
            config,
            TimeValue.timeValueSeconds(30),
            TimeValue.timeValueSeconds(10)
        );
        putRequest.indices(indexName);

        AcknowledgedResponse putResponse = client().execute(PutSampleConfigurationAction.INSTANCE, putRequest).actionGet();
        assertTrue("Put sampling configuration should be acknowledged", putResponse.isAcknowledged());

        // Index some documents
        int docsToIndex = randomIntBetween(5, 15);
        for (int i = 0; i < docsToIndex; i++) {
            indexDoc(indexName, "doc-" + i, "field1", "value" + i);
        }

        // Verify the sampling configuration works by getting samples
        GetSampleAction.Request getSampleRequest = new GetSampleAction.Request(indexName);
        GetSampleAction.Response getSampleResponse = client().execute(GetSampleAction.INSTANCE, getSampleRequest).actionGet();

        // Since we're sampling at 100%, we should get all documents sampled
        assertEquals("All documents should be sampled", docsToIndex, getSampleResponse.getSample().size());
    }

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
