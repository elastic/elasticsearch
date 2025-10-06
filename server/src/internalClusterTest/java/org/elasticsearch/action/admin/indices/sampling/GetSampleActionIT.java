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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.ingest.SamplingService;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class GetSampleActionIT extends ESIntegTestCase {

    public void testGetSample() throws Exception {
        assumeTrue("Requires the sampling feature flag to be enabled", SamplingService.RANDOM_SAMPLING_FEATURE_FLAG);
        String indexName = randomIdentifier();
        // the index doesn't exist, so getting its sample will throw an exception:
        assertGetSampleThrowsResourceNotFoundException(indexName);
        createIndex(indexName);
        // the index exists but there is no sampling configuration for it, so getting its sample will throw an exception:
        assertGetSampleThrowsResourceNotFoundException(indexName);
        addSamplingConfig(indexName);
        // There is now a sampling configuration, but no data has been ingested:
        assertEmptySample(indexName);
        int docsToIndex = randomIntBetween(1, 20);
        for (int i = 0; i < docsToIndex; i++) {
            indexDoc(indexName, randomIdentifier(), randomAlphanumericOfLength(10), randomAlphanumericOfLength(10));
        }
        GetSampleAction.Request request = new GetSampleAction.Request(indexName);
        GetSampleAction.Response response = client().execute(GetSampleAction.INSTANCE, request).actionGet();
        List<SamplingService.RawDocument> sample = response.getSample();
        // The sampling config created by addSamplingConfig samples at 100%, so we expect everything to be sampled:
        assertThat(sample.size(), equalTo(docsToIndex));
        for (int i = 0; i < docsToIndex; i++) {
            assertRawDocument(sample.get(i), indexName);
        }
    }

    private void assertRawDocument(SamplingService.RawDocument rawDocument, String indexName) {
        assertThat(rawDocument.indexName(), equalTo(indexName));
    }

    private void assertEmptySample(String indexName) {
        GetSampleAction.Request request = new GetSampleAction.Request(indexName);
        GetSampleAction.Response response = client().execute(GetSampleAction.INSTANCE, request).actionGet();
        List<SamplingService.RawDocument> sample = response.getSample();
        assertThat(sample, equalTo(List.of()));
    }

    private void assertGetSampleThrowsResourceNotFoundException(String indexName) {
        GetSampleAction.Request request = new GetSampleAction.Request(indexName);
        assertThrows(ResourceNotFoundException.class, () -> client().execute(GetSampleAction.INSTANCE, request).actionGet());
    }

    @SuppressWarnings("deprecation")
    private void addSamplingConfig(String indexName) throws Exception {
        /*
         * Note: The following code writes a sampling config directly to the cluster state. It can be replaced with a call to the action
         * that does this once that action exists.
         */
        final ClusterService clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        clusterService.submitUnbatchedStateUpdateTask("blocking-task", new ClusterStateUpdateTask(Priority.IMMEDIATE) {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(
                    currentState.metadata().getProject(ProjectId.DEFAULT)
                );
                SamplingMetadata samplingMetadata = new SamplingMetadata(
                    Map.of(indexName, new SamplingConfiguration(1.0d, 100, null, null, null))
                );
                projectMetadataBuilder.putCustom(SamplingMetadata.TYPE, samplingMetadata);
                ClusterState newState = new ClusterState.Builder(currentState).putProjectMetadata(projectMetadataBuilder).build();
                return newState;
            }

            @Override
            public void onFailure(Exception e) {
                assert false : e.getMessage();
            }
        });
        awaitClusterState(state -> {
            SamplingMetadata samplingMetadata = clusterService.state()
                .metadata()
                .getProject(ProjectId.DEFAULT)
                .custom(SamplingMetadata.TYPE);
            return samplingMetadata != null && samplingMetadata.getIndexToSamplingConfigMap().get(indexName) != null;
        });
    }
}
