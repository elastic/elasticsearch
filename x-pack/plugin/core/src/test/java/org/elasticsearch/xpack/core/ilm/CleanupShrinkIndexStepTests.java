/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.util.Map;

import static org.elasticsearch.xpack.core.ilm.GenerateUniqueIndexNameStep.generateValidIndexName;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.is;

public class CleanupShrinkIndexStepTests extends AbstractStepTestCase<CleanupShrinkIndexStep> {

    @Override
    public CleanupShrinkIndexStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        return new CleanupShrinkIndexStep(stepKey, nextStepKey, client);
    }

    @Override
    protected CleanupShrinkIndexStep copyInstance(CleanupShrinkIndexStep instance) {
        return new CleanupShrinkIndexStep(instance.getKey(), instance.getNextStepKey(), instance.getClient());
    }

    @Override
    public CleanupShrinkIndexStep mutateInstance(CleanupShrinkIndexStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        switch (between(0, 1)) {
            case 0:
                key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 1:
                nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new CleanupShrinkIndexStep(key, nextKey, instance.getClient());
    }

    public void testPerformActionDoesntFailIfShrinkingIndexNameIsMissing() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";

        IndexMetadata.Builder indexMetadataBuilder =
            IndexMetadata.builder(indexName).settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5));

        IndexMetadata indexMetadata = indexMetadataBuilder.build();

        ClusterState clusterState =
            ClusterState.builder(emptyClusterState()).metadata(Metadata.builder().put(indexMetadata, true).build()).build();

        CleanupShrinkIndexStep cleanupShrinkIndexStep = createRandomInstance();
        cleanupShrinkIndexStep.performAction(indexMetadata, clusterState, null, new ActionListener<>() {
            @Override
            public void onResponse(Boolean complete) {
                assertThat(complete, is(true));
            }

            @Override
            public void onFailure(Exception e) {
                fail("expecting the step to not report any failure if there isn't any shrink index name stored in the ILM execution " +
                    "state but got:" + e.getMessage());
            }
        });
    }

    public void testPerformAction() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";
        String shrinkIndexName = generateValidIndexName("shrink-", indexName);
        Map<String, String> ilmCustom = Map.of("shrink_index_name", shrinkIndexName);

        IndexMetadata.Builder indexMetadataBuilder =
            IndexMetadata.builder(indexName).settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, ilmCustom)
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5));
        IndexMetadata indexMetadata = indexMetadataBuilder.build();

        ClusterState clusterState =
            ClusterState.builder(emptyClusterState()).metadata(Metadata.builder().put(indexMetadata, true).build()).build();

        try (NoOpClient client = getDeleteIndexRequestAssertingClient(shrinkIndexName)) {
            CleanupShrinkIndexStep step = new CleanupShrinkIndexStep(randomStepKey(), randomStepKey(), client);
            step.performAction(indexMetadata, clusterState, null, new ActionListener<>() {
                @Override
                public void onResponse(Boolean complete) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            });
        }
    }

    public void testDeleteSkippedIfManagedIndexIsShrunkAndSourceDoesntExist() throws Exception {
        String sourceIndex = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";
        String shrinkIndexName = generateValidIndexName("shrink-", sourceIndex);
        Map<String, String> ilmCustom = Map.of("shrink_index_name", shrinkIndexName);

        IndexMetadata.Builder shrunkIndexMetadataBuilder = IndexMetadata.builder(shrinkIndexName)
            .settings(
                settings(Version.CURRENT)
                    .put(LifecycleSettings.LIFECYCLE_NAME, policyName)
                    .put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY, sourceIndex)
            )
            .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, ilmCustom)
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5));
        IndexMetadata shrunkIndexMetadata = shrunkIndexMetadataBuilder.build();

        ClusterState clusterState =
            ClusterState.builder(emptyClusterState()).metadata(Metadata.builder().put(shrunkIndexMetadata, true).build()).build();

        try (NoOpClient client = getFailingIfCalledClient()) {
            CleanupShrinkIndexStep step = new CleanupShrinkIndexStep(randomStepKey(), randomStepKey(), client);
            step.performAction(shrunkIndexMetadata, clusterState, null, new ActionListener<>() {
                @Override
                public void onResponse(Boolean complete) {
                    assertThat(complete, is(true));
                }

                @Override
                public void onFailure(Exception e) {

                }
            });
        }
    }

    private NoOpClient getDeleteIndexRequestAssertingClient(String shrinkIndexName) {
        return new NoOpClient(getTestName()) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action,
                                                                                                      Request request,
                                                                                                      ActionListener<Response> listener) {
                assertThat(action.name(), is(DeleteIndexAction.NAME));
                assertTrue(request instanceof DeleteIndexRequest);
                assertThat(((DeleteIndexRequest) request).indices(), arrayContaining(shrinkIndexName));
            }
        };
    }

    private NoOpClient getFailingIfCalledClient() {
        return new NoOpClient(getTestName()) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action,
                                                                                                      Request request,
                                                                                                      ActionListener<Response> listener) {
                throw new IllegalStateException("not expecting client to be called, but received request [" + request + "] for action ["
                    + action + "]");
            }
        };
    }
}
