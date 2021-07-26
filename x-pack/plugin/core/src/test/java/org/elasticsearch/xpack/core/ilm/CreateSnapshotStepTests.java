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
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class CreateSnapshotStepTests extends AbstractStepTestCase<CreateSnapshotStep> {

    @Override
    public CreateSnapshotStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        return new CreateSnapshotStep(stepKey, nextStepKey, client);
    }

    @Override
    protected CreateSnapshotStep copyInstance(CreateSnapshotStep instance) {
        return new CreateSnapshotStep(instance.getKey(), instance.getNextStepKey(), instance.getClient());
    }

    @Override
    public CreateSnapshotStep mutateInstance(CreateSnapshotStep instance) {
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
        return new CreateSnapshotStep(key, nextKey, instance.getClient());
    }

    public void testPerformActionFailure() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";

        {
            IndexMetadata.Builder indexMetadataBuilder =
                IndexMetadata.builder(indexName).settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                    .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5));
            Map<String, String> ilmCustom = new HashMap<>();
            String repository = "repository";
            ilmCustom.put("snapshot_repository", repository);
            indexMetadataBuilder.putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, ilmCustom);

            IndexMetadata indexMetadata = indexMetadataBuilder.build();

            ClusterState clusterState =
                ClusterState.builder(emptyClusterState()).metadata(Metadata.builder().put(indexMetadata, true).build()).build();

            CreateSnapshotStep createSnapshotStep = createRandomInstance();
            Exception e = expectThrows(IllegalStateException.class, () -> PlainActionFuture.<Boolean, Exception>get(
                    f -> createSnapshotStep.performAction(indexMetadata, clusterState, null, f)));
            assertThat(e.getMessage(),
                is("snapshot name was not generated for policy [" + policyName + "] and index [" + indexName + "]"));
        }

        {
            IndexMetadata.Builder indexMetadataBuilder =
                IndexMetadata.builder(indexName).settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                    .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5));
            IndexMetadata indexMetadata = indexMetadataBuilder.build();

            ClusterState clusterState =
                ClusterState.builder(emptyClusterState()).metadata(Metadata.builder().put(indexMetadata, true).build()).build();

            CreateSnapshotStep createSnapshotStep = createRandomInstance();
            Exception e = expectThrows(IllegalStateException.class, () -> PlainActionFuture.<Boolean, Exception>get(
                f -> createSnapshotStep.performAction(indexMetadata, clusterState, null, f)));
            assertThat(e.getMessage(),
                is("snapshot repository is not present for policy [" + policyName + "] and index [" + indexName + "]"));
        }
    }

    public void testPerformAction() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";
        Map<String, String> ilmCustom = new HashMap<>();
        String snapshotName = indexName + "-" + policyName;
        ilmCustom.put("snapshot_name", snapshotName);
        String repository = "repository";
        ilmCustom.put("snapshot_repository", repository);

        IndexMetadata.Builder indexMetadataBuilder =
            IndexMetadata.builder(indexName).settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, ilmCustom)
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5));
        IndexMetadata indexMetadata = indexMetadataBuilder.build();

        ClusterState clusterState =
            ClusterState.builder(emptyClusterState()).metadata(Metadata.builder().put(indexMetadata, true).build()).build();

        try (NoOpClient client = getCreateSnapshotRequestAssertingClient(repository, snapshotName, indexName)) {
            CreateSnapshotStep step = new CreateSnapshotStep(randomStepKey(), randomStepKey(), client);
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

    private NoOpClient getCreateSnapshotRequestAssertingClient(String expectedRepoName, String expectedSnapshotName, String indexName) {
        return new NoOpClient(getTestName()) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action,
                                                                                                      Request request,
                                                                                                      ActionListener<Response> listener) {
                assertThat(action.name(), is(CreateSnapshotAction.NAME));
                assertTrue(request instanceof CreateSnapshotRequest);
                CreateSnapshotRequest createSnapshotRequest = (CreateSnapshotRequest) request;
                assertThat(createSnapshotRequest.indices().length, is(1));
                assertThat(createSnapshotRequest.indices()[0], is(indexName));
                assertThat(createSnapshotRequest.repository(), is(expectedRepoName));
                assertThat(createSnapshotRequest.snapshot(), is(expectedSnapshotName));
                assertThat(CreateSnapshotStep.NAME + " waits for the create snapshot request to complete",
                    createSnapshotRequest.waitForCompletion(), is(true));
                assertThat("ILM generated snapshots should not include global state", createSnapshotRequest.includeGlobalState(),
                    is(false));
            }
        };
    }
}
