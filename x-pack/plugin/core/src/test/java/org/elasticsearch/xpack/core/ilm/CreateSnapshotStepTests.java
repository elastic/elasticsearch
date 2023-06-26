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
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.snapshots.SnapshotNameAlreadyInUseException;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class CreateSnapshotStepTests extends AbstractStepTestCase<CreateSnapshotStep> {

    @Override
    public CreateSnapshotStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKeyOnComplete = randomStepKey();
        StepKey nextStepKeyOnIncomplete = randomStepKey();
        return new CreateSnapshotStep(stepKey, nextStepKeyOnComplete, nextStepKeyOnIncomplete, client);
    }

    @Override
    protected CreateSnapshotStep copyInstance(CreateSnapshotStep instance) {
        return new CreateSnapshotStep(
            instance.getKey(),
            instance.getNextKeyOnComplete(),
            instance.getNextKeyOnIncomplete(),
            instance.getClient()
        );
    }

    @Override
    public CreateSnapshotStep mutateInstance(CreateSnapshotStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKeyOnCompleteResponse = instance.getNextKeyOnComplete();
        StepKey nextKeyOnIncompleteResponse = instance.getNextKeyOnIncomplete();
        switch (between(0, 2)) {
            case 0 -> key = new StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
            case 1 -> nextKeyOnCompleteResponse = randomStepKey();
            case 2 -> nextKeyOnIncompleteResponse = randomStepKey();
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new CreateSnapshotStep(key, nextKeyOnCompleteResponse, nextKeyOnIncompleteResponse, instance.getClient());
    }

    public void testPerformActionFailure() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";

        {
            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexName)
                .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(0, 5));
            Map<String, String> ilmCustom = new HashMap<>();
            String repository = "repository";
            ilmCustom.put("snapshot_repository", repository);
            indexMetadataBuilder.putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, ilmCustom);

            IndexMetadata indexMetadata = indexMetadataBuilder.build();

            ClusterState clusterState = ClusterState.builder(emptyClusterState())
                .metadata(Metadata.builder().put(indexMetadata, true).build())
                .build();

            CreateSnapshotStep createSnapshotStep = createRandomInstance();
            Exception e = expectThrows(
                IllegalStateException.class,
                () -> PlainActionFuture.<Void, Exception>get(f -> createSnapshotStep.performAction(indexMetadata, clusterState, null, f))
            );
            assertThat(e.getMessage(), is("snapshot name was not generated for policy [" + policyName + "] and index [" + indexName + "]"));
        }

        {
            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexName)
                .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(0, 5));
            IndexMetadata indexMetadata = indexMetadataBuilder.build();

            ClusterState clusterState = ClusterState.builder(emptyClusterState())
                .metadata(Metadata.builder().put(indexMetadata, true).build())
                .build();

            CreateSnapshotStep createSnapshotStep = createRandomInstance();
            Exception e = expectThrows(
                IllegalStateException.class,
                () -> PlainActionFuture.<Void, Exception>get(f -> createSnapshotStep.performAction(indexMetadata, clusterState, null, f))
            );
            assertThat(
                e.getMessage(),
                is("snapshot repository is not present for policy [" + policyName + "] and index [" + indexName + "]")
            );
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

        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, ilmCustom)
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5));
        IndexMetadata indexMetadata = indexMetadataBuilder.build();

        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .build();

        try (NoOpClient client = getCreateSnapshotRequestAssertingClient(repository, snapshotName, indexName)) {
            CreateSnapshotStep step = new CreateSnapshotStep(randomStepKey(), randomStepKey(), randomStepKey(), client);
            step.performAction(indexMetadata, clusterState, null, ActionListener.noop());
        }
    }

    public void testNextStepKey() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";
        Map<String, String> ilmCustom = new HashMap<>();
        String snapshotName = indexName + "-" + policyName;
        ilmCustom.put("snapshot_name", snapshotName);
        String repository = "repository";
        ilmCustom.put("snapshot_repository", repository);

        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, ilmCustom)
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5));
        IndexMetadata indexMetadata = indexMetadataBuilder.build();

        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .build();
        {
            try (NoOpClient client = new NoOpClient(getTestName())) {
                StepKey nextKeyOnComplete = randomStepKey();
                StepKey nextKeyOnIncomplete = randomStepKey();
                CreateSnapshotStep completeStep = new CreateSnapshotStep(randomStepKey(), nextKeyOnComplete, nextKeyOnIncomplete, client) {
                    @Override
                    void createSnapshot(IndexMetadata indexMetadata, ActionListener<Boolean> listener) {
                        listener.onResponse(true);
                    }
                };
                completeStep.performAction(indexMetadata, clusterState, null, ActionListener.noop());
                assertThat(completeStep.getNextStepKey(), is(nextKeyOnComplete));
            }
        }

        {
            try (NoOpClient client = new NoOpClient(getTestName())) {
                StepKey nextKeyOnComplete = randomStepKey();
                StepKey nextKeyOnIncomplete = randomStepKey();
                CreateSnapshotStep incompleteStep = new CreateSnapshotStep(
                    randomStepKey(),
                    nextKeyOnComplete,
                    nextKeyOnIncomplete,
                    client
                ) {
                    @Override
                    void createSnapshot(IndexMetadata indexMetadata, ActionListener<Boolean> listener) {
                        listener.onResponse(false);
                    }
                };
                incompleteStep.performAction(indexMetadata, clusterState, null, ActionListener.noop());
                assertThat(incompleteStep.getNextStepKey(), is(nextKeyOnIncomplete));
            }
        }

        {
            try (NoOpClient client = new NoOpClient(getTestName())) {
                StepKey nextKeyOnComplete = randomStepKey();
                StepKey nextKeyOnIncomplete = randomStepKey();
                CreateSnapshotStep doubleInvocationStep = new CreateSnapshotStep(
                    randomStepKey(),
                    nextKeyOnComplete,
                    nextKeyOnIncomplete,
                    client
                ) {
                    @Override
                    void createSnapshot(IndexMetadata indexMetadata, ActionListener<Boolean> listener) {
                        listener.onFailure(new SnapshotNameAlreadyInUseException(repository, snapshotName, "simulated"));
                    }
                };
                doubleInvocationStep.performAction(indexMetadata, clusterState, null, ActionListener.noop());
                assertThat(doubleInvocationStep.getNextStepKey(), is(nextKeyOnIncomplete));
            }
        }
    }

    private NoOpClient getCreateSnapshotRequestAssertingClient(String expectedRepoName, String expectedSnapshotName, String indexName) {
        return new NoOpClient(getTestName()) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                assertThat(action.name(), is(CreateSnapshotAction.NAME));
                assertTrue(request instanceof CreateSnapshotRequest);
                CreateSnapshotRequest createSnapshotRequest = (CreateSnapshotRequest) request;
                assertThat(createSnapshotRequest.indices().length, is(1));
                assertThat(createSnapshotRequest.indices()[0], is(indexName));
                assertThat(createSnapshotRequest.repository(), is(expectedRepoName));
                assertThat(createSnapshotRequest.snapshot(), is(expectedSnapshotName));
                assertThat(
                    CreateSnapshotStep.NAME + " waits for the create snapshot request to complete",
                    createSnapshotRequest.waitForCompletion(),
                    is(true)
                );
                assertThat(
                    "ILM generated snapshots should not include global state",
                    createSnapshotRequest.includeGlobalState(),
                    is(false)
                );
            }
        };
    }
}
