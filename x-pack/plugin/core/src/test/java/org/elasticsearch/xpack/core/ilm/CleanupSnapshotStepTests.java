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
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.util.Map;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.is;

public class CleanupSnapshotStepTests extends AbstractStepTestCase<CleanupSnapshotStep> {

    @Override
    public CleanupSnapshotStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        return new CleanupSnapshotStep(stepKey, nextStepKey, client);
    }

    @Override
    protected CleanupSnapshotStep copyInstance(CleanupSnapshotStep instance) {
        return new CleanupSnapshotStep(instance.getKey(), instance.getNextStepKey(), instance.getClient());
    }

    @Override
    public CleanupSnapshotStep mutateInstance(CleanupSnapshotStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        switch (between(0, 1)) {
            case 0 -> key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            case 1 -> nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new CleanupSnapshotStep(key, nextKey, instance.getClient());
    }

    public void testPerformActionDoesntFailIfSnapshotInfoIsMissing() throws Exception {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";

        {
            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexName)
                .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(0, 5));

            IndexMetadata indexMetadata = indexMetadataBuilder.build();

            ClusterState clusterState = ClusterState.builder(emptyClusterState())
                .metadata(Metadata.builder().put(indexMetadata, true).build())
                .build();

            CleanupSnapshotStep cleanupSnapshotStep = createRandomInstance();
            PlainActionFuture.<Void, Exception>get(f -> cleanupSnapshotStep.performAction(indexMetadata, clusterState, null, f));
        }

        {
            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexName)
                .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(0, 5));
            Map<String, String> ilmCustom = Map.of("snapshot_repository", "repository_name");
            indexMetadataBuilder.putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, ilmCustom);

            IndexMetadata indexMetadata = indexMetadataBuilder.build();

            ClusterState clusterState = ClusterState.builder(emptyClusterState())
                .metadata(Metadata.builder().put(indexMetadata, true).build())
                .build();

            CleanupSnapshotStep cleanupSnapshotStep = createRandomInstance();
            PlainActionFuture.<Void, Exception>get(f -> cleanupSnapshotStep.performAction(indexMetadata, clusterState, null, f));
        }
    }

    public void testPerformAction() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";
        String snapshotName = indexName + "-" + policyName;
        Map<String, String> ilmCustom = Map.of("snapshot_name", snapshotName);

        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, ilmCustom)
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5));
        IndexMetadata indexMetadata = indexMetadataBuilder.build();

        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .build();

        try (NoOpClient client = getDeleteSnapshotRequestAssertingClient(snapshotName)) {
            CleanupSnapshotStep step = new CleanupSnapshotStep(randomStepKey(), randomStepKey(), client);
            step.performAction(indexMetadata, clusterState, null, ActionListener.noop());
        }
    }

    private NoOpClient getDeleteSnapshotRequestAssertingClient(String expectedSnapshotName) {
        return new NoOpClient(getTestName()) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                assertThat(action.name(), is(DeleteSnapshotAction.NAME));
                assertTrue(request instanceof DeleteSnapshotRequest);
                assertThat(((DeleteSnapshotRequest) request).snapshots(), arrayContaining(expectedSnapshotName));
            }
        };
    }
}
