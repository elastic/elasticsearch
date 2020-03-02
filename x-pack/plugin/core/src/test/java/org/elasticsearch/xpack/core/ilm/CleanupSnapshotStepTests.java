/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.ilm.AbstractStepMasterTimeoutTestCase.emptyClusterState;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class CleanupSnapshotStepTests extends AbstractStepTestCase<CleanupSnapshotStep> {

    @Override
    public CleanupSnapshotStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        String repository = randomAlphaOfLength(10);
        return new CleanupSnapshotStep(stepKey, nextStepKey, client, repository);
    }

    @Override
    protected CleanupSnapshotStep copyInstance(CleanupSnapshotStep instance) {
        return new CleanupSnapshotStep(instance.getKey(), instance.getNextStepKey(), instance.getClient(),
            instance.getSnapshotRepository());
    }

    @Override
    public CleanupSnapshotStep mutateInstance(CleanupSnapshotStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        String snapshotRepository = instance.getSnapshotRepository();
        switch (between(0, 2)) {
            case 0:
                key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 1:
                nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 2:
                snapshotRepository += randomAlphaOfLength(5);
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new CleanupSnapshotStep(key, nextKey, instance.getClient(), snapshotRepository);
    }

    public void testPerformActionFailure() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";
        IndexMetaData.Builder indexMetadataBuilder =
            IndexMetaData.builder(indexName).settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5));
        IndexMetaData indexMetaData = indexMetadataBuilder.build();

        ClusterState clusterState =
            ClusterState.builder(emptyClusterState()).metaData(MetaData.builder().put(indexMetaData, true).build()).build();

        CleanupSnapshotStep cleanupSnapshotStep = createRandomInstance();
        cleanupSnapshotStep.performAction(indexMetaData, clusterState, null, new AsyncActionStep.Listener() {
            @Override
            public void onResponse(boolean complete) {
                fail("expecting a failure as the index doesn't have any snapshot name in its ILM execution state");
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(e, instanceOf(IllegalStateException.class));
                assertThat(e.getMessage(),
                    is("snapshot name was not generated for policy [" + policyName + "] and index [" + indexName + "]"));
            }
        });
    }

    public void testPerformAction() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";
        Map<String, String> ilmCustom = new HashMap<>();
        String snapshotName = indexName + "-" + policyName;
        ilmCustom.put("snapshot_name", snapshotName);

        IndexMetaData.Builder indexMetadataBuilder =
            IndexMetaData.builder(indexName).settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, ilmCustom)
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5));
        IndexMetaData indexMetaData = indexMetadataBuilder.build();

        ClusterState clusterState =
            ClusterState.builder(emptyClusterState()).metaData(MetaData.builder().put(indexMetaData, true).build()).build();

        try (NoOpClient client = getDeleteSnapshotRequestAssertingClient(snapshotName)) {
            CleanupSnapshotStep step = new CleanupSnapshotStep(randomStepKey(), randomStepKey(), client,
                randomAlphaOfLengthBetween(1, 10));
            step.performAction(indexMetaData, clusterState, null, new AsyncActionStep.Listener() {
                @Override
                public void onResponse(boolean complete) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            });
        }
    }

    private NoOpClient getDeleteSnapshotRequestAssertingClient(String expectedSnapshotName) {
        return new NoOpClient(SwapAliasesAndDeleteSourceIndexStep.class.getSimpleName()) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action,
                                                                                                      Request request,
                                                                                                      ActionListener<Response> listener) {
                assertThat(action.name(), is(DeleteSnapshotAction.NAME));
                assertTrue(request instanceof DeleteSnapshotRequest);
                assertThat(((DeleteSnapshotRequest) request).snapshot(), equalTo(expectedSnapshotName));
            }
        };
    }
}
