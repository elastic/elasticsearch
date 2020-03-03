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
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.ilm.AbstractStepMasterTimeoutTestCase.emptyClusterState;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class RestoreSnapshotStepTests extends AbstractStepTestCase<RestoreSnapshotStep> {

    @Override
    public RestoreSnapshotStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        String repository = randomAlphaOfLength(10);
        String restoredIndexPrefix = randomAlphaOfLength(10);
        return new RestoreSnapshotStep(stepKey, nextStepKey, client, repository, restoredIndexPrefix);
    }

    @Override
    protected RestoreSnapshotStep copyInstance(RestoreSnapshotStep instance) {
        return new RestoreSnapshotStep(instance.getKey(), instance.getNextStepKey(), instance.getClient(),
            instance.getSnapshotRepository(), instance.getRestoredIndexPrefix());
    }

    @Override
    public RestoreSnapshotStep mutateInstance(RestoreSnapshotStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        String snapshotRepository = instance.getSnapshotRepository();
        String restoredIndexPrefix = instance.getRestoredIndexPrefix();
        switch (between(0, 3)) {
            case 0:
                key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 1:
                nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 2:
                snapshotRepository = randomValueOtherThan(snapshotRepository, () -> randomAlphaOfLengthBetween(1, 10));
                break;
            case 3:
                restoredIndexPrefix = randomValueOtherThan(restoredIndexPrefix, () -> randomAlphaOfLengthBetween(1, 10));
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new RestoreSnapshotStep(key, nextKey, instance.getClient(), snapshotRepository, restoredIndexPrefix);
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

        RestoreSnapshotStep restoreSnapshotStep = createRandomInstance();
        restoreSnapshotStep.performAction(indexMetaData, clusterState, null, new AsyncActionStep.Listener() {
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

        String repository = "repository";
        String restoredIndexPrefix = "restored-";
        try (NoOpClient client = getRestoreSnapshotRequestAssertingClient(repository, snapshotName, indexName, restoredIndexPrefix)) {
            RestoreSnapshotStep step = new RestoreSnapshotStep(randomStepKey(), randomStepKey(), client, repository, restoredIndexPrefix);
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

    private NoOpClient getRestoreSnapshotRequestAssertingClient(String expectedRepoName, String expectedSnapshotName, String indexName,
                                                                String restoredIndexPrefix) {
        return new NoOpClient(getTestName()) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action,
                                                                                                      Request request,
                                                                                                      ActionListener<Response> listener) {
                assertThat(action.name(), is(RestoreSnapshotAction.NAME));
                assertTrue(request instanceof RestoreSnapshotRequest);
                RestoreSnapshotRequest restoreSnapshotRequest = (RestoreSnapshotRequest) request;
                assertThat(restoreSnapshotRequest.repository(), is(expectedRepoName));
                assertThat(restoreSnapshotRequest.snapshot(), is(expectedSnapshotName));
                assertThat("another ILM step will wait for the restore to complete. the " + RestoreSnapshotStep.NAME + " step should not",
                    restoreSnapshotRequest.waitForCompletion(), is(false));
                assertThat("another ILM step will transfer the aliases to the restored index", restoreSnapshotRequest.includeAliases(),
                    is(false));
                assertThat(restoreSnapshotRequest.ignoreIndexSettings(), is(notNullValue()));
                assertThat(restoreSnapshotRequest.ignoreIndexSettings()[0], is(LifecycleSettings.LIFECYCLE_NAME));
                assertThat(restoreSnapshotRequest.renameReplacement(), is(restoredIndexPrefix + indexName));
                assertThat(restoreSnapshotRequest.renamePattern(), is(indexName));
            }
        };
    }
}
