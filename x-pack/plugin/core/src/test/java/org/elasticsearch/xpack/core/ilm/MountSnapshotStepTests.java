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
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.ilm.AbstractStepMasterTimeoutTestCase.emptyClusterState;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class MountSnapshotStepTests extends AbstractStepTestCase<MountSnapshotStep> {

    private static final String RESTORED_INDEX_PREFIX = "restored-";

    @Override
    public MountSnapshotStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        String restoredIndexPrefix = randomAlphaOfLength(10);
        return new MountSnapshotStep(stepKey, nextStepKey, client, restoredIndexPrefix);
    }

    @Override
    protected MountSnapshotStep copyInstance(MountSnapshotStep instance) {
        return new MountSnapshotStep(instance.getKey(), instance.getNextStepKey(), instance.getClient(), instance.getRestoredIndexPrefix());
    }

    @Override
    public MountSnapshotStep mutateInstance(MountSnapshotStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        String restoredIndexPrefix = instance.getRestoredIndexPrefix();
        switch (between(0, 2)) {
            case 0:
                key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 1:
                nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 2:
                restoredIndexPrefix = randomValueOtherThan(restoredIndexPrefix, () -> randomAlphaOfLengthBetween(1, 10));
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new MountSnapshotStep(key, nextKey, instance.getClient(), restoredIndexPrefix);
    }

    public void testPerformActionFailure() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";

        {
            IndexMetadata.Builder indexMetadataBuilder =
                IndexMetadata.builder(indexName).settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                    .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5));
            IndexMetadata indexMetadata = indexMetadataBuilder.build();

            ClusterState clusterState =
                ClusterState.builder(emptyClusterState()).metadata(Metadata.builder().put(indexMetadata, true).build()).build();

            MountSnapshotStep mountSnapshotStep = createRandomInstance();
            mountSnapshotStep.performAction(indexMetadata, clusterState, null, new AsyncActionStep.Listener() {
                @Override
                public void onResponse(boolean complete) {
                    fail("expecting a failure as the index doesn't have any repository name in its ILM execution state");
                }

                @Override
                public void onFailure(Exception e) {
                    assertThat(e, instanceOf(IllegalStateException.class));
                    assertThat(e.getMessage(),
                        is("snapshot repository is not present for policy [" + policyName + "] and index [" + indexName + "]"));
                }
            });
        }

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

            MountSnapshotStep mountSnapshotStep = createRandomInstance();
            mountSnapshotStep.performAction(indexMetadata, clusterState, null, new AsyncActionStep.Listener() {
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

        try (NoOpClient client = getRestoreSnapshotRequestAssertingClient(repository, snapshotName, indexName, RESTORED_INDEX_PREFIX)) {
            MountSnapshotStep step = new MountSnapshotStep(randomStepKey(), randomStepKey(), client, RESTORED_INDEX_PREFIX);
            step.performAction(indexMetadata, clusterState, null, new AsyncActionStep.Listener() {
                @Override
                public void onResponse(boolean complete) {
                    assertThat(complete, is(true));
                }

                @Override
                public void onFailure(Exception e) {
                    fail("expecting successful response but got: [" + e.getMessage() + "]");
                }
            });
        }
    }

    public void testResponseStatusHandling() {
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

        {
            RestoreSnapshotResponse responseWithOKStatus = new RestoreSnapshotResponse(new RestoreInfo("test", List.of(), 1, 1));
            try (NoOpClient clientPropagatingOKResponse = getClientTriggeringResponse(responseWithOKStatus)) {
                MountSnapshotStep step = new MountSnapshotStep(randomStepKey(), randomStepKey(), clientPropagatingOKResponse,
                    RESTORED_INDEX_PREFIX);
                step.performAction(indexMetadata, clusterState, null, new AsyncActionStep.Listener() {
                    @Override
                    public void onResponse(boolean complete) {
                        assertThat(complete, is(true));
                    }

                    @Override
                    public void onFailure(Exception e) {
                        fail("expecting successful response but got: [" + e.getMessage() + "]");
                    }
                });
            }
        }

        {
            RestoreSnapshotResponse responseWithACCEPTEDStatus = new RestoreSnapshotResponse((RestoreInfo) null);
            try (NoOpClient clientPropagatingACCEPTEDResponse = getClientTriggeringResponse(responseWithACCEPTEDStatus)) {
                MountSnapshotStep step = new MountSnapshotStep(randomStepKey(), randomStepKey(), clientPropagatingACCEPTEDResponse,
                    RESTORED_INDEX_PREFIX);
                step.performAction(indexMetadata, clusterState, null, new AsyncActionStep.Listener() {
                    @Override
                    public void onResponse(boolean complete) {
                        assertThat(complete, is(true));
                    }

                    @Override
                    public void onFailure(Exception e) {
                        fail("expecting successful response but got: [" + e.getMessage() + "]");
                    }
                });
            }
        }
    }

    @SuppressWarnings("unchecked")
    private NoOpClient getClientTriggeringResponse(RestoreSnapshotResponse response) {
        return new NoOpClient(getTestName()) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action,
                                                                                                      Request request,
                                                                                                      ActionListener<Response> listener) {
                listener.onResponse((Response) response);
            }
        };
    }

    @SuppressWarnings("unchecked")
    private NoOpClient getRestoreSnapshotRequestAssertingClient(String expectedRepoName, String expectedSnapshotName, String indexName,
                                                                String restoredIndexPrefix) {
        return new NoOpClient(getTestName()) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action,
                                                                                                      Request request,
                                                                                                      ActionListener<Response> listener) {
                assertThat(action.name(), is(MountSearchableSnapshotAction.NAME));
                assertTrue(request instanceof MountSearchableSnapshotRequest);
                MountSearchableSnapshotRequest mountSearchableSnapshotRequest = (MountSearchableSnapshotRequest) request;
                assertThat(mountSearchableSnapshotRequest.repositoryName(), is(expectedRepoName));
                assertThat(mountSearchableSnapshotRequest.snapshotName(), is(expectedSnapshotName));
                assertThat("another ILM step will wait for the restore to complete. the " + MountSnapshotStep.NAME + " step should not",
                    mountSearchableSnapshotRequest.waitForCompletion(), is(false));
                assertThat(mountSearchableSnapshotRequest.ignoreIndexSettings(), is(notNullValue()));
                assertThat(mountSearchableSnapshotRequest.ignoreIndexSettings()[0], is(LifecycleSettings.LIFECYCLE_NAME));
                assertThat(mountSearchableSnapshotRequest.mountedIndexName(), is(restoredIndexPrefix + indexName));
            }
        };
    }
}
