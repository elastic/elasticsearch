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
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusAction;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.core.ilm.OnAsyncWaitBranchingStep.BranchingStepListener;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction.NAME;
import static org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction.getCheckSnapshotStatusAsyncAction;
import static org.hamcrest.Matchers.is;

public class SearchableSnaposhotActionTests extends AbstractActionTestCase<SearchableSnapshotAction> {

    @Override
    public void testToSteps() {
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey expectedFirstStep = new StepKey(phase, NAME, WaitForNoFollowersStep.NAME);
        StepKey expectedSecondStep = new StepKey(phase, NAME, GenerateSnapshotNameStep.NAME);
        StepKey expectedThirdStep = new StepKey(phase, NAME, CleanupSnapshotStep.NAME);
        StepKey expectedFourthStep = new StepKey(phase, NAME, CreateSnapshotStep.NAME);
        StepKey expectedFifthStep = new StepKey(phase, NAME, OnAsyncWaitBranchingStep.NAME);
        StepKey expectedSixthStep = new StepKey(phase, NAME, MountSnapshotStep.NAME);
        StepKey expectedSeventhStep = new StepKey(phase, NAME, WaitForIndexColorStep.NAME);
        StepKey expectedEighthStep = new StepKey(phase, NAME, CopyExecutionStateStep.NAME);
        StepKey expectedNinthStep = new StepKey(phase, NAME, CopySettingsStep.NAME);
        StepKey expectedTenthStep = new StepKey(phase, NAME, SwapAliasesAndDeleteSourceIndexStep.NAME);

        SearchableSnapshotAction action = createTestInstance();
        StepKey nextStepKey = new StepKey(phase, randomAlphaOfLengthBetween(1, 5), randomAlphaOfLengthBetween(1, 5));

        List<Step> steps = action.toSteps(null, phase, nextStepKey);
        assertThat(steps.size(), is(10));

        assertThat(steps.get(0).getKey(), is(expectedFirstStep));
        assertThat(steps.get(1).getKey(), is(expectedSecondStep));
        assertThat(steps.get(2).getKey(), is(expectedThirdStep));
        assertThat(steps.get(3).getKey(), is(expectedFourthStep));
        assertThat(steps.get(4).getKey(), is(expectedFifthStep));
        assertThat(steps.get(5).getKey(), is(expectedSixthStep));
        assertThat(steps.get(6).getKey(), is(expectedSeventhStep));
        assertThat(steps.get(7).getKey(), is(expectedEighthStep));
        assertThat(steps.get(8).getKey(), is(expectedNinthStep));
        assertThat(steps.get(9).getKey(), is(expectedTenthStep));

        OnAsyncWaitBranchingStep branchStep = (OnAsyncWaitBranchingStep) steps.get(4);
        assertThat(branchStep.getNextStepKeyFulfilledWaitAction(), is(expectedSixthStep));
        assertThat(branchStep.getNextStepKeyUnfulfilledWaitAction(), is(expectedThirdStep));
    }

    public void testCheckSnapshotStatusActionReportsFailure() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";

        {
            IndexMetaData.Builder indexMetadataBuilder =
                IndexMetaData.builder(indexName).settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                    .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5));

            TriConsumer<Client, IndexMetaData, BranchingStepListener> checkSnapshotStatusAsyncAction =
                getCheckSnapshotStatusAsyncAction();

            try (NoOpClient client = new NoOpClient(getTestName())) {
                checkSnapshotStatusAsyncAction.apply(client, indexMetadataBuilder.build(), new BranchingStepListener() {
                    @Override
                    public void onStopWaitingAndMoveToNextKey(ToXContentObject informationContext) {

                    }

                    @Override
                    public void onResponse(boolean conditionMet, ToXContentObject informationContext) {
                    }

                    @Override
                    public void onFailure(Exception e) {
                        assertThat(e.getMessage(), is("snapshot repository is not present for policy [" + policyName +
                            "] and index [" + indexName + "]"));
                    }
                });
            }
        }

        {
            IndexMetaData.Builder indexMetadataBuilder =
                IndexMetaData.builder(indexName).settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                    .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5));
            Map<String, String> ilmCustom = Map.of("snapshot_repository", "repository_name");
            indexMetadataBuilder.putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, ilmCustom);

            TriConsumer<Client, IndexMetaData, BranchingStepListener> checkSnapshotStatusAsyncAction =
                getCheckSnapshotStatusAsyncAction();

            try (NoOpClient client = new NoOpClient(getTestName())) {
                checkSnapshotStatusAsyncAction.apply(client, indexMetadataBuilder.build(), new BranchingStepListener() {
                    @Override
                    public void onStopWaitingAndMoveToNextKey(ToXContentObject informationContext) {

                    }

                    @Override
                    public void onResponse(boolean conditionMet, ToXContentObject informationContext) {
                    }

                    @Override
                    public void onFailure(Exception e) {
                        assertThat(e.getMessage(), is("snapshot name was not generated for policy [" + policyName +
                            "] and index [" + indexName + "]"));
                    }
                });
            }
        }
    }

    public void testCheckSnapshotStatusAction() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";
        String snapshotName = "ilm-snapshot";
        String repoName = "repo_name";
        IndexMetaData.Builder indexMetadataBuilder =
            IndexMetaData.builder(indexName).settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5))
                .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, Map.of("repository_name", repoName, "snapshot_name",
                    snapshotName));

        TriConsumer<Client, IndexMetaData, BranchingStepListener> checkSnapshotStatusAsyncAction =
            getCheckSnapshotStatusAsyncAction();

        try (NoOpClient client = getSnapshotStatusRequestAssertingClient(repoName, snapshotName)) {
            checkSnapshotStatusAsyncAction.apply(client, indexMetadataBuilder.build(), new BranchingStepListener() {
                @Override
                public void onStopWaitingAndMoveToNextKey(ToXContentObject informationContext) {
                }

                @Override
                public void onResponse(boolean conditionMet, ToXContentObject informationContext) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            });
        }
    }

    private NoOpClient getSnapshotStatusRequestAssertingClient(String expectedRepoName, String expectedSnapshotName) {
        return new NoOpClient(getTestName()) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action,
                                                                                                      Request request,
                                                                                                      ActionListener<Response> listener) {
                assertThat(action.name(), is(SnapshotsStatusAction.NAME));
                assertTrue(request instanceof SnapshotsStatusRequest);
                SnapshotsStatusRequest snapshotsStatusRequest = (SnapshotsStatusRequest) request;
                assertThat(snapshotsStatusRequest.repository(), is(expectedRepoName));
                assertThat(snapshotsStatusRequest.snapshots(), is(new String[]{expectedSnapshotName}));
            }
        };
    }


    @Override
    protected SearchableSnapshotAction doParseInstance(XContentParser parser) throws IOException {
        return SearchableSnapshotAction.parse(parser);
    }

    @Override
    protected SearchableSnapshotAction createTestInstance() {
        return randomInstance();
    }

    @Override
    protected Writeable.Reader<SearchableSnapshotAction> instanceReader() {
        return SearchableSnapshotAction::new;
    }

    @Override
    protected SearchableSnapshotAction mutateInstance(SearchableSnapshotAction instance) throws IOException {
        return randomInstance();
    }

    static SearchableSnapshotAction randomInstance() {
        return new SearchableSnapshotAction(randomAlphaOfLengthBetween(5, 10));
    }
}
