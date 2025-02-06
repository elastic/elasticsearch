/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.client.internal.ClusterAdminClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xpack.core.ilm.step.info.EmptyInfo;
import org.elasticsearch.xpack.core.slm.SnapshotInvocationRecord;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

public class WaitForSnapshotStepTests extends AbstractStepTestCase<WaitForSnapshotStep> {

    private final ClusterAdminClient clusterAdminClient = mock(ClusterAdminClient.class);

    @Before
    public void setupClusterClient() {
        Mockito.when(adminClient.cluster()).thenReturn(clusterAdminClient);
    }

    @Override
    protected WaitForSnapshotStep createRandomInstance() {
        return new WaitForSnapshotStep(randomStepKey(), randomStepKey(), client, randomAlphaOfLengthBetween(1, 10));
    }

    @Override
    protected WaitForSnapshotStep mutateInstance(WaitForSnapshotStep instance) {
        Step.StepKey key = instance.getKey();
        Step.StepKey nextKey = instance.getNextStepKey();
        String policy = instance.getPolicy();

        switch (between(0, 2)) {
            case 0 -> key = new Step.StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
            case 1 -> nextKey = new Step.StepKey(nextKey.phase(), nextKey.action(), nextKey.name() + randomAlphaOfLength(5));
            case 2 -> policy = randomValueOtherThan(policy, () -> randomAlphaOfLengthBetween(1, 10));
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new WaitForSnapshotStep(key, nextKey, client, policy);
    }

    @Override
    protected WaitForSnapshotStep copyInstance(WaitForSnapshotStep instance) {
        return new WaitForSnapshotStep(instance.getKey(), instance.getNextStepKey(), client, instance.getPolicy());
    }

    public void testNoSlmPolicies() {
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, Map.of("action_time", Long.toString(randomLong())))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        Map<String, IndexMetadata> indices = Map.of(indexMetadata.getIndex().getName(), indexMetadata);
        Metadata.Builder meta = Metadata.builder().indices(indices);
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(meta).build();
        WaitForSnapshotStep instance = createRandomInstance();
        SetOnce<Exception> error = new SetOnce<>();
        instance.evaluateCondition(clusterState.metadata(), indexMetadata.getIndex(), new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject info) {
                logger.warn("expected an error got unexpected response {}", conditionMet);
                throw new AssertionError("unexpected method call");
            }

            @Override
            public void onFailure(Exception e) {
                error.set(e);
            }
        }, MASTER_TIMEOUT);

        assertThat(error.get().getMessage(), containsString("'" + instance.getPolicy() + "' not found"));
    }

    public void testSlmPolicyNotExecuted() {
        WaitForSnapshotStep instance = createRandomInstance();
        SnapshotLifecyclePolicyMetadata slmPolicy = SnapshotLifecyclePolicyMetadata.builder()
            .setModifiedDate(randomLong())
            .setPolicy(new SnapshotLifecyclePolicy("", "", "", "", null, null))
            .build();
        SnapshotLifecycleMetadata smlMetadata = new SnapshotLifecycleMetadata(
            Map.of(instance.getPolicy(), slmPolicy),
            OperationMode.RUNNING,
            null
        );

        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, Map.of("action_time", Long.toString(randomLong())))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        Map<String, IndexMetadata> indices = Map.of(indexMetadata.getIndex().getName(), indexMetadata);
        Metadata.Builder meta = Metadata.builder().indices(indices).putCustom(SnapshotLifecycleMetadata.TYPE, smlMetadata);
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(meta).build();
        SetOnce<Boolean> isConditionMet = new SetOnce<>();
        SetOnce<ToXContentObject> informationContext = new SetOnce<>();
        instance.evaluateCondition(clusterState.metadata(), indexMetadata.getIndex(), new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject info) {
                isConditionMet.set(conditionMet);
                informationContext.set(info);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("unexpected onFailure call", e);
                throw new AssertionError("unexpected method call");
            }
        }, MASTER_TIMEOUT);
        assertThat(isConditionMet.get(), is(false));
        assertTrue(toString(informationContext.get()).contains("to be executed"));
    }

    public void testSlmPolicyExecutedBeforeStep() throws IOException {
        // The snapshot was started and finished before the phase time, so we do not expect the step to finish:
        assertSlmPolicyExecuted(false, false);
    }

    public void testSlmPolicyExecutedAfterStep() {
        String repoName = randomAlphaOfLength(10);
        String snapshotName = randomAlphaOfLength(10);
        String indexName = randomAlphaOfLength(10);
        // The snapshot was started and finished after the phase time, so we do expect the step to finish:
        GetSnapshotsResponse response = new GetSnapshotsResponse(
            List.of(
                new SnapshotInfo(
                    new Snapshot(randomAlphaOfLength(10), new SnapshotId(snapshotName, randomAlphaOfLength(10))),
                    List.of(indexName),
                    List.of(),
                    List.of(),
                    SnapshotState.SUCCESS
                )
            ),
            null,
            0,
            0
        );
        Mockito.doAnswer(invocationOnMock -> {
            GetSnapshotsRequest request = (GetSnapshotsRequest) invocationOnMock.getArguments()[0];
            assertGetSnapshotRequest(repoName, snapshotName, request);
            @SuppressWarnings("unchecked")
            ActionListener<GetSnapshotsResponse> listener = (ActionListener<GetSnapshotsResponse>) invocationOnMock.getArguments()[1];
            listener.onResponse(response);
            return null;
        }).when(clusterAdminClient).getSnapshots(any(), any());

        assertSlmPolicyExecuted(repoName, snapshotName, indexName, true, true);
    }

    public void testSlmPolicyNotExecutedWhenStartIsBeforePhaseTime() throws IOException {
        // The snapshot was started before the phase time and finished after, so we do expect the step to finish:
        assertSlmPolicyExecuted(false, true);
    }

    public void testIndexNotBackedUpYet() {
        String repoName = randomAlphaOfLength(10);
        String snapshotName = randomAlphaOfLength(10);
        String indexName = randomAlphaOfLength(10);

        // The latest snapshot does not contain the index we are interested in
        GetSnapshotsResponse response = new GetSnapshotsResponse(
            List.of(
                new SnapshotInfo(
                    new Snapshot(randomAlphaOfLength(10), new SnapshotId(snapshotName, randomAlphaOfLength(10))),
                    List.of(),
                    List.of(),
                    List.of(),
                    SnapshotState.SUCCESS
                )
            ),
            null,
            0,
            0
        );
        Mockito.doAnswer(invocationOnMock -> {
            GetSnapshotsRequest request = (GetSnapshotsRequest) invocationOnMock.getArguments()[0];
            assertGetSnapshotRequest(repoName, snapshotName, request);
            @SuppressWarnings("unchecked")
            ActionListener<GetSnapshotsResponse> listener = (ActionListener<GetSnapshotsResponse>) invocationOnMock.getArguments()[1];
            listener.onResponse(response);
            return null;
        }).when(clusterAdminClient).getSnapshots(any(), any());

        long phaseTime = randomLongBetween(100, 100000);
        long actionTime = phaseTime + randomLongBetween(100, 100000);
        WaitForSnapshotStep instance = createRandomInstance();
        SnapshotLifecyclePolicyMetadata slmPolicy = SnapshotLifecyclePolicyMetadata.builder()
            .setModifiedDate(randomLong())
            .setPolicy(new SnapshotLifecyclePolicy("", "", "", repoName, null, null))
            .setLastSuccess(new SnapshotInvocationRecord(snapshotName, actionTime + 10, actionTime + 100, ""))
            .build();
        SnapshotLifecycleMetadata smlMetadata = new SnapshotLifecycleMetadata(
            Map.of(instance.getPolicy(), slmPolicy),
            OperationMode.RUNNING,
            null
        );

        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, Map.of("action_time", Long.toString(actionTime)))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        Map<String, IndexMetadata> indices = Map.of(indexMetadata.getIndex().getName(), indexMetadata);
        Metadata.Builder meta = Metadata.builder().indices(indices).putCustom(SnapshotLifecycleMetadata.TYPE, smlMetadata);
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(meta).build();
        SetOnce<Exception> error = new SetOnce<>();
        instance.evaluateCondition(clusterState.metadata(), indexMetadata.getIndex(), new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject info) {
                logger.warn("expected an error got unexpected response {}", conditionMet);
                throw new AssertionError("unexpected method call");
            }

            @Override
            public void onFailure(Exception e) {
                error.set(e);
            }
        }, MASTER_TIMEOUT);

        assertThat(error.get().getMessage(), containsString("does not include index '" + indexName + "'"));
    }

    private void assertSlmPolicyExecuted(boolean startTimeAfterPhaseTime, boolean finishTimeAfterPhaseTime) {
        assertSlmPolicyExecuted(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            startTimeAfterPhaseTime,
            finishTimeAfterPhaseTime
        );
    }

    private void assertSlmPolicyExecuted(
        String repoName,
        String snapshotName,
        String indexName,
        boolean startTimeAfterPhaseTime,
        boolean finishTimeAfterPhaseTime
    ) {
        long phaseTime = randomLong();

        WaitForSnapshotStep instance = createRandomInstance();
        SnapshotLifecyclePolicyMetadata slmPolicy = SnapshotLifecyclePolicyMetadata.builder()
            .setModifiedDate(randomLong())
            .setPolicy(new SnapshotLifecyclePolicy("", "", "", repoName, null, null))
            .setLastSuccess(
                new SnapshotInvocationRecord(
                    snapshotName,
                    phaseTime + (startTimeAfterPhaseTime ? 10 : -100),
                    phaseTime + (finishTimeAfterPhaseTime ? 100 : -10),
                    ""
                )
            )
            .build();
        SnapshotLifecycleMetadata smlMetadata = new SnapshotLifecycleMetadata(
            Map.of(instance.getPolicy(), slmPolicy),
            OperationMode.RUNNING,
            null
        );

        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, Map.of("action_time", Long.toString(phaseTime)))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        Map<String, IndexMetadata> indices = Map.of(indexMetadata.getIndex().getName(), indexMetadata);
        Metadata.Builder meta = Metadata.builder().indices(indices).putCustom(SnapshotLifecycleMetadata.TYPE, smlMetadata);
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(meta).build();
        SetOnce<Boolean> isConditionMet = new SetOnce<>();
        SetOnce<ToXContentObject> informationContext = new SetOnce<>();
        instance.evaluateCondition(clusterState.metadata(), indexMetadata.getIndex(), new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject info) {
                isConditionMet.set(conditionMet);
                informationContext.set(info);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("unexpected onFailure call", e);
                throw new AssertionError("unexpected method call");
            }
        }, MASTER_TIMEOUT);
        if (startTimeAfterPhaseTime) {
            assertThat(isConditionMet.get(), is(true));
            assertThat(informationContext.get(), is(EmptyInfo.INSTANCE));
        } else {
            assertThat(isConditionMet.get(), is(false));
            assertThat(toString(informationContext.get()), containsString("to be executed"));
        }
    }

    private void assertGetSnapshotRequest(String repoName, String snapshotName, GetSnapshotsRequest request) {
        assertThat(request.repositories().length, is(1));
        assertThat(request.repositories()[0], equalTo(repoName));
        assertThat(request.snapshots().length, is(1));
        assertThat(request.snapshots()[0], equalTo(snapshotName));
        assertThat(request.includeIndexNames(), is(true));
        assertThat(request.verbose(), is(false));
    }

    public void testNullStartTime() {
        long phaseTime = randomLong();

        WaitForSnapshotStep instance = createRandomInstance();
        SnapshotLifecyclePolicyMetadata slmPolicy = SnapshotLifecyclePolicyMetadata.builder()
            .setModifiedDate(randomLong())
            .setPolicy(new SnapshotLifecyclePolicy("", "", "", "", null, null))
            .setLastSuccess(new SnapshotInvocationRecord("", null, phaseTime + 100, ""))
            .build();
        SnapshotLifecycleMetadata smlMetadata = new SnapshotLifecycleMetadata(
            Map.of(instance.getPolicy(), slmPolicy),
            OperationMode.RUNNING,
            null
        );

        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, Map.of("phase_time", Long.toString(phaseTime)))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        Map<String, IndexMetadata> indices = Map.of(indexMetadata.getIndex().getName(), indexMetadata);
        Metadata.Builder meta = Metadata.builder().indices(indices).putCustom(SnapshotLifecycleMetadata.TYPE, smlMetadata);
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(meta).build();
        SetOnce<Exception> error = new SetOnce<>();
        instance.evaluateCondition(clusterState.metadata(), indexMetadata.getIndex(), new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject info) {
                logger.warn("expected an error got unexpected response {}", conditionMet);
                throw new AssertionError("unexpected method call");
            }

            @Override
            public void onFailure(Exception e) {
                error.set(e);
            }
        }, MASTER_TIMEOUT);

        assertThat(error.get().getMessage(), containsString("no information about ILM action start"));
    }

    private String toString(ToXContentObject info) {
        return Strings.toString(info);
    }
}
