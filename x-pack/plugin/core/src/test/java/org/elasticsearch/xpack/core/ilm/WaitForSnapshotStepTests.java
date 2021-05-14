/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.xpack.core.slm.SnapshotInvocationRecord;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;

import java.io.IOException;
import java.util.Map;

public class WaitForSnapshotStepTests extends AbstractStepTestCase<WaitForSnapshotStep> {

    @Override
    protected WaitForSnapshotStep createRandomInstance() {
        return new WaitForSnapshotStep(randomStepKey(), randomStepKey(), randomAlphaOfLengthBetween(1, 10));
    }

    @Override
    protected WaitForSnapshotStep mutateInstance(WaitForSnapshotStep instance) {
        Step.StepKey key = instance.getKey();
        Step.StepKey nextKey = instance.getNextStepKey();
        String policy = instance.getPolicy();

        switch (between(0, 2)) {
            case 0:
                key = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 1:
                nextKey = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 2:
                policy = randomValueOtherThan(policy, () -> randomAlphaOfLengthBetween(1, 10));
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }

        return new WaitForSnapshotStep(key, nextKey, policy);
    }

    @Override
    protected WaitForSnapshotStep copyInstance(WaitForSnapshotStep instance) {
        return new WaitForSnapshotStep(instance.getKey(), instance.getNextStepKey(), instance.getPolicy());
    }

    public void testNoSlmPolicies() {
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, Map.of("phase_time", Long.toString(randomLong())))
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ImmutableOpenMap.Builder<String, IndexMetadata> indices =
            ImmutableOpenMap.<String, IndexMetadata>builder().fPut(indexMetadata.getIndex().getName(), indexMetadata);
        Metadata.Builder meta = Metadata.builder().indices(indices.build());
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(meta).build();
        WaitForSnapshotStep instance = createRandomInstance();
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> instance.isConditionMet(indexMetadata.getIndex(),
            clusterState));
        assertTrue(e.getMessage().contains(instance.getPolicy()));
    }

    public void testSlmPolicyNotExecuted() throws IOException {
        WaitForSnapshotStep instance = createRandomInstance();
        SnapshotLifecyclePolicyMetadata slmPolicy = SnapshotLifecyclePolicyMetadata.builder()
            .setModifiedDate(randomLong())
            .setPolicy(new SnapshotLifecyclePolicy("", "", "", "", null, null))
            .build();
        SnapshotLifecycleMetadata smlMetadata = new SnapshotLifecycleMetadata(Map.of(instance.getPolicy(), slmPolicy),
            OperationMode.RUNNING, null);


        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, Map.of("phase_time", Long.toString(randomLong())))
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ImmutableOpenMap.Builder<String, IndexMetadata> indices =
            ImmutableOpenMap.<String, IndexMetadata>builder().fPut(indexMetadata.getIndex().getName(), indexMetadata);
        Metadata.Builder meta = Metadata.builder().indices(indices.build()).putCustom(SnapshotLifecycleMetadata.TYPE, smlMetadata);
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(meta).build();
        ClusterStateWaitStep.Result result = instance.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertFalse(result.isComplete());
        assertTrue(getMessage(result).contains("to be executed"));
    }

    public void testSlmPolicyExecutedBeforeStep() throws IOException {
        long phaseTime = randomLong();

        WaitForSnapshotStep instance = createRandomInstance();
        SnapshotLifecyclePolicyMetadata slmPolicy = SnapshotLifecyclePolicyMetadata.builder()
            .setModifiedDate(randomLong())
            .setPolicy(new SnapshotLifecyclePolicy("", "", "", "", null, null))
            .setLastSuccess(new SnapshotInvocationRecord("", phaseTime - 10, ""))
            .build();
        SnapshotLifecycleMetadata smlMetadata = new SnapshotLifecycleMetadata(Map.of(instance.getPolicy(), slmPolicy),
            OperationMode.RUNNING, null);

        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, Map.of("phase_time", Long.toString(phaseTime)))
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ImmutableOpenMap.Builder<String, IndexMetadata> indices =
            ImmutableOpenMap.<String, IndexMetadata>builder().fPut(indexMetadata.getIndex().getName(), indexMetadata);
        Metadata.Builder meta = Metadata.builder().indices(indices.build()).putCustom(SnapshotLifecycleMetadata.TYPE, smlMetadata);
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(meta).build();
        ClusterStateWaitStep.Result result = instance.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertFalse(result.isComplete());
        assertTrue(getMessage(result).contains("to be executed"));
    }

    public void testSlmPolicyExecutedAfterStep() throws IOException {
        long phaseTime = randomLong();

        WaitForSnapshotStep instance = createRandomInstance();
        SnapshotLifecyclePolicyMetadata slmPolicy = SnapshotLifecyclePolicyMetadata.builder()
            .setModifiedDate(randomLong())
            .setPolicy(new SnapshotLifecyclePolicy("", "", "", "", null, null))
            .setLastSuccess(new SnapshotInvocationRecord("", phaseTime + 10, ""))
            .build();
        SnapshotLifecycleMetadata smlMetadata = new SnapshotLifecycleMetadata(Map.of(instance.getPolicy(), slmPolicy),
            OperationMode.RUNNING, null);

        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, Map.of("phase_time", Long.toString(phaseTime)))
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ImmutableOpenMap.Builder<String, IndexMetadata> indices =
            ImmutableOpenMap.<String, IndexMetadata>builder().fPut(indexMetadata.getIndex().getName(), indexMetadata);
        Metadata.Builder meta = Metadata.builder().indices(indices.build()).putCustom(SnapshotLifecycleMetadata.TYPE, smlMetadata);
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(meta).build();
        ClusterStateWaitStep.Result result = instance.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertTrue(result.isComplete());
        assertNull(result.getInfomationContext());
    }

    private String getMessage(ClusterStateWaitStep.Result result) throws IOException {
        return Strings.toString(result.getInfomationContext());
    }
}
