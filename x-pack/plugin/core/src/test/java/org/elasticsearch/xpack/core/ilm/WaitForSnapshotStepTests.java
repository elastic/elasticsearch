/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.slm.SnapshotInvocationRecord;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
                policy = randomAlphaOfLengthBetween(1, 10);
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

    public void testNoSlmPolicies() throws IOException {
        IndexMetaData indexMetaData = IndexMetaData.builder(randomAlphaOfLength(10))
            .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, Map.of("phase_time", Long.toString(randomLong())))
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ImmutableOpenMap.Builder<String, IndexMetaData> indices =
            ImmutableOpenMap.<String, IndexMetaData>builder().fPut(indexMetaData.getIndex().getName(), indexMetaData);
        MetaData.Builder meta = MetaData.builder().indices(indices.build());
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(meta).build();
        WaitForSnapshotStep instance = createRandomInstance();
        ClusterStateWaitStep.Result result = instance.isConditionMet(indexMetaData.getIndex(), clusterState);
        assertFalse(result.isComplete());
        assertTrue(getMessage(result).contains("not found"));
    }

    public void testSlmPolicyNotExecuted() throws IOException {
        WaitForSnapshotStep instance = createRandomInstance();
        SnapshotLifecyclePolicyMetadata slmPolicy = SnapshotLifecyclePolicyMetadata.builder()
            .setModifiedDate(randomLong())
            .setPolicy(new SnapshotLifecyclePolicy("", "", "", "", null, null))
            .build();
        SnapshotLifecycleMetadata smlMetaData = new SnapshotLifecycleMetadata(Map.of(instance.getPolicy(), slmPolicy),
            OperationMode.RUNNING, null);


        IndexMetaData indexMetaData = IndexMetaData.builder(randomAlphaOfLength(10))
            .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, Map.of("phase_time", Long.toString(randomLong())))
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ImmutableOpenMap.Builder<String, IndexMetaData> indices =
            ImmutableOpenMap.<String, IndexMetaData>builder().fPut(indexMetaData.getIndex().getName(), indexMetaData);
        MetaData.Builder meta = MetaData.builder().indices(indices.build()).putCustom(SnapshotLifecycleMetadata.TYPE, smlMetaData);
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(meta).build();
        ClusterStateWaitStep.Result result = instance.isConditionMet(indexMetaData.getIndex(), clusterState);
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
        SnapshotLifecycleMetadata smlMetaData = new SnapshotLifecycleMetadata(Map.of(instance.getPolicy(), slmPolicy),
            OperationMode.RUNNING, null);

        IndexMetaData indexMetaData = IndexMetaData.builder(randomAlphaOfLength(10))
            .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, Map.of("phase_time", Long.toString(phaseTime)))
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ImmutableOpenMap.Builder<String, IndexMetaData> indices =
            ImmutableOpenMap.<String, IndexMetaData>builder().fPut(indexMetaData.getIndex().getName(), indexMetaData);
        MetaData.Builder meta = MetaData.builder().indices(indices.build()).putCustom(SnapshotLifecycleMetadata.TYPE, smlMetaData);
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(meta).build();
        ClusterStateWaitStep.Result result = instance.isConditionMet(indexMetaData.getIndex(), clusterState);
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
        SnapshotLifecycleMetadata smlMetaData = new SnapshotLifecycleMetadata(Map.of(instance.getPolicy(), slmPolicy),
            OperationMode.RUNNING, null);

        IndexMetaData indexMetaData = IndexMetaData.builder(randomAlphaOfLength(10))
            .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, Map.of("phase_time", Long.toString(phaseTime)))
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ImmutableOpenMap.Builder<String, IndexMetaData> indices =
            ImmutableOpenMap.<String, IndexMetaData>builder().fPut(indexMetaData.getIndex().getName(), indexMetaData);
        MetaData.Builder meta = MetaData.builder().indices(indices.build()).putCustom(SnapshotLifecycleMetadata.TYPE, smlMetaData);
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(meta).build();
        ClusterStateWaitStep.Result result = instance.isConditionMet(indexMetaData.getIndex(), clusterState);
        assertTrue(result.isComplete());
        assertNull(result.getInfomationContext());
    }

    private String getMessage(ClusterStateWaitStep.Result result) throws IOException {
        XContentBuilder s = result.getInfomationContext().toXContent(JsonXContent.contentBuilder(), null);
        s.flush();
        return new String(((ByteArrayOutputStream) s.getOutputStream()).toByteArray(), StandardCharsets.UTF_8);
    }
}
