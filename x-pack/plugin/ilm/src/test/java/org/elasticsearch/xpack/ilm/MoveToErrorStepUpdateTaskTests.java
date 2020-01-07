/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ilm;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ilm.ErrorStep;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyTests;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.MockStep;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class MoveToErrorStepUpdateTaskTests extends ESTestCase {

    String policy;
    ClusterState clusterState;
    Index index;

    @Before
    public void setupClusterState() {
        policy = randomAlphaOfLength(10);
        LifecyclePolicy lifecyclePolicy = LifecyclePolicyTests.randomTestLifecyclePolicy(policy);
        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        index = indexMetadata.getIndex();
        IndexLifecycleMetadata ilmMeta = new IndexLifecycleMetadata(
            Collections.singletonMap(policy, new LifecyclePolicyMetadata(lifecyclePolicy, Collections.emptyMap(),
                randomNonNegativeLong(), randomNonNegativeLong())),
            OperationMode.RUNNING);
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .put(IndexMetaData.builder(indexMetadata))
            .putCustom(IndexLifecycleMetadata.TYPE, ilmMeta)
            .build();
        clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).build();
    }

    public void testExecuteSuccessfullyMoved() throws IOException {
        StepKey currentStepKey = new StepKey("current-phase", "current-action", "current-name");
        StepKey nextStepKey = new StepKey("next-phase", "next-action", "next-step-name");
        long now = randomNonNegativeLong();
        Exception cause = new ElasticsearchException("THIS IS AN EXPECTED CAUSE");

        setStateToKey(currentStepKey);

        MoveToErrorStepUpdateTask task = new MoveToErrorStepUpdateTask(index, policy, currentStepKey, cause, () -> now,
            (idxMeta, stepKey) -> new MockStep(stepKey, nextStepKey), state -> {});
        ClusterState newState = task.execute(clusterState);
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(newState.getMetaData().index(index));
        StepKey actualKey = LifecycleExecutionState.getCurrentStepKey(lifecycleState);
        assertThat(actualKey, equalTo(new StepKey(currentStepKey.getPhase(), currentStepKey.getAction(), ErrorStep.NAME)));
        assertThat(lifecycleState.getFailedStep(), equalTo(currentStepKey.getName()));
        assertThat(lifecycleState.getPhaseTime(), nullValue());
        assertThat(lifecycleState.getActionTime(), nullValue());
        assertThat(lifecycleState.getStepTime(), equalTo(now));

        XContentBuilder causeXContentBuilder = JsonXContent.contentBuilder();
        causeXContentBuilder.startObject();
        ElasticsearchException.generateThrowableXContent(causeXContentBuilder, ToXContent.EMPTY_PARAMS, cause);
        causeXContentBuilder.endObject();
        String expectedCauseValue = BytesReference.bytes(causeXContentBuilder).utf8ToString();
        assertThat(lifecycleState.getStepInfo(),
            containsString("{\"type\":\"exception\",\"reason\":\"THIS IS AN EXPECTED CAUSE\",\"stack_trace\":\""));
    }

    public void testExecuteNoopDifferentStep() throws IOException {
        StepKey currentStepKey = new StepKey("current-phase", "current-action", "current-name");
        StepKey notCurrentStepKey = new StepKey("not-current", "not-current", "not-current");
        long now = randomNonNegativeLong();
        Exception cause = new ElasticsearchException("THIS IS AN EXPECTED CAUSE");
        setStateToKey(notCurrentStepKey);
        MoveToErrorStepUpdateTask task = new MoveToErrorStepUpdateTask(index, policy, currentStepKey, cause, () -> now,
            (idxMeta, stepKey) -> new MockStep(stepKey, new StepKey("next-phase", "action", "step")), state -> {});
        ClusterState newState = task.execute(clusterState);
        assertThat(newState, sameInstance(clusterState));
    }

    public void testExecuteNoopDifferentPolicy() throws IOException {
        StepKey currentStepKey = new StepKey("current-phase", "current-action", "current-name");
        long now = randomNonNegativeLong();
        Exception cause = new ElasticsearchException("THIS IS AN EXPECTED CAUSE");
        setStateToKey(currentStepKey);
        setStatePolicy("not-" + policy);
        MoveToErrorStepUpdateTask task = new MoveToErrorStepUpdateTask(index, policy, currentStepKey, cause, () -> now,
            (idxMeta, stepKey) -> new MockStep(stepKey, new StepKey("next-phase", "action", "step")), state -> {});
        ClusterState newState = task.execute(clusterState);
        assertThat(newState, sameInstance(clusterState));
    }

    public void testOnFailure() {
        StepKey currentStepKey = new StepKey("current-phase", "current-action", "current-name");
        long now = randomNonNegativeLong();
        Exception cause = new ElasticsearchException("THIS IS AN EXPECTED CAUSE");

        setStateToKey(currentStepKey);

        MoveToErrorStepUpdateTask task = new MoveToErrorStepUpdateTask(index, policy, currentStepKey, cause, () -> now,
            (idxMeta, stepKey) -> new MockStep(stepKey, new StepKey("next-phase", "action", "step")), state -> {});
        Exception expectedException = new RuntimeException();
        ElasticsearchException exception = expectThrows(ElasticsearchException.class,
                () -> task.onFailure(randomAlphaOfLength(10), expectedException));
        assertEquals("policy [" + policy + "] for index [" + index.getName() + "] failed trying to move from step [" + currentStepKey
                + "] to the ERROR step.", exception.getMessage());
        assertSame(expectedException, exception.getCause());
    }

    private void setStatePolicy(String policy) {
        clusterState = ClusterState.builder(clusterState)
            .metaData(MetaData.builder(clusterState.metaData())
                .updateSettings(Settings.builder()
                    .put(LifecycleSettings.LIFECYCLE_NAME, policy).build(), index.getName())).build();
    }
    private void setStateToKey(StepKey stepKey) {
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder(
            LifecycleExecutionState.fromIndexMetadata(clusterState.metaData().index(index)));
        lifecycleState.setPhase(stepKey.getPhase());
        lifecycleState.setAction(stepKey.getAction());
        lifecycleState.setStep(stepKey.getName());

        clusterState = ClusterState.builder(clusterState)
            .metaData(MetaData.builder(clusterState.getMetaData())
                .put(IndexMetaData.builder(clusterState.getMetaData().index(index))
                    .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap()))).build();
    }
}
