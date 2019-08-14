/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ilm;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyTests;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.hamcrest.Matchers.equalTo;

public class MoveToNextStepUpdateTaskTests extends ESTestCase {

    String policy;
    ClusterState clusterState;
    Index index;
    LifecyclePolicy lifecyclePolicy;

    @Before
    public void setupClusterState() {
        policy = randomAlphaOfLength(10);
        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        index = indexMetadata.getIndex();
        lifecyclePolicy = LifecyclePolicyTests.randomTestLifecyclePolicy(policy);
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

    public void testExecuteSuccessfullyMoved() {
        long now = randomNonNegativeLong();
        List<Step> steps = lifecyclePolicy.toSteps(null);
        StepKey currentStepKey = steps.get(0).getKey();
        StepKey nextStepKey = steps.get(0).getNextStepKey();

        setStateToKey(currentStepKey, now);

        AtomicBoolean changed = new AtomicBoolean(false);
        MoveToNextStepUpdateTask task = new MoveToNextStepUpdateTask(index, policy, currentStepKey, nextStepKey,
            () -> now, state -> changed.set(true));
        ClusterState newState = task.execute(clusterState);
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(newState.getMetaData().index(index));
        StepKey actualKey = IndexLifecycleRunner.getCurrentStepKey(lifecycleState);
        assertThat(actualKey, equalTo(nextStepKey));
        assertThat(lifecycleState.getPhaseTime(), equalTo(now));
        assertThat(lifecycleState.getActionTime(), equalTo(now));
        assertThat(lifecycleState.getStepTime(), equalTo(now));
        task.clusterStateProcessed("source", clusterState, newState);
        assertTrue(changed.get());
    }

    public void testExecuteDifferentCurrentStep() {
        StepKey currentStepKey = new StepKey("current-phase", "current-action", "current-name");
        StepKey notCurrentStepKey = new StepKey("not-current", "not-current", "not-current");
        long now = randomNonNegativeLong();
        setStateToKey(notCurrentStepKey, now);
        MoveToNextStepUpdateTask task = new MoveToNextStepUpdateTask(index, policy, currentStepKey, null, () -> now, null);
        ClusterState newState = task.execute(clusterState);
        assertSame(newState, clusterState);
    }

    public void testExecuteDifferentPolicy() {
        StepKey currentStepKey = new StepKey("current-phase", "current-action", "current-name");
        long now = randomNonNegativeLong();
        setStateToKey(currentStepKey, now);
        setStatePolicy("not-" + policy);
        MoveToNextStepUpdateTask task = new MoveToNextStepUpdateTask(index, policy, currentStepKey, null, () -> now, null);
        ClusterState newState = task.execute(clusterState);
        assertSame(newState, clusterState);
    }

    public void testExecuteSuccessfulMoveWithInvalidNextStep() {
        long now = randomNonNegativeLong();
        List<Step> steps = lifecyclePolicy.toSteps(null);
        StepKey currentStepKey = steps.get(0).getKey();
        StepKey invalidNextStep = new StepKey("next-invalid", "next-invalid", "next-invalid");

        setStateToKey(currentStepKey, now);

        SetOnce<Boolean> changed = new SetOnce<>();
        MoveToNextStepUpdateTask task = new MoveToNextStepUpdateTask(index, policy, currentStepKey,
            invalidNextStep, () -> now, s -> changed.set(true));
        ClusterState newState = task.execute(clusterState);
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(newState.getMetaData().index(index));
        StepKey actualKey = IndexLifecycleRunner.getCurrentStepKey(lifecycleState);
        assertThat(actualKey, equalTo(invalidNextStep));
        assertThat(lifecycleState.getPhaseTime(), equalTo(now));
        assertThat(lifecycleState.getActionTime(), equalTo(now));
        assertThat(lifecycleState.getStepTime(), equalTo(now));
        task.clusterStateProcessed("source", clusterState, newState);
        assertTrue(changed.get());
    }

    public void testOnFailure() {
        StepKey currentStepKey = new StepKey("current-phase", "current-action", "current-name");
        StepKey nextStepKey = new StepKey("next-phase", "next-action", "next-name");
        long now = randomNonNegativeLong();

        setStateToKey(currentStepKey, now);

        MoveToNextStepUpdateTask task = new MoveToNextStepUpdateTask(index, policy, currentStepKey, nextStepKey, () -> now, state -> {});
        Exception expectedException = new RuntimeException();
        ElasticsearchException exception = expectThrows(ElasticsearchException.class,
                () -> task.onFailure(randomAlphaOfLength(10), expectedException));
        assertEquals("policy [" + policy + "] for index [" + index.getName() + "] failed trying to move from step [" + currentStepKey
                + "] to step [" + nextStepKey + "].", exception.getMessage());
        assertSame(expectedException, exception.getCause());
    }

    private void setStatePolicy(String policy) {
        clusterState = ClusterState.builder(clusterState)
            .metaData(MetaData.builder(clusterState.metaData())
                .updateSettings(Settings.builder()
                    .put(LifecycleSettings.LIFECYCLE_NAME, policy).build(), index.getName())).build();

    }
    private void setStateToKey(StepKey stepKey, long now) {
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder(
            LifecycleExecutionState.fromIndexMetadata(clusterState.metaData().index(index)));
        lifecycleState.setPhase(stepKey.getPhase());
        lifecycleState.setPhaseTime(now);
        lifecycleState.setAction(stepKey.getAction());
        lifecycleState.setActionTime(now);
        lifecycleState.setStep(stepKey.getName());
        lifecycleState.setStepTime(now);
        lifecycleState.setPhaseDefinition("{\"actions\":{\"TEST_ACTION\":{}}}");
        clusterState = ClusterState.builder(clusterState)
            .metaData(MetaData.builder(clusterState.getMetaData())
                .put(IndexMetaData.builder(clusterState.getMetaData().index(index))
                    .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap()))).build();
    }
}
