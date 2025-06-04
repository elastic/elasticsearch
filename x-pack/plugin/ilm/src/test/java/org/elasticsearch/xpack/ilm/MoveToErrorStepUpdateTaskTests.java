/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ilm;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ilm.ErrorStep;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyTests;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.MockStep;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.junit.Before;

import java.util.Map;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
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
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policy))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        index = indexMetadata.getIndex();
        IndexLifecycleMetadata ilmMeta = new IndexLifecycleMetadata(
            Map.of(policy, new LifecyclePolicyMetadata(lifecyclePolicy, Map.of(), randomNonNegativeLong(), randomNonNegativeLong())),
            OperationMode.RUNNING
        );
        Metadata metadata = Metadata.builder()
            .persistentSettings(settings(IndexVersion.current()).build())
            .put(IndexMetadata.builder(indexMetadata))
            .putCustom(IndexLifecycleMetadata.TYPE, ilmMeta)
            .build();
        clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
    }

    public void testExecuteSuccessfullyMoved() throws Exception {
        StepKey currentStepKey = new StepKey("current-phase", "current-action", "current-name");
        StepKey nextStepKey = new StepKey("next-phase", "next-action", "next-step-name");
        long now = randomNonNegativeLong();
        Exception cause = new ElasticsearchException("THIS IS AN EXPECTED CAUSE");

        setStateToKey(currentStepKey);

        MoveToErrorStepUpdateTask task = new MoveToErrorStepUpdateTask(
            index,
            policy,
            currentStepKey,
            cause,
            () -> now,
            (idxMeta, stepKey) -> new MockStep(stepKey, nextStepKey),
            state -> {}
        );
        ClusterState newState = task.execute(clusterState);
        LifecycleExecutionState lifecycleState = newState.getMetadata().getProject().index(index).getLifecycleExecutionState();
        StepKey actualKey = Step.getCurrentStepKey(lifecycleState);
        assertThat(actualKey, equalTo(new StepKey(currentStepKey.phase(), currentStepKey.action(), ErrorStep.NAME)));
        assertThat(lifecycleState.failedStep(), equalTo(currentStepKey.name()));
        assertThat(lifecycleState.phaseTime(), nullValue());
        assertThat(lifecycleState.actionTime(), nullValue());
        assertThat(lifecycleState.stepTime(), equalTo(now));

        assertThat(lifecycleState.stepInfo(), containsString("""
            {"type":"exception","reason":"THIS IS AN EXPECTED CAUSE\""""));
    }

    public void testExecuteNoopDifferentStep() throws Exception {
        StepKey currentStepKey = new StepKey("current-phase", "current-action", "current-name");
        StepKey notCurrentStepKey = new StepKey("not-current", "not-current", "not-current");
        long now = randomNonNegativeLong();
        Exception cause = new ElasticsearchException("THIS IS AN EXPECTED CAUSE");
        setStateToKey(notCurrentStepKey);
        MoveToErrorStepUpdateTask task = new MoveToErrorStepUpdateTask(
            index,
            policy,
            currentStepKey,
            cause,
            () -> now,
            (idxMeta, stepKey) -> new MockStep(stepKey, new StepKey("next-phase", "action", "step")),
            state -> {}
        );
        ClusterState newState = task.doExecute(clusterState);
        assertThat(newState, sameInstance(clusterState));
    }

    public void testExecuteNoopDifferentPolicy() throws Exception {
        StepKey currentStepKey = new StepKey("current-phase", "current-action", "current-name");
        long now = randomNonNegativeLong();
        Exception cause = new ElasticsearchException("THIS IS AN EXPECTED CAUSE");
        setStateToKey(currentStepKey);
        setStatePolicy("not-" + policy);
        MoveToErrorStepUpdateTask task = new MoveToErrorStepUpdateTask(
            index,
            policy,
            currentStepKey,
            cause,
            () -> now,
            (idxMeta, stepKey) -> new MockStep(stepKey, new StepKey("next-phase", "action", "step")),
            state -> {}
        );
        ClusterState newState = task.doExecute(clusterState);
        assertThat(newState, sameInstance(clusterState));
    }

    private void setStatePolicy(String policyValue) {
        clusterState = ClusterState.builder(clusterState)
            .metadata(
                Metadata.builder(clusterState.metadata())
                    .updateSettings(Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policyValue).build(), index.getName())
            )
            .build();
    }

    private void setStateToKey(StepKey stepKey) {
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder(
            clusterState.metadata().getProject().index(index).getLifecycleExecutionState()
        );
        lifecycleState.setPhase(stepKey.phase());
        lifecycleState.setAction(stepKey.action());
        lifecycleState.setStep(stepKey.name());

        clusterState = ClusterState.builder(clusterState)
            .metadata(
                Metadata.builder(clusterState.getMetadata())
                    .put(
                        IndexMetadata.builder(clusterState.getMetadata().getProject().index(index))
                            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
                    )
            )
            .build();
    }
}
