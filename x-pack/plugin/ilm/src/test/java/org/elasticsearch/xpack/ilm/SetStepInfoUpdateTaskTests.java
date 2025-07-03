/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.junit.Before;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class SetStepInfoUpdateTaskTests extends ESTestCase {

    String policy;
    ProjectState state;
    Index index;

    @Before
    public void setupClusterState() {
        policy = randomAlphaOfLength(10);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policy))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        index = indexMetadata.getIndex();
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).put(IndexMetadata.builder(indexMetadata)).build();
        state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(project).build().projectState(project.id());
    }

    public void testExecuteSuccessfullySet() throws Exception {
        StepKey currentStepKey = new StepKey("current-phase", "current-action", "current-name");
        ToXContentObject stepInfo = getRandomStepInfo();
        setStateToKey(currentStepKey);

        SetStepInfoUpdateTask task = new SetStepInfoUpdateTask(state.projectId(), index, policy, currentStepKey, stepInfo);
        ClusterState newState = task.execute(state);
        LifecycleExecutionState lifecycleState = newState.metadata()
            .getProject(state.projectId())
            .index(index)
            .getLifecycleExecutionState();
        StepKey actualKey = Step.getCurrentStepKey(lifecycleState);
        assertThat(actualKey, equalTo(currentStepKey));
        assertThat(lifecycleState.phaseTime(), nullValue());
        assertThat(lifecycleState.actionTime(), nullValue());
        assertThat(lifecycleState.stepTime(), nullValue());

        XContentBuilder infoXContentBuilder = JsonXContent.contentBuilder();
        stepInfo.toXContent(infoXContentBuilder, ToXContent.EMPTY_PARAMS);
        String expectedCauseValue = BytesReference.bytes(infoXContentBuilder).utf8ToString();
        assertThat(lifecycleState.stepInfo(), equalTo(expectedCauseValue));
    }

    private ToXContentObject getRandomStepInfo() {
        String key = randomAlphaOfLength(20);
        String value = randomAlphaOfLength(20);
        return (b, p) -> {
            b.startObject();
            b.field(key, value);
            b.endObject();
            return b;
        };
    }

    public void testExecuteNoopDifferentStep() throws Exception {
        StepKey currentStepKey = new StepKey("current-phase", "current-action", "current-name");
        StepKey notCurrentStepKey = new StepKey("not-current", "not-current", "not-current");
        ToXContentObject stepInfo = getRandomStepInfo();
        setStateToKey(notCurrentStepKey);
        SetStepInfoUpdateTask task = new SetStepInfoUpdateTask(state.projectId(), index, policy, currentStepKey, stepInfo);
        ClusterState newState = task.execute(state);
        assertThat(newState, sameInstance(state.cluster()));
    }

    public void testExecuteNoopDifferentPolicy() throws Exception {
        StepKey currentStepKey = new StepKey("current-phase", "current-action", "current-name");
        ToXContentObject stepInfo = getRandomStepInfo();
        setStateToKey(currentStepKey);
        setStatePolicy("not-" + policy);
        SetStepInfoUpdateTask task = new SetStepInfoUpdateTask(state.projectId(), index, policy, currentStepKey, stepInfo);
        ClusterState newState = task.execute(state);
        assertThat(newState, sameInstance(state.cluster()));
    }

    @TestLogging(reason = "logging test", value = "logger.org.elasticsearch.xpack.ilm.SetStepInfoUpdateTask:WARN")
    public void testOnFailure() throws IllegalAccessException {
        StepKey currentStepKey = new StepKey("current-phase", "current-action", "current-name");
        ToXContentObject stepInfo = getRandomStepInfo();

        setStateToKey(currentStepKey);

        SetStepInfoUpdateTask task = new SetStepInfoUpdateTask(state.projectId(), index, policy, currentStepKey, stepInfo);

        try (var mockLog = MockLog.capture(SetStepInfoUpdateTask.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "warning",
                    SetStepInfoUpdateTask.class.getCanonicalName(),
                    Level.WARN,
                    "*policy [" + policy + "] for index [" + index + "] failed trying to set step info for step [" + currentStepKey + "]."
                )
            );

            task.onFailure(new RuntimeException("test exception"));
            mockLog.assertAllExpectationsMatched();
        }
    }

    private void setStatePolicy(String policyValue) {
        state = state.updateProject(
            ProjectMetadata.builder(state.metadata())
                .updateSettings(Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policyValue).build(), index.getName())
                .build()
        );
    }

    private void setStateToKey(StepKey stepKey) {
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder(
            state.metadata().index(index).getLifecycleExecutionState()
        );
        lifecycleState.setPhase(stepKey.phase());
        lifecycleState.setAction(stepKey.action());
        lifecycleState.setStep(stepKey.name());

        state = state.updateProject(
            ProjectMetadata.builder(state.metadata())
                .put(
                    IndexMetadata.builder(state.metadata().index(index)).putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
                )
                .build()
        );
    }
}
