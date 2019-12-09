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
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.junit.Before;

import java.io.IOException;

import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class SetStepInfoUpdateTaskTests extends ESTestCase {

    String policy;
    ClusterState clusterState;
    Index index;

    @Before
    public void setupClusterState() {
        policy = randomAlphaOfLength(10);
        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        index = indexMetadata.getIndex();
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .put(IndexMetaData.builder(indexMetadata))
            .build();
        clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).build();
    }

    public void testExecuteSuccessfullySet() throws IOException {
        StepKey currentStepKey = new StepKey("current-phase", "current-action", "current-name");
        ToXContentObject stepInfo = getRandomStepInfo();
        setStateToKey(currentStepKey);

        SetStepInfoUpdateTask task = new SetStepInfoUpdateTask(index, policy, currentStepKey, stepInfo);
        ClusterState newState = task.execute(clusterState);
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(newState.getMetaData().index(index));
        StepKey actualKey = LifecycleExecutionState.getCurrentStepKey(lifecycleState);
        assertThat(actualKey, equalTo(currentStepKey));
        assertThat(lifecycleState.getPhaseTime(), nullValue());
        assertThat(lifecycleState.getActionTime(), nullValue());
        assertThat(lifecycleState.getStepTime(), nullValue());

        XContentBuilder infoXContentBuilder = JsonXContent.contentBuilder();
        stepInfo.toXContent(infoXContentBuilder, ToXContent.EMPTY_PARAMS);
        String expectedCauseValue = BytesReference.bytes(infoXContentBuilder).utf8ToString();
        assertThat(lifecycleState.getStepInfo(), equalTo(expectedCauseValue));
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

    public void testExecuteNoopDifferentStep() throws IOException {
        StepKey currentStepKey = new StepKey("current-phase", "current-action", "current-name");
        StepKey notCurrentStepKey = new StepKey("not-current", "not-current", "not-current");
        ToXContentObject stepInfo = getRandomStepInfo();
        setStateToKey(notCurrentStepKey);
        SetStepInfoUpdateTask task = new SetStepInfoUpdateTask(index, policy, currentStepKey, stepInfo);
        ClusterState newState = task.execute(clusterState);
        assertThat(newState, sameInstance(clusterState));
    }

    public void testExecuteNoopDifferentPolicy() throws IOException {
        StepKey currentStepKey = new StepKey("current-phase", "current-action", "current-name");
        ToXContentObject stepInfo = getRandomStepInfo();
        setStateToKey(currentStepKey);
        setStatePolicy("not-" + policy);
        SetStepInfoUpdateTask task = new SetStepInfoUpdateTask(index, policy, currentStepKey, stepInfo);
        ClusterState newState = task.execute(clusterState);
        assertThat(newState, sameInstance(clusterState));
    }

    public void testOnFailure() {
        StepKey currentStepKey = new StepKey("current-phase", "current-action", "current-name");
        ToXContentObject stepInfo = getRandomStepInfo();

        setStateToKey(currentStepKey);

        SetStepInfoUpdateTask task = new SetStepInfoUpdateTask(index, policy, currentStepKey, stepInfo);
        Exception expectedException = new RuntimeException();
        ElasticsearchException exception = expectThrows(ElasticsearchException.class,
                () -> task.onFailure(randomAlphaOfLength(10), expectedException));
        assertEquals("policy [" + policy + "] for index [" + index.getName() + "] failed trying to set step info for step ["
                + currentStepKey + "].", exception.getMessage());
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
