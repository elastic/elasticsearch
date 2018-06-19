/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

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
import org.elasticsearch.xpack.core.indexlifecycle.ErrorStep;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;
import org.junit.Before;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class MoveToErrorStepUpdateTaskTests extends ESTestCase {

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

    public void testExecuteSuccessfullyMoved() throws IOException {
        StepKey currentStepKey = new StepKey("current-phase", "current-action", "current-name");
        long now = randomNonNegativeLong();
        Exception cause = new ElasticsearchException("THIS IS AN EXPECTED CAUSE");

        setStateToKey(currentStepKey);

        MoveToErrorStepUpdateTask task = new MoveToErrorStepUpdateTask(index, policy, currentStepKey, cause, () -> now);
        ClusterState newState = task.execute(clusterState);
        StepKey actualKey = IndexLifecycleRunner.getCurrentStepKey(newState.metaData().index(index).getSettings());
        assertThat(actualKey, equalTo(new StepKey(currentStepKey.getPhase(), currentStepKey.getAction(), ErrorStep.NAME)));
        assertThat(LifecycleSettings.LIFECYCLE_FAILED_STEP_SETTING.get(newState.metaData().index(index).getSettings()),
                equalTo(currentStepKey.getName()));
        assertThat(LifecycleSettings.LIFECYCLE_PHASE_TIME_SETTING.get(newState.metaData().index(index).getSettings()), equalTo(-1L));
        assertThat(LifecycleSettings.LIFECYCLE_ACTION_TIME_SETTING.get(newState.metaData().index(index).getSettings()), equalTo(-1L));
        assertThat(LifecycleSettings.LIFECYCLE_STEP_TIME_SETTING.get(newState.metaData().index(index).getSettings()), equalTo(now));

        XContentBuilder causeXContentBuilder = JsonXContent.contentBuilder();
        causeXContentBuilder.startObject();
        ElasticsearchException.generateThrowableXContent(causeXContentBuilder, ToXContent.EMPTY_PARAMS, cause);
        causeXContentBuilder.endObject();
        String expectedCauseValue = BytesReference.bytes(causeXContentBuilder).utf8ToString();
        assertThat(LifecycleSettings.LIFECYCLE_STEP_INFO_SETTING.get(newState.metaData().index(index).getSettings()),
                equalTo(expectedCauseValue));
    }

    public void testExecuteNoopDifferentStep() throws IOException {
        StepKey currentStepKey = new StepKey("current-phase", "current-action", "current-name");
        StepKey notCurrentStepKey = new StepKey("not-current", "not-current", "not-current");
        long now = randomNonNegativeLong();
        Exception cause = new ElasticsearchException("THIS IS AN EXPECTED CAUSE");
        setStateToKey(notCurrentStepKey);
        MoveToErrorStepUpdateTask task = new MoveToErrorStepUpdateTask(index, policy, currentStepKey, cause, () -> now);
        ClusterState newState = task.execute(clusterState);
        assertThat(newState, sameInstance(clusterState));
    }

    public void testExecuteNoopDifferentPolicy() throws IOException {
        StepKey currentStepKey = new StepKey("current-phase", "current-action", "current-name");
        long now = randomNonNegativeLong();
        Exception cause = new ElasticsearchException("THIS IS AN EXPECTED CAUSE");
        setStateToKey(currentStepKey);
        setStatePolicy("not-" + policy);
        MoveToErrorStepUpdateTask task = new MoveToErrorStepUpdateTask(index, policy, currentStepKey, cause, () -> now);
        ClusterState newState = task.execute(clusterState);
        assertThat(newState, sameInstance(clusterState));
    }

    public void testOnFailure() {
        StepKey currentStepKey = new StepKey("current-phase", "current-action", "current-name");
        long now = randomNonNegativeLong();
        Exception cause = new ElasticsearchException("THIS IS AN EXPECTED CAUSE");

        setStateToKey(currentStepKey);

        MoveToErrorStepUpdateTask task = new MoveToErrorStepUpdateTask(index, policy, currentStepKey, cause, () -> now);
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
        clusterState = ClusterState.builder(clusterState)
            .metaData(MetaData.builder(clusterState.metaData())
                .updateSettings(Settings.builder()
                    .put(LifecycleSettings.LIFECYCLE_PHASE, stepKey.getPhase())
                    .put(LifecycleSettings.LIFECYCLE_ACTION, stepKey.getAction())
                    .put(LifecycleSettings.LIFECYCLE_STEP, stepKey.getName()).build(), index.getName())).build();
    }
}
