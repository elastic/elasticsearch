/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;
import org.junit.Before;

import static org.hamcrest.Matchers.equalTo;

public class MoveToNextStepUpdateTaskTests extends ESTestCase {

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

    public void testExecuteSuccessfullyMoved() {
        StepKey currentStepKey = new StepKey("current-phase", "current-action", "current-name");
        StepKey nextStepKey = new StepKey("next-phase", "next-action", "next-name");

        setStateToKey(currentStepKey);

        SetOnce<Boolean> changed = new SetOnce<>();
        MoveToNextStepUpdateTask.Listener listener = (c) -> changed.set(true);
        MoveToNextStepUpdateTask task = new MoveToNextStepUpdateTask(index, policy, currentStepKey, nextStepKey, listener);
        ClusterState newState = task.execute(clusterState);
        StepKey actualKey = IndexLifecycleRunner.getCurrentStepKey(newState.metaData().index(index).getSettings());
        assertThat(actualKey, equalTo(nextStepKey));
        task.clusterStateProcessed("source", clusterState, newState);
        assertTrue(changed.get());
    }

    public void testExecuteNoop() {
        StepKey currentStepKey = new StepKey("current-phase", "current-action", "current-name");
        StepKey notCurrentStepKey = new StepKey("not-current", "not-current", "not-current");
        setStateToKey(currentStepKey);
        if (randomBoolean()) {
            setStateToKey(notCurrentStepKey);
        } else {
            setStatePolicy("not-" + policy);
        }
        MoveToNextStepUpdateTask.Listener listener = (c) -> {};
        MoveToNextStepUpdateTask task = new MoveToNextStepUpdateTask(index, policy, currentStepKey, null, listener);
        ClusterState newState = task.execute(clusterState);
        assertThat(newState, equalTo(clusterState));
    }

    public void testClusterProcessedWithNoChange() {
        StepKey currentStepKey = new StepKey("current-phase", "current-action", "current-name");
        setStateToKey(currentStepKey);
        SetOnce<Boolean> changed = new SetOnce<>();
        MoveToNextStepUpdateTask.Listener listener = (c) -> changed.set(true);
        MoveToNextStepUpdateTask task = new MoveToNextStepUpdateTask(index, policy, currentStepKey, null, listener);
        task.clusterStateProcessed("source", clusterState, clusterState);
        assertNull(changed.get());
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
