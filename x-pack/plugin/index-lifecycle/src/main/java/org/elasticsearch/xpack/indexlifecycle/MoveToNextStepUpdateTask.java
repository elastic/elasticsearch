/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.Step;

import java.util.function.LongSupplier;

public class MoveToNextStepUpdateTask extends ClusterStateUpdateTask {
    private final Index index;
    private final String policy;
    private final Step.StepKey currentStepKey;
    private final Step.StepKey nextStepKey;
    private final Listener listener;
    private final LongSupplier nowSupplier;
    private final PolicyStepsRegistry policyStepsRegistry;

    public MoveToNextStepUpdateTask(Index index, String policy, Step.StepKey currentStepKey, Step.StepKey nextStepKey,
            LongSupplier nowSupplier, PolicyStepsRegistry policyStepsRegistry, Listener listener) {
        this.index = index;
        this.policy = policy;
        this.currentStepKey = currentStepKey;
        this.nextStepKey = nextStepKey;
        this.nowSupplier = nowSupplier;
        this.listener = listener;
        this.policyStepsRegistry = policyStepsRegistry;
    }

    Index getIndex() {
        return index;
    }

    String getPolicy() {
        return policy;
    }

    Step.StepKey getCurrentStepKey() {
        return currentStepKey;
    }

    Step.StepKey getNextStepKey() {
        return nextStepKey;
    }

    @Override
    public ClusterState execute(ClusterState currentState) {
        Settings indexSettings = currentState.getMetaData().index(index).getSettings();
        if (policy.equals(LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexSettings))
            && currentStepKey.equals(IndexLifecycleRunner.getCurrentStepKey(indexSettings))) {
            return IndexLifecycleRunner.moveClusterStateToNextStep(index, currentState, currentStepKey, nextStepKey, nowSupplier);
        } else {
            // either the policy has changed or the step is now
            // not the same as when we submitted the update task. In
            // either case we don't want to do anything now
            return currentState;
        }
    }

    @Override
    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
        // if the new cluster state is different from the old one then
        // we moved to the new step in the execute method so we should
        // execute the next step
        if (oldState != newState) {
            listener.onClusterStateProcessed(newState);
        }
    }

    @Override
    public void onFailure(String source, Exception e) {
        throw new ElasticsearchException("policy [" + policy + "] for index [" + index.getName() + "] failed trying to move from step ["
                + currentStepKey + "] to step [" + nextStepKey + "].", e);
    }

    @FunctionalInterface
    public interface Listener {
        void onClusterStateProcessed(ClusterState clusterState);
    }
}
