/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.indexlifecycle.ClusterStateActionStep;
import org.elasticsearch.xpack.core.indexlifecycle.ClusterStateWaitStep;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.Step;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class MoveToNextStepUpdateTask extends ClusterStateUpdateTask {
    private static final Logger logger = ESLoggerFactory.getLogger(MoveToNextStepUpdateTask.class);
    private final Index index;
    private final String policy;
    private final Step.StepKey currentStepKey;
    private final Function<ClusterState, ClusterState> moveClusterStateToNextStep;
    private final Function<Settings, Step.StepKey> getCurrentStepKey;
    private final Consumer<ClusterState> runPolicy;

    public MoveToNextStepUpdateTask(Index index, String policy, Step.StepKey currentStepKey,
                                    Function<ClusterState, ClusterState> moveClusterStateToNextStep,
                                    Function<Settings, Step.StepKey> getCurrentStepKey,
                                    Consumer<ClusterState> runPolicy) {
        this.index = index;
        this.policy = policy;
        this.currentStepKey = currentStepKey;
        this.moveClusterStateToNextStep = moveClusterStateToNextStep;
        this.getCurrentStepKey = getCurrentStepKey;
        this.runPolicy = runPolicy;
    }

    @Override
    public ClusterState execute(ClusterState currentState) throws Exception {
        Settings indexSettings = currentState.getMetaData().index(index).getSettings();
        if (policy.equals(LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexSettings))
            && currentStepKey.equals(getCurrentStepKey.apply(indexSettings))) {
            return moveClusterStateToNextStep.apply(currentState);
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
            runPolicy.accept(newState);
        }
    }

    @Override
    public void onFailure(String source, Exception e) {
        throw new RuntimeException(e); // NORELEASE implement error handling
    }

}
