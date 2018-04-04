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
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.Step;

public class MoveToNextStepUpdateTask extends ClusterStateUpdateTask {
    private static final Logger logger = ESLoggerFactory.getLogger(MoveToNextStepUpdateTask.class);
    private final Index index;
    private final String policy;
    private final Step.StepKey currentStepKey;
    private final Step.StepKey nextStepKey;
    private final Listener listener;

    public MoveToNextStepUpdateTask(Index index, String policy, Step.StepKey currentStepKey, Step.StepKey nextStepKey,
                                    Listener listener) {
        this.index = index;
        this.policy = policy;
        this.currentStepKey = currentStepKey;
        this.nextStepKey = nextStepKey;
        this.listener = listener;
    }

    @Override
    public ClusterState execute(ClusterState currentState) throws Exception {
        Settings indexSettings = currentState.getMetaData().index(index).getSettings();
        if (policy.equals(LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexSettings))
            && currentStepKey.equals(IndexLifecycleRunner.getCurrentStepKey(indexSettings))) {
            return IndexLifecycleRunner.moveClusterStateToNextStep(index, currentState, nextStepKey);
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
        throw new RuntimeException(e); // NORELEASE implement error handling
    }

    @FunctionalInterface
    public interface Listener {
        void onClusterStateProcessed(ClusterState clusterState);
    }
}
