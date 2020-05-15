/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Step;

import java.util.function.Consumer;
import java.util.function.LongSupplier;

public class MoveToNextStepUpdateTask extends ClusterStateUpdateTask {
    private static final Logger logger = LogManager.getLogger(MoveToNextStepUpdateTask.class);

    private final Index index;
    private final String policy;
    private final Step.StepKey currentStepKey;
    private final Step.StepKey nextStepKey;
    private final LongSupplier nowSupplier;
    private final PolicyStepsRegistry stepRegistry;
    private final Consumer<ClusterState> stateChangeConsumer;

    public MoveToNextStepUpdateTask(Index index, String policy, Step.StepKey currentStepKey, Step.StepKey nextStepKey,
                                    LongSupplier nowSupplier, PolicyStepsRegistry stepRegistry,
                                    Consumer<ClusterState> stateChangeConsumer) {
        this.index = index;
        this.policy = policy;
        this.currentStepKey = currentStepKey;
        this.nextStepKey = nextStepKey;
        this.nowSupplier = nowSupplier;
        this.stepRegistry = stepRegistry;
        this.stateChangeConsumer = stateChangeConsumer;
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
        IndexMetadata indexMetadata = currentState.getMetadata().index(index);
        if (indexMetadata == null) {
            // Index must have been since deleted, ignore it
            return currentState;
        }
        Settings indexSettings = indexMetadata.getSettings();
        LifecycleExecutionState indexILMData = LifecycleExecutionState.fromIndexMetadata(currentState.getMetadata().index(index));
        if (policy.equals(LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexSettings))
            && currentStepKey.equals(LifecycleExecutionState.getCurrentStepKey(indexILMData))) {
            logger.trace("moving [{}] to next step ({})", index.getName(), nextStepKey);
            return IndexLifecycleTransition.moveClusterStateToStep(index, currentState, nextStepKey, nowSupplier, stepRegistry, false);
        } else {
            // either the policy has changed or the step is now
            // not the same as when we submitted the update task. In
            // either case we don't want to do anything now
            return currentState;
        }
    }

    @Override
    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
        if (oldState.equals(newState) == false) {
            stateChangeConsumer.accept(newState);
        }
    }

    @Override
    public void onFailure(String source, Exception e) {
        throw new ElasticsearchException("policy [" + policy + "] for index [" + index.getName() + "] failed trying to move from step ["
                + currentStepKey + "] to step [" + nextStepKey + "].", e);
    }
}
