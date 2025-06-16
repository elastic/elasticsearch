/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.ilm.Step;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import static org.elasticsearch.core.Strings.format;

public class MoveToNextStepUpdateTask extends IndexLifecycleClusterStateUpdateTask {

    private static final Logger logger = LogManager.getLogger(MoveToNextStepUpdateTask.class);

    private final String policy;
    private final Step.StepKey nextStepKey;
    private final LongSupplier nowSupplier;
    private final PolicyStepsRegistry stepRegistry;
    private final Consumer<ProjectState> stateChangeConsumer;

    public MoveToNextStepUpdateTask(
        Index index,
        String policy,
        Step.StepKey currentStepKey,
        Step.StepKey nextStepKey,
        LongSupplier nowSupplier,
        PolicyStepsRegistry stepRegistry,
        Consumer<ProjectState> stateChangeConsumer
    ) {
        super(index, currentStepKey);
        this.policy = policy;
        this.nextStepKey = nextStepKey;
        this.nowSupplier = nowSupplier;
        this.stepRegistry = stepRegistry;
        this.stateChangeConsumer = stateChangeConsumer;
    }

    @Override
    public ClusterState doExecute(ProjectState currentState) {
        IndexMetadata idxMeta = currentState.metadata().index(index);
        if (idxMeta == null) {
            // Index must have been since deleted, ignore it
            return currentState.cluster();
        }
        LifecycleExecutionState lifecycleState = idxMeta.getLifecycleExecutionState();
        if (policy.equals(idxMeta.getLifecyclePolicyName()) && currentStepKey.equals(Step.getCurrentStepKey(lifecycleState))) {
            logger.trace("moving [{}] to next step ({})", index.getName(), nextStepKey);
            return currentState.updatedState(
                IndexLifecycleTransition.moveIndexToStep(index, currentState.metadata(), nextStepKey, nowSupplier, stepRegistry, false)
            );
        } else {
            // either the policy has changed or the step is now
            // not the same as when we submitted the update task. In
            // either case we don't want to do anything now
            return currentState.cluster();
        }
    }

    @Override
    public void onClusterStateProcessed(ProjectState newState) {
        stateChangeConsumer.accept(newState);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MoveToNextStepUpdateTask that = (MoveToNextStepUpdateTask) o;
        return index.equals(that.index)
            && policy.equals(that.policy)
            && currentStepKey.equals(that.currentStepKey)
            && nextStepKey.equals(that.nextStepKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, policy, currentStepKey, nextStepKey);
    }

    @Override
    public void handleFailure(Exception e) {
        logger.warn(
            () -> format(
                "policy [%s] for index [%s] failed trying to move from step [%s] to step [%s].",
                policy,
                index,
                currentStepKey,
                nextStepKey
            ),
            e
        );
    }
}
