/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.MessageSupplier;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.ilm.Step;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

public class MoveToErrorStepUpdateTask extends ClusterStateUpdateTask {

    private static final Logger logger = LogManager.getLogger(MoveToErrorStepUpdateTask.class);

    private final Index index;
    private final String policy;
    private final Step.StepKey currentStepKey;
    private final BiFunction<IndexMetadata, Step.StepKey, Step> stepLookupFunction;
    private final Consumer<ClusterState> stateChangeConsumer;
    private final LongSupplier nowSupplier;
    private final Exception cause;

    public MoveToErrorStepUpdateTask(
        Index index,
        String policy,
        Step.StepKey currentStepKey,
        Exception cause,
        LongSupplier nowSupplier,
        BiFunction<IndexMetadata, Step.StepKey, Step> stepLookupFunction,
        Consumer<ClusterState> stateChangeConsumer
    ) {
        this.index = index;
        this.policy = policy;
        this.currentStepKey = currentStepKey;
        this.cause = cause;
        this.nowSupplier = nowSupplier;
        this.stepLookupFunction = stepLookupFunction;
        this.stateChangeConsumer = stateChangeConsumer;
    }

    @Override
    public ClusterState execute(ClusterState currentState) {
        IndexMetadata idxMeta = currentState.getMetadata().index(index);
        if (idxMeta == null) {
            // Index must have been since deleted, ignore it
            return currentState;
        }
        LifecycleExecutionState lifecycleState = idxMeta.getLifecycleExecutionState();
        if (policy.equals(idxMeta.getLifecyclePolicyName()) && currentStepKey.equals(Step.getCurrentStepKey(lifecycleState))) {
            return IndexLifecycleTransition.moveClusterStateToErrorStep(index, currentState, cause, nowSupplier, stepLookupFunction);
        } else {
            // either the policy has changed or the step is now
            // not the same as when we submitted the update task. In
            // either case we don't want to do anything now
            return currentState;
        }
    }

    @Override
    public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
        if (newState.equals(oldState) == false) {
            stateChangeConsumer.accept(newState);
        }
    }

    @Override
    public void onFailure(Exception e) {
        final MessageSupplier messageSupplier = () -> new ParameterizedMessage(
            "policy [{}] for index [{}] failed trying to move from step [{}] to the ERROR step.",
            policy,
            index.getName(),
            currentStepKey
        );
        if (ExceptionsHelper.unwrap(e, NotMasterException.class, FailedToCommitClusterStateException.class) != null) {
            logger.debug(messageSupplier, e);
        } else {
            logger.error(messageSupplier, e);
            assert false : new AssertionError("unexpected exception", e);
        }
    }
}
