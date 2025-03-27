/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;

import static org.elasticsearch.xpack.core.ilm.LifecycleOperationMetadata.currentILMMode;
import static org.elasticsearch.xpack.core.ilm.LifecycleOperationMetadata.currentSLMMode;

/**
 * This task updates the operation mode state for ILM.
 * <p>
 * As stopping ILM proved to be an action we want to sometimes take in order to allow clusters to stabilise when under heavy load this
 * task might run at {@link Priority#IMMEDIATE} priority so please make sure to keep this task as lightweight as possible.
 */
public class OperationModeUpdateTask extends ClusterStateUpdateTask {
    private static final Logger logger = LogManager.getLogger(OperationModeUpdateTask.class);
    @Nullable
    private final OperationMode ilmMode;
    @Nullable
    private final OperationMode slmMode;

    public static AckedClusterStateUpdateTask wrap(
        OperationModeUpdateTask task,
        AcknowledgedRequest<?> request,
        ActionListener<AcknowledgedResponse> listener
    ) {
        return new AckedClusterStateUpdateTask(task.priority(), request, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return task.execute(currentState);
            }
        };
    }

    private OperationModeUpdateTask(Priority priority, OperationMode ilmMode, OperationMode slmMode) {
        super(priority);
        this.ilmMode = ilmMode;
        this.slmMode = slmMode;
    }

    public static OperationModeUpdateTask ilmMode(OperationMode mode) {
        return new OperationModeUpdateTask(getPriority(mode), mode, null);
    }

    public static OperationModeUpdateTask slmMode(OperationMode mode) {
        return new OperationModeUpdateTask(getPriority(mode), null, mode);
    }

    private static Priority getPriority(OperationMode mode) {
        if (mode == OperationMode.STOPPED || mode == OperationMode.STOPPING) {
            return Priority.IMMEDIATE;
        } else {
            return Priority.NORMAL;
        }
    }

    public OperationMode getILMOperationMode() {
        return ilmMode;
    }

    public OperationMode getSLMOperationMode() {
        return slmMode;
    }

    @Override
    public ClusterState execute(ClusterState currentState) {
        ClusterState newState = currentState;
        newState = updateILMState(newState);
        newState = updateSLMState(newState);
        return newState;
    }

    private ClusterState updateILMState(final ClusterState currentState) {
        if (ilmMode == null) {
            return currentState;
        }

        final OperationMode currentMode = currentILMMode(currentState.metadata().getProject());
        if (currentMode.equals(ilmMode)) {
            // No need for a new state
            return currentState;
        }

        final OperationMode newMode;
        if (currentMode.isValidChange(ilmMode)) {
            newMode = ilmMode;
        } else {
            // The transition is invalid, return the current state
            return currentState;
        }

        logger.info("updating ILM operation mode to {}", newMode);
        return ClusterState.builder(currentState)
            .metadata(
                Metadata.builder(currentState.metadata())
                    .putCustom(LifecycleOperationMetadata.TYPE, new LifecycleOperationMetadata(newMode, currentSLMMode(currentState)))
            )
            .build();
    }

    private ClusterState updateSLMState(final ClusterState currentState) {
        if (slmMode == null) {
            return currentState;
        }

        final OperationMode currentMode = currentSLMMode(currentState);
        if (currentMode.equals(slmMode)) {
            // No need for a new state
            return currentState;
        }

        final OperationMode newMode;
        if (currentMode.isValidChange(slmMode)) {
            newMode = slmMode;
        } else {
            // The transition is invalid, return the current state
            return currentState;
        }

        logger.info("updating SLM operation mode to {}", newMode);
        return ClusterState.builder(currentState)
            .metadata(
                Metadata.builder(currentState.metadata())
                    .putCustom(
                        LifecycleOperationMetadata.TYPE,
                        new LifecycleOperationMetadata(currentILMMode(currentState.metadata().getProject()), newMode)
                    )
            )
            .build();
    }

    @Override
    public void onFailure(Exception e) {
        logger.error(
            () -> Strings.format("unable to update lifecycle metadata with new ilm mode [%s], slm mode [%s]", ilmMode, slmMode),
            e
        );
    }

    @Override
    public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
        if (ilmMode != null) {
            logger.info("ILM operation mode updated to {}", ilmMode);
        }
        if (slmMode != null) {
            logger.info("SLM operation mode updated to {}", slmMode);
        }
    }
}
