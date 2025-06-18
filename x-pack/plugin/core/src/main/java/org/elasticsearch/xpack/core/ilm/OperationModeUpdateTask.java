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
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.FixForMultiProject;
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

    private final ProjectId projectId;
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

    private OperationModeUpdateTask(Priority priority, ProjectId projectId, OperationMode ilmMode, OperationMode slmMode) {
        super(priority);
        this.projectId = projectId;
        this.ilmMode = ilmMode;
        this.slmMode = slmMode;
    }

    public static OperationModeUpdateTask ilmMode(ProjectId projectId, OperationMode mode) {
        return new OperationModeUpdateTask(getPriority(mode), projectId, mode, null);
    }

    public static OperationModeUpdateTask slmMode(OperationMode mode) {
        @FixForMultiProject // Use non-default ID when SLM has been made project-aware
        final var projectId = ProjectId.DEFAULT;
        return new OperationModeUpdateTask(getPriority(mode), projectId, null, mode);
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
        ProjectMetadata newProject = currentState.metadata().getProject(projectId);
        newProject = updateILMState(newProject);
        newProject = updateSLMState(newProject);
        if (newProject == currentState.metadata().getProject(projectId)) {
            return currentState;
        }
        return ClusterState.builder(currentState).putProjectMetadata(newProject).build();
    }

    private ProjectMetadata updateILMState(final ProjectMetadata currentProject) {
        if (ilmMode == null) {
            return currentProject;
        }

        final OperationMode currentMode = currentILMMode(currentProject);
        if (currentMode.equals(ilmMode)) {
            // No need for a new state
            return currentProject;
        }

        final OperationMode newMode;
        if (currentMode.isValidChange(ilmMode)) {
            newMode = ilmMode;
        } else {
            // The transition is invalid, return the current state
            return currentProject;
        }

        logger.info("updating ILM operation mode to {}", newMode);
        final var updatedMetadata = new LifecycleOperationMetadata(newMode, currentSLMMode(currentProject));
        return currentProject.copyAndUpdate(b -> b.putCustom(LifecycleOperationMetadata.TYPE, updatedMetadata));
    }

    private ProjectMetadata updateSLMState(final ProjectMetadata currentProject) {
        if (slmMode == null) {
            return currentProject;
        }

        final OperationMode currentMode = currentSLMMode(currentProject);
        if (currentMode.equals(slmMode)) {
            // No need for a new state
            return currentProject;
        }

        final OperationMode newMode;
        if (currentMode.isValidChange(slmMode)) {
            newMode = slmMode;
        } else {
            // The transition is invalid, return the current state
            return currentProject;
        }

        logger.info("updating SLM operation mode to {}", newMode);
        final var updatedMetadata = new LifecycleOperationMetadata(currentILMMode(currentProject), newMode);
        return currentProject.copyAndUpdate(b -> b.putCustom(LifecycleOperationMetadata.TYPE, updatedMetadata));
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
