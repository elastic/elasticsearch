/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reservedstate.service;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectStateRegistry;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.reservedstate.ReservedProjectStateHandler;
import org.elasticsearch.reservedstate.TransformState;

import java.util.Map;
import java.util.Optional;
import java.util.SequencedCollection;
import java.util.function.Consumer;

public class ReservedProjectStateUpdateTask extends ReservedStateUpdateTask<ReservedProjectStateHandler<?>> {
    private final ProjectId projectId;

    public ReservedProjectStateUpdateTask(
        ProjectId projectId,
        String namespace,
        ReservedStateChunk stateChunk,
        ReservedStateVersionCheck versionCheck,
        Map<String, ReservedProjectStateHandler<?>> handlers,
        SequencedCollection<String> updateSequence,
        Consumer<ErrorState> errorReporter,
        ActionListener<ActionResponse.Empty> listener
    ) {
        super(namespace, stateChunk, versionCheck, handlers, updateSequence, errorReporter, listener);
        this.projectId = projectId;
    }

    @Override
    protected Optional<ProjectId> projectId() {
        return Optional.of(projectId);
    }

    @Override
    protected TransformState transform(ReservedProjectStateHandler<?> handler, Object state, TransformState transformState)
        throws Exception {
        return ReservedClusterStateService.transform(handler, projectId, state, transformState);
    }

    @Override
    protected ClusterState remove(ReservedProjectStateHandler<?> handler, TransformState prevState) throws Exception {
        return ReservedClusterStateService.remove(handler, projectId, prevState);
    }

    @Override
    // public visibility for testing
    public ClusterState execute(ClusterState currentState) {
        if (currentState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // If cluster state has become blocked, this task was submitted while the node was master but is now not master.
            // The new master will re-read file settings, so whatever update was to be written here will be handled
            // by the new master.
            return currentState;
        }

        // use an empty project if it doesn't exist, this is then added to ClusterState below.
        final boolean isNewProject = currentState.metadata().hasProject(projectId) == false;
        final ProjectMetadata currentProject = isNewProject
            ? ProjectMetadata.builder(projectId).build()
            : currentState.metadata().getProject(projectId);
        ClusterState.Builder builder = ClusterState.builder(currentState).putProjectMetadata(currentProject);
        if (isNewProject) {
            // A project_under_creation block is added initially to prevent any mutations on the project that might go through some
            // resurrection process after the current step. Once the creation/resurrection is final, we remove the block in a subsequent
            // cluster state.
            builder.blocks(
                ClusterBlocks.builder(currentState.blocks())
                    .addProjectGlobalBlock(projectId, ProjectMetadata.PROJECT_UNDER_CREATION_BLOCK)
                    .build()
            );
        }
        var result = execute(builder.build(), ProjectStateRegistry.get(currentState).reservedStateMetadata(projectId));
        if (result == null) {
            return currentState;
        }

        ClusterState updatedClusterState = result.v1();
        ProjectStateRegistry updatedProjectStateRegistry = updatedClusterState.custom(
            ProjectStateRegistry.TYPE,
            ProjectStateRegistry.EMPTY
        );
        return ClusterState.builder(updatedClusterState)
            .putCustom(
                ProjectStateRegistry.TYPE,
                ProjectStateRegistry.builder(updatedProjectStateRegistry).putReservedStateMetadata(projectId, result.v2()).build()
            )
            .build();
    }
}
