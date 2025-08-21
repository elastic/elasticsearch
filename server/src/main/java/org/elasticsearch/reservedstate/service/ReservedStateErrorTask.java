/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reservedstate.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateErrorMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;

import static org.elasticsearch.cluster.metadata.ReservedStateMetadata.EMPTY_VERSION;
import static org.elasticsearch.cluster.metadata.ReservedStateMetadata.NO_VERSION;
import static org.elasticsearch.cluster.metadata.ReservedStateMetadata.RESTORED_VERSION;
import static org.elasticsearch.core.Strings.format;

/**
 * Cluster state update task that sets the error state of the reserved cluster state metadata.
 * <p>
 * This is used when a reserved cluster state update encounters error(s) while processing
 * the {@link ReservedStateChunk}.
 */
public class ReservedStateErrorTask implements ClusterStateTaskListener {
    private static final Logger logger = LogManager.getLogger(ReservedStateErrorTask.class);

    private final ErrorState errorState;
    private final ActionListener<ActionResponse.Empty> listener;

    public ReservedStateErrorTask(ErrorState errorState, ActionListener<ActionResponse.Empty> listener) {
        this.errorState = errorState;
        this.listener = listener;
    }

    @Override
    public void onFailure(Exception e) {
        listener.onFailure(e);
    }

    ActionListener<ActionResponse.Empty> listener() {
        return listener;
    }

    // package private for testing
    static boolean isNewError(ReservedStateMetadata existingMetadata, Long newStateVersion, ReservedStateVersionCheck versionCheck) {
        return (existingMetadata == null
            || existingMetadata.errorMetadata() == null
            || versionCheck.test(existingMetadata.errorMetadata().version(), newStateVersion)
            || newStateVersion.equals(RESTORED_VERSION)
            || newStateVersion.equals(EMPTY_VERSION)
            || newStateVersion.equals(NO_VERSION));
    }

    static ReservedStateMetadata getMetadata(ClusterState state, ErrorState errorState) {
        return errorState.projectId()
            .map(p -> ReservedClusterStateService.getPotentiallyNewProject(state, p).reservedStateMetadata())
            .orElseGet(() -> state.metadata().reservedStateMetadata())
            .get(errorState.namespace());
    }

    static boolean checkErrorVersion(ClusterState currentState, ErrorState errorState) {
        ReservedStateMetadata existingMetadata = getMetadata(currentState, errorState);
        // check for noop here
        if (isNewError(existingMetadata, errorState.version(), errorState.versionCheck()) == false) {
            logger.info(
                () -> format(
                    "Not updating error state because version [%s] is less or equal to the last state error version [%s]",
                    errorState.version(),
                    existingMetadata.errorMetadata().version()
                )
            );
            return false;
        }
        return true;
    }

    boolean shouldUpdate(ClusterState currentState) {
        return checkErrorVersion(currentState, errorState);
    }

    ClusterState execute(ClusterState currentState) {
        ClusterState.Builder stateBuilder = new ClusterState.Builder(currentState);
        var errorMetadata = new ReservedStateErrorMetadata(errorState.version(), errorState.errorKind(), errorState.errors());

        if (errorState.projectId().isPresent()) {
            ProjectMetadata project = currentState.metadata().getProject(errorState.projectId().get());

            ReservedStateMetadata reservedMetadata = project.reservedStateMetadata().get(errorState.namespace());
            ReservedStateMetadata.Builder resBuilder = ReservedStateMetadata.builder(errorState.namespace(), reservedMetadata);
            resBuilder.errorMetadata(errorMetadata);

            stateBuilder.putProjectMetadata(ProjectMetadata.builder(project).put(resBuilder.build()));
        } else {
            Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());

            ReservedStateMetadata reservedMetadata = currentState.metadata().reservedStateMetadata().get(errorState.namespace());
            ReservedStateMetadata.Builder resBuilder = ReservedStateMetadata.builder(errorState.namespace(), reservedMetadata);
            resBuilder.errorMetadata(errorMetadata);

            metadataBuilder.put(resBuilder.build());
            stateBuilder.metadata(metadataBuilder);
        }

        return stateBuilder.build();
    }
}
