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
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.TransformState;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

public class ReservedClusterStateUpdateTask extends ReservedStateUpdateTask<ReservedClusterStateHandler<?>> {
    public ReservedClusterStateUpdateTask(
        String namespace,
        ReservedStateChunk stateChunk,
        ReservedStateVersionCheck versionCheck,
        Map<String, ReservedClusterStateHandler<?>> handlers,
        Collection<String> orderedHandlers,
        Consumer<ErrorState> errorReporter,
        ActionListener<ActionResponse.Empty> listener
    ) {
        super(namespace, stateChunk, versionCheck, handlers, orderedHandlers, errorReporter, listener);
    }

    @Override
    protected Optional<ProjectId> projectId() {
        return Optional.empty();
    }

    @Override
    protected TransformState transform(ReservedClusterStateHandler<?> handler, Object state, TransformState transformState)
        throws Exception {
        return ReservedClusterStateService.transform(handler, state, transformState);
    }

    @Override
    ClusterState execute(ClusterState currentState) {
        if (currentState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // If cluster state has become blocked, this task was submitted while the node was master but is now not master.
            // The new master will re-read file settings, so whatever update was to be written here will be handled
            // by the new master.
            return currentState;
        }

        var result = execute(currentState, currentState.getMetadata().reservedStateMetadata());
        if (result == null) {
            return currentState;
        }

        return ClusterState.builder(result.v1()).metadata(Metadata.builder(result.v1().metadata()).put(result.v2())).build();
    }
}
