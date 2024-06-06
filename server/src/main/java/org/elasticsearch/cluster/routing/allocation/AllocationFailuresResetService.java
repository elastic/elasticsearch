/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

/**
 * A service that resets allocation failures counter and triggers reroute. For
 * example, reset counter when a new node joins a cluster. Resetting counter on
 * new node join covers a variety of use cases, such as rolling update, version
 * (and other settings), node restarts.
 */
public class AllocationFailuresResetService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(AllocationFailuresResetService.class);
    private final ClusterService clusterService;
    private final AllocationService allocationService;

    public AllocationFailuresResetService(ClusterService clusterService, AllocationService allocationService) {
        this.clusterService = clusterService;
        this.allocationService = allocationService;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        processEvent(event, null);
    }

    /**
     * Unit-test-friendly method, result listener completes with new ClusterState
     * after reroute with reset counter. Returns current cluster state if reset
     * condition does not happen.
     */
    protected void processEvent(ClusterChangedEvent event, @Nullable ActionListener<ClusterState> result) {
        if (event.nodesAdded() && event.state().getRoutingNodes().hasAllocationFailures()) {
            submitUnbatchedStateUpdateTask(new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    var nextState = allocationService.reroute(
                        currentState,
                        new AllocationCommands(),
                        false,
                        true,
                        false,
                        ActionListener.noop()
                    ).clusterState();
                    return nextState;
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn(() -> "cluster update failure: reset allocation failures counter", e);
                    if (result != null) {
                        result.onFailure(e);
                    }
                }

                @Override
                public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                    if (result != null) {
                        result.onResponse(newState);
                    }
                }
            });
        } else {
            if (result != null) {
                result.onResponse(event.state());
            }
        }
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task")
    private void submitUnbatchedStateUpdateTask(ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask("reset-allocation-failures", task);
    }
}
