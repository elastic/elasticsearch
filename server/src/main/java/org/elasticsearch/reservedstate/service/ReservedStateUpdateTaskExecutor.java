/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reservedstate.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.common.Priority;

/**
 * Reserved cluster state update task executor
 *
 * @param rerouteService instance of {@link RerouteService}, so that we can execute reroute after cluster state is published
 */
public record ReservedStateUpdateTaskExecutor(RerouteService rerouteService) implements ClusterStateTaskExecutor<ReservedStateUpdateTask> {

    private static final Logger logger = LogManager.getLogger(ReservedStateUpdateTaskExecutor.class);

    @Override
    public ClusterState execute(BatchExecutionContext<ReservedStateUpdateTask> batchExecutionContext) throws Exception {
        var updatedState = batchExecutionContext.initialState();
        for (final var taskContext : batchExecutionContext.taskContexts()) {
            try (var ignored = taskContext.captureResponseHeaders()) {
                updatedState = taskContext.getTask().execute(updatedState);
            }
            taskContext.success(() -> taskContext.getTask().listener().onResponse(ActionResponse.Empty.INSTANCE));
        }
        return updatedState;
    }

    @Override
    public void clusterStatePublished(ClusterState newClusterState) {
        rerouteService.reroute(
            "reroute after saving and reserving part of the cluster state",
            Priority.NORMAL,
            ActionListener.wrap(
                r -> logger.trace("reroute after applying and reserving part of the cluster state succeeded"),
                e -> logger.debug("reroute after applying and reserving part of the cluster state failed", e)
            )
        );
    }
}
