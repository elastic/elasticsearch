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
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.common.Priority;

/**
 * Reserved cluster state update task executor
 */
public class ReservedStateUpdateTaskExecutor implements ClusterStateTaskExecutor<ReservedStateUpdateTask> {

    private static final Logger logger = LogManager.getLogger(ReservedStateUpdateTaskExecutor.class);

    // required to execute a reroute after cluster state is published
    private final RerouteService rerouteService;

    public ReservedStateUpdateTaskExecutor(RerouteService rerouteService) {
        this.rerouteService = rerouteService;
    }

    @Override
    public final ClusterState execute(BatchExecutionContext<ReservedStateUpdateTask> batchExecutionContext) throws Exception {
        var initState = batchExecutionContext.initialState();
        var taskContexts = batchExecutionContext.taskContexts();
        if (taskContexts.isEmpty()) {
            return initState;
        }

        // Only the last update is relevant; the others can be skipped.
        // However, if that last update task fails, we should fall back to the preceding one.
        for (var iterator = taskContexts.listIterator(taskContexts.size()); iterator.hasPrevious();) {
            var taskContext = iterator.previous();
            ClusterState clusterState = initState;
            try (var ignored = taskContext.captureResponseHeaders()) {
                var task = taskContext.getTask();
                clusterState = task.execute(clusterState);
                taskContext.success(() -> task.listener().onResponse(ActionResponse.Empty.INSTANCE));
                logger.debug("Update task succeeded");
                return clusterState;
            } catch (Exception e) {
                taskContext.onFailure(e);
                if (iterator.hasPrevious()) {
                    logger.warn("Update task failed; will try the previous update task");
                }
            }
        }

        logger.warn("All {} update tasks failed; returning initial state", taskContexts.size());
        return initState;
    }

    @Override
    public final void clusterStatePublished(ClusterState newClusterState) {
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
