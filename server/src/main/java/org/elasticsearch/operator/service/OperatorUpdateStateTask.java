/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.operator.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.common.Priority;

import java.util.List;

/**
 * Generic operator cluster state update task
 *
 * @param listener
 */
public record OperatorUpdateStateTask(ActionListener<ActionResponse.Empty> listener) implements ClusterStateTaskListener {
    private static final Logger logger = LogManager.getLogger(FileSettingsService.class);

    @Override
    public void onFailure(Exception e) {
        listener.onFailure(e);
    }

    /**
     * Operator update cluster state task executor
     *
     * @param namespace of the state we are updating
     * @param rerouteService instance of RerouteService so we can execute reroute after cluster state is published
     */
    public record OperatorUpdateStateTaskExecutor(String namespace, ClusterState newState, RerouteService rerouteService)
        implements
            ClusterStateTaskExecutor<OperatorUpdateStateTask> {

        @Override
        public ClusterState execute(ClusterState currentState, List<TaskContext<OperatorUpdateStateTask>> taskContexts) throws Exception {
            for (final var taskContext : taskContexts) {
                taskContext.success(
                    () -> taskContext.getTask().listener().delegateFailure((l, s) -> l.onResponse(ActionResponse.Empty.INSTANCE))
                );
            }
            return newState;
        }

        @Override
        public void clusterStatePublished(ClusterState newClusterState) {
            rerouteService.reroute(
                "reroute after applying operator cluster state for namespace [" + namespace + "]",
                Priority.NORMAL,
                ActionListener.wrap(
                    r -> logger.trace("reroute after applying operator cluster state for [{}] succeeded", namespace),
                    e -> logger.debug("reroute after applying operator cluster state failed", e)
                )
            );
        }
    }
}
