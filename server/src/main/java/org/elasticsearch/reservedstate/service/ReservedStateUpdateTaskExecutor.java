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
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.Tuple;

/**
 * Reserved cluster state update task executor
 */
public class ReservedStateUpdateTaskExecutor extends SimpleBatchedExecutor<ReservedStateUpdateTask, Void> {

    private static final Logger logger = LogManager.getLogger(ReservedStateUpdateTaskExecutor.class);

    // required to execute a reroute after cluster state is published
    private final RerouteService rerouteService;

    public ReservedStateUpdateTaskExecutor(RerouteService rerouteService) {
        this.rerouteService = rerouteService;
    }

    @Override
    public Tuple<ClusterState, Void> executeTask(ReservedStateUpdateTask task, ClusterState clusterState) {
        return Tuple.tuple(task.execute(clusterState), null);
    }

    @Override
    public void taskSucceeded(ReservedStateUpdateTask task, Void unused) {
        task.listener().onResponse(ActionResponse.Empty.INSTANCE);
    }

    @Override
    public void clusterStatePublished() {
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
