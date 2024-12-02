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

import java.util.ArrayList;

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

        // In a given batch of update tasks, only one will actually take effect,
        // and we want to execute only that task, because if we execute all the tasks
        // one after another, that will require all the tasks to be capable of executing
        // correctly even without prior state updates being applied.
        //
        // The correct task to run would be whichever one would take effect if we were to
        // run the tasks one-per-batch. In effect, this is the task with the highest version number;
        // if multiple tasks have the same version number, their ReservedStateVersionCheck fields
        // will be used to break the tie.
        //
        // One wrinkle is: if the tasks fails, then we will know retroactively that it was
        // not the task that actually took effect, and we must eliminate that one and try again.

        var candidates = new ArrayList<>(taskContexts);
        while (candidates.isEmpty() == false) {
            TaskContext<ReservedStateUpdateTask> taskContext = removeEffectiveTaskContext(candidates);
            logger.info("Effective task: {}", taskContext.getTask());
            ClusterState clusterState = initState;
            try (var ignored = taskContext.captureResponseHeaders()) {
                var task = taskContext.getTask();
                clusterState = task.execute(clusterState);
                taskContext.success(() -> task.listener().onResponse(ActionResponse.Empty.INSTANCE));
                logger.debug("-> Update task succeeded");
                // All the others "succeeded" and then were conceptually superseded by the effective task
                candidates.forEach(c -> c.success(() -> c.getTask().listener().onResponse(ActionResponse.Empty.INSTANCE)));
                return clusterState;
            } catch (Exception e) {
                taskContext.onFailure(e);
                if (candidates.isEmpty() == false) {
                    logger.warn("-> Update task failed; will try the previous update task");
                }
            }
        }

        logger.warn("All {} update tasks failed; returning initial state", taskContexts.size());
        return initState;
    }

    /**
     * Removes and returns the {@link TaskContext} corresponding to the task that would take effect
     * if the tasks were executed one after the other.
     */
    private TaskContext<ReservedStateUpdateTask> removeEffectiveTaskContext(ArrayList<? extends TaskContext<ReservedStateUpdateTask>> candidates) {
        assert candidates.isEmpty() == false;
        int winner = 0;
        for (int candidate = 1; candidate < candidates.size(); candidate++) {
            if (candidates.get(candidate).getTask().supersedes(candidates.get(winner).getTask())) {
                winner = candidate;
            }
        }
        return candidates.remove(winner);
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
