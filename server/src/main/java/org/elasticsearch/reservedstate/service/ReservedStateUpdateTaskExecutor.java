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

import static java.util.Comparator.comparing;
import static org.elasticsearch.reservedstate.service.ReservedStateUpdateTask.SUPERSEDING_FIRST;

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
        // One wrinkle is: if the task fails, then we will know retroactively that it was
        // not the task that actually took effect, and we must then identify which of the
        // remaining tasks would have taken effect. We achieve this by sorting the tasks
        // using the SUPERSEDING_FIRST comparator.

        var candidates = new ArrayList<>(taskContexts);
        candidates.sort(comparing(TaskContext::getTask, SUPERSEDING_FIRST));
        for (var iter = candidates.iterator(); iter.hasNext();) {
            TaskContext<ReservedStateUpdateTask> taskContext = iter.next();
            logger.info("Effective task: {}", taskContext.getTask());
            ClusterState clusterState = initState;
            try (var ignored = taskContext.captureResponseHeaders()) {
                var task = taskContext.getTask();
                clusterState = task.execute(clusterState);
                taskContext.success(() -> task.listener().onResponse(ActionResponse.Empty.INSTANCE));
                logger.debug("-> Update task succeeded");
                // All the others conceptually "succeeded" and then were superseded by the effective task
                iter.forEachRemaining(c -> c.success(() -> c.getTask().listener().onResponse(ActionResponse.Empty.INSTANCE)));
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
