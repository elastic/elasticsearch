/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.LeaderBulkByScrollTaskState;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

public class TransportRethrottleAction extends TransportTasksAction<BulkByScrollTask, RethrottleRequest, ListTasksResponse, TaskInfo> {
    private final Client client;

    @Inject
    public TransportRethrottleAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        Client client
    ) {
        super(
            RethrottleAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            RethrottleRequest::new,
            ListTasksResponse::new,
            TaskInfo::from,
            ThreadPool.Names.MANAGEMENT
        );
        this.client = client;
    }

    @Override
    protected void taskOperation(
        CancellableTask actionTask,
        RethrottleRequest request,
        BulkByScrollTask task,
        ActionListener<TaskInfo> listener
    ) {
        rethrottle(logger, clusterService.localNode().getId(), client, task, request.getRequestsPerSecond(), listener);
    }

    static void rethrottle(
        Logger logger,
        String localNodeId,
        Client client,
        BulkByScrollTask task,
        float newRequestsPerSecond,
        ActionListener<TaskInfo> listener
    ) {

        if (task.isWorker()) {
            rethrottleChildTask(logger, localNodeId, task, newRequestsPerSecond, listener);
            return;
        }

        if (task.isLeader()) {
            rethrottleParentTask(logger, localNodeId, client, task, newRequestsPerSecond, listener);
            return;
        }

        throw new IllegalArgumentException(
            "task [" + task.getId() + "] has not yet been initialized to the point where it knows how to " + "rethrottle itself"
        );
    }

    private static void rethrottleParentTask(
        Logger logger,
        String localNodeId,
        Client client,
        BulkByScrollTask task,
        float newRequestsPerSecond,
        ActionListener<TaskInfo> listener
    ) {
        final LeaderBulkByScrollTaskState leaderState = task.getLeaderState();
        final int runningSubtasks = leaderState.runningSliceSubTasks();

        if (runningSubtasks > 0) {
            RethrottleRequest subRequest = new RethrottleRequest();
            subRequest.setRequestsPerSecond(newRequestsPerSecond / runningSubtasks);
            subRequest.setTargetParentTaskId(new TaskId(localNodeId, task.getId()));
            logger.debug("rethrottling children of task [{}] to [{}] requests per second", task.getId(), subRequest.getRequestsPerSecond());
            client.execute(RethrottleAction.INSTANCE, subRequest, ActionListener.wrap(r -> {
                r.rethrowFailures("Rethrottle");
                listener.onResponse(task.taskInfoGivenSubtaskInfo(localNodeId, r.getTasks()));
            }, listener::onFailure));
        } else {
            logger.debug("children of task [{}] are already finished, nothing to rethrottle", task.getId());
            listener.onResponse(task.taskInfo(localNodeId, true));
        }
    }

    private static void rethrottleChildTask(
        Logger logger,
        String localNodeId,
        BulkByScrollTask task,
        float newRequestsPerSecond,
        ActionListener<TaskInfo> listener
    ) {
        logger.debug("rethrottling local task [{}] to [{}] requests per second", task.getId(), newRequestsPerSecond);
        task.getWorkerState().rethrottle(newRequestsPerSecond);
        listener.onResponse(task.taskInfo(localNodeId, true));
    }

    @Override
    protected ListTasksResponse newResponse(
        RethrottleRequest request,
        List<TaskInfo> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {
        return new ListTasksResponse(tasks, taskOperationFailures, failedNodeExceptions);
    }

}
