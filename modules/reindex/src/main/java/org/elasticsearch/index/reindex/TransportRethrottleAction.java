/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.reindex;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportRethrottleAction extends TransportTasksAction<BulkByScrollTask, RethrottleRequest, ListTasksResponse, TaskInfo> {
    private final Client client;

    @Inject
    public TransportRethrottleAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
            TransportService transportService, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
            Client client) {
        super(settings, RethrottleAction.NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                RethrottleRequest::new, ListTasksResponse::new, ThreadPool.Names.MANAGEMENT);
        this.client = client;
    }

    @Override
    protected void taskOperation(RethrottleRequest request, BulkByScrollTask task, ActionListener<TaskInfo> listener) {
        logger.warn("[{}]: RETHROTTLE start taskOperation", task.getId());
        rethrottle(logger, clusterService.localNode().getId(), client, task, request.getRequestsPerSecond(), listener);
        logger.warn("[{}]: RETHROTTLE finish taskOperation", task.getId());
    }

    static void rethrottle(Logger logger, String localNodeId, Client client, BulkByScrollTask task, float newRequestsPerSecond,
            ActionListener<TaskInfo> listener) {

        if (task.isChild()) {
            rethrottleChildTask(logger, localNodeId, task, newRequestsPerSecond, listener);
            logger.warn("[{}]: FINISH CHILD INIT RETHROTTLE", task.getId());
            return;
        }

        if (task.isParent()) {
            rethrottleParentTask(logger, localNodeId, client, task, newRequestsPerSecond, listener);
            logger.warn("[{}]: FINISH PARENT INIT RETHROTTLE", task.getId());
            return;
        }

        logger.warn("[{}]: THROW EXCEPTION RETHROTTLE", task.getId());
        throw new IllegalArgumentException("task [" + task.getId() + "] must be set as a child or parent");
    }

    private static void rethrottleParentTask(Logger logger, String localNodeId, Client client, BulkByScrollTask task, float newRequestsPerSecond, ActionListener<TaskInfo> listener) {
        final ParentBulkByScrollWorker parentWorker = task.getParentWorker();
        final int runningSubtasks = parentWorker.runningSliceSubTasks();

        if (runningSubtasks > 0) {
            RethrottleRequest subRequest = new RethrottleRequest();
            subRequest.setRequestsPerSecond(newRequestsPerSecond / runningSubtasks);
            subRequest.setParentTask(new TaskId(localNodeId, task.getId()));
            logger.debug("rethrottling children of task [{}] to [{}] requests per second", task.getId(),
                subRequest.getRequestsPerSecond());
            client.execute(RethrottleAction.INSTANCE, subRequest, ActionListener.wrap(
                r -> {
                    r.rethrowFailures("Rethrottle");
                    listener.onResponse(parentWorker.getStatusGivenSlicesTaskInfo(localNodeId, r.getTasks()));
                },
                listener::onFailure));
        } else {
            logger.debug("children of task [{}] are already finished, nothing to rethrottle", task.getId());
        }
    }

    private static void rethrottleChildTask(Logger logger, String localNodeId, BulkByScrollTask task, float newRequestsPerSecond, ActionListener<TaskInfo> listener) {
        logger.debug("rethrottling local task [{}] to [{}] requests per second", task.getId(), newRequestsPerSecond);
        task.getChildWorker().rethrottle(newRequestsPerSecond);
        listener.onResponse(task.taskInfo(localNodeId, true));
    }

    @Override
    protected TaskInfo readTaskResponse(StreamInput in) throws IOException {
        return new TaskInfo(in);
    }

    @Override
    protected ListTasksResponse newResponse(RethrottleRequest request, List<TaskInfo> tasks,
            List<TaskOperationFailure> taskOperationFailures, List<FailedNodeException> failedNodeExceptions) {
        return new ListTasksResponse(tasks, taskOperationFailures, failedNodeExceptions);
    }

}
