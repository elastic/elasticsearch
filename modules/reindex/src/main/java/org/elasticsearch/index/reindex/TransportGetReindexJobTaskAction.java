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

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.function.Consumer;

public class TransportGetReindexJobTaskAction extends TransportTasksAction<BulkByScrollTask, GetReindexJobTaskAction.Request,
    GetReindexJobTaskAction.Responses, GetReindexJobTaskAction.Response> {

    @Inject
    public TransportGetReindexJobTaskAction(ClusterService clusterService, TransportService transportService, ActionFilters actionFilters) {
        super(GetReindexJobTaskAction.NAME, clusterService, transportService, actionFilters, GetReindexJobTaskAction.Request::new,
            GetReindexJobTaskAction.Responses::new, GetReindexJobTaskAction.Response::new, ThreadPool.Names.GENERIC);
    }

    @Override
    protected GetReindexJobTaskAction.Responses newResponse(
        final GetReindexJobTaskAction.Request request,
        final List<GetReindexJobTaskAction.Response> tasks,
        final List<TaskOperationFailure> taskOperationFailures,
        final List<FailedNodeException> failedNodeExceptions) {
        return new GetReindexJobTaskAction.Responses(tasks, taskOperationFailures, failedNodeExceptions);
    }

    @Override
    protected void taskOperation(GetReindexJobTaskAction.Request request, BulkByScrollTask task,
                                 ActionListener<GetReindexJobTaskAction.Response> listener) {
        listener.onResponse(new GetReindexJobTaskAction.Response(new TaskId(clusterService.localNode().getId(), task.getId())));
    }

    @Override
    protected void processTasks(GetReindexJobTaskAction.Request request, Consumer<BulkByScrollTask> operation) {
        String persistentTaskId = request.getPersistentTaskId();

        TaskId parentTaskId = null;
        for (Task task : taskManager.getTasks().values()) {
            if (task instanceof ReindexTask) {
                AllocatedPersistentTask reindexTask = (AllocatedPersistentTask) task;
                if (reindexTask.getPersistentTaskId().equals(persistentTaskId)) {
                    parentTaskId = new TaskId(clusterService.localNode().getId(), reindexTask.getId());
                }
            }
        }

        if (parentTaskId == null) {
            throw new ResourceNotFoundException("task [{}] is missing", request.getPersistentTaskId());
        }

        for (Task task : taskManager.getTasks().values()) {
            if (task instanceof BulkByScrollTask) {
                if (task.getParentTaskId().equals(parentTaskId)) {
                    operation.accept((BulkByScrollTask) task);
                }
            }
        }
    }
}
