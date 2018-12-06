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

package org.elasticsearch.action.admin.cluster.node.tasks.list;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.function.Consumer;

import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

public class TransportListTasksAction extends TransportTasksAction<Task, ListTasksRequest, ListTasksResponse, TaskInfo> {
    public static long waitForCompletionTimeout(TimeValue timeout) {
        if (timeout == null) {
            timeout = DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT;
        }
        return System.nanoTime() + timeout.nanos();
    }

    private static final TimeValue DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT = timeValueSeconds(30);

    @Inject
    public TransportListTasksAction(ClusterService clusterService, TransportService transportService, ActionFilters actionFilters) {
        super(ListTasksAction.NAME, clusterService, transportService, actionFilters,
            ListTasksRequest::new, ListTasksResponse::new, TaskInfo::new, ThreadPool.Names.MANAGEMENT);
    }

    @Override
    protected ListTasksResponse newResponse(ListTasksRequest request, List<TaskInfo> tasks,
            List<TaskOperationFailure> taskOperationFailures, List<FailedNodeException> failedNodeExceptions) {
        return new ListTasksResponse(tasks, taskOperationFailures, failedNodeExceptions);
    }

    @Override
    protected void taskOperation(ListTasksRequest request, Task task, ActionListener<TaskInfo> listener) {
        listener.onResponse(task.taskInfo(clusterService.localNode().getId(), request.getDetailed()));
    }

    @Override
    protected void processTasks(ListTasksRequest request, Consumer<Task> operation) {
        if (request.getWaitForCompletion()) {
            long timeoutNanos = waitForCompletionTimeout(request.getTimeout());
            operation = operation.andThen(task -> {
                if (task.getAction().startsWith(ListTasksAction.NAME)) {
                    // It doesn't make sense to wait for List Tasks and it can cause an infinite loop of the task waiting
                    // for itself or one of its child tasks
                    return;
                }
                taskManager.waitForTaskCompletion(task, timeoutNanos);
            });
        }
        super.processTasks(request, operation);
    }

}
