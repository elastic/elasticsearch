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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.Consumer;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

/**
 *
 */
public class TransportListTasksAction extends TransportTasksAction<Task, ListTasksRequest, ListTasksResponse, TaskInfo> {
    private static final TimeValue WAIT_FOR_COMPLETION_POLL = timeValueMillis(100);
    private static final TimeValue DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT = timeValueSeconds(30);

    @Inject
    public TransportListTasksAction(Settings settings, ClusterName clusterName, ThreadPool threadPool, ClusterService clusterService, TransportService transportService, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ListTasksAction.NAME, clusterName, threadPool, clusterService, transportService, actionFilters,
            indexNameExpressionResolver, new Callable<ListTasksRequest>() {
                @Override
                public ListTasksRequest call() throws Exception {
                    return new ListTasksRequest();
                }
            }, ThreadPool.Names.MANAGEMENT);
    }

    @Override
    protected ListTasksResponse newResponse(ListTasksRequest request, List<TaskInfo> tasks, List<TaskOperationFailure> taskOperationFailures, List<FailedNodeException> failedNodeExceptions) {
        return new ListTasksResponse(tasks, taskOperationFailures, failedNodeExceptions);
    }

    @Override
    protected TaskInfo readTaskResponse(StreamInput in) throws IOException {
        return new TaskInfo(in);
    }

    @Override
    protected TaskInfo taskOperation(ListTasksRequest request, Task task) {
        return task.taskInfo(clusterService.localNode(), request.getDetailed());
    }

    @Override
    protected void processTasks(ListTasksRequest request, final Consumer<Task> operation) {
        if (false == request.getWaitForCompletion()) {
            super.processTasks(request, operation);
            return;
        }
        // If we should wait for completion then we have to intercept every found task and wait for it to leave the manager.
        TimeValue timeout = request.getTimeout();
        if (timeout == null) {
            timeout = DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT;
        }
        final long timeoutTime = System.nanoTime() + timeout.nanos();
        super.processTasks(request, new Consumer<Task>() {
            @Override
            public void accept(Task t) {
                operation.accept(t);
                while (System.nanoTime() - timeoutTime < 0) {
                    Task task = taskManager.getTask(t.getId());
                    if (task == null) {
                        return;
                    }
                    if (task.getAction().startsWith(ListTasksAction.NAME)) {
                        // It doesn't make sense to wait for List Tasks and it can cause an infinite loop of the task waiting
                        // for itself of one of its child tasks
                        return;
                    }
                    try {
                        Thread.sleep(WAIT_FOR_COMPLETION_POLL.millis());
                    } catch (InterruptedException e) {
                        throw new ElasticsearchException("Interrupted waiting for completion of [{}]", e, t);
                    }
                }
                throw new ElasticsearchTimeoutException("Timed out waiting for completion of [{}]", t);
            }
        });
    }

    @Override
    protected boolean accumulateExceptions() {
        return true;
    }
}
