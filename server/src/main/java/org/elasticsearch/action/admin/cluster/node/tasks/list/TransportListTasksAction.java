/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.tasks.list;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.RemovedTaskListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNullElse;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;

public class TransportListTasksAction extends TransportTasksAction<Task, ListTasksRequest, ListTasksResponse, TaskInfo> {
    public static long waitForCompletionTimeout(TimeValue timeout) {
        if (timeout == null) {
            timeout = DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT;
        }
        return System.nanoTime() + timeout.nanos();
    }

    private static final TimeValue DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT = timeValueSeconds(30);

    private final ThreadPool threadPool;

    @Inject
    public TransportListTasksAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool
    ) {
        super(
            ListTasksAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            ListTasksRequest::new,
            ListTasksResponse::new,
            TaskInfo::from,
            ThreadPool.Names.MANAGEMENT
        );
        this.threadPool = threadPool;
    }

    @Override
    protected ListTasksResponse newResponse(
        ListTasksRequest request,
        List<TaskInfo> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {
        return new ListTasksResponse(tasks, taskOperationFailures, failedNodeExceptions);
    }

    @Override
    protected void taskOperation(Task actionTask, ListTasksRequest request, Task task, ActionListener<TaskInfo> listener) {
        listener.onResponse(task.taskInfo(clusterService.localNode().getId(), request.getDetailed()));
    }

    @Override
    protected void processTasks(ListTasksRequest request, Consumer<Task> operation, Runnable nodeOperation, Consumer<Exception> onFailure) {
        if (request.getWaitForCompletion()) {
            Set<Task> matchedTasks = Sets.newConcurrentHashSet();
            CountDownLatch matchedTasksReady = new CountDownLatch(1);
            AtomicBoolean nodeOperationFiredOff = new AtomicBoolean();
            // Register the listener before we start iterating over tasks and sync it with a latch,
            // because matched tasks can get removed whilst we are iterating over them
            var listener = new RemovedTaskListener() {
                @Override
                public void onRemoved(Task task) {
                    try {
                        matchedTasksReady.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    logger.info(
                        "onRemoved task {} matchedTasks {}, nodeOperationFiredOff {}",
                        task.getId(),
                        matchedTasks.stream().map(Task::getId).toList(),
                        nodeOperationFiredOff
                    );
                    matchedTasks.remove(task);
                    if (matchedTasks.isEmpty() && nodeOperationFiredOff.compareAndSet(false, true)) {
                        try {
                            nodeOperation.run();
                        } finally {
                            taskManager.removeRemovedTaskListener(this);
                        }
                    }
                }
            };
            taskManager.addRemovedTaskListener(listener);
            processTasks(request, operation.andThen(task -> {
                if (task.getAction().startsWith(ListTasksAction.NAME)) {
                    // It doesn't make sense to wait for List Tasks and it can cause an infinite loop of the task waiting
                    // for itself or one of its child tasks
                    return;
                }
                matchedTasks.add(task);
            }));
            if (matchedTasks.isEmpty()) {
                // No tasks to wait, we can run nodeOperation in the management pool
                try {
                    nodeOperationFiredOff.set(true);
                    nodeOperation.run();
                } finally {
                    taskManager.removeRemovedTaskListener(listener);
                    matchedTasksReady.countDown();
                }
                return;
            }
            matchedTasksReady.countDown();
            threadPool.schedule(() -> {
                if (nodeOperationFiredOff.get() == false) {
                    onFailure.accept(new ElasticsearchTimeoutException("Timed out waiting for completion of tasks"));
                    taskManager.removeRemovedTaskListener(listener);
                }
            }, requireNonNullElse(request.getTimeout(), DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT), ThreadPool.Names.GENERIC);
        } else {
            super.processTasks(request, operation, nodeOperation, onFailure);
        }
    }
}
