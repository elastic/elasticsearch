/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.tasks.list;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.RemovedTaskListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Set;
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

    @Inject
    public TransportListTasksAction(ClusterService clusterService, TransportService transportService, ActionFilters actionFilters) {
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
    protected void processTasks(ListTasksRequest request, Consumer<Task> operation, ActionListener<Void> nodeOperation) {
        if (request.getWaitForCompletion()) {
            final ListenableActionFuture<Void> future = new ListenableActionFuture<>();
            final Set<Task> removedTasks = Sets.newConcurrentHashSet();
            final Set<Task> matchedTasks = Sets.newConcurrentHashSet();
            final RefCounted removalRefs = AbstractRefCounted.of(() -> {
                matchedTasks.removeAll(removedTasks);
                removedTasks.clear();
                if (matchedTasks.isEmpty()) {
                    future.onResponse(null);
                }
            });

            final AtomicBoolean collectionComplete = new AtomicBoolean();
            final RemovedTaskListener removedTaskListener = task -> {
                if (collectionComplete.get() == false && removalRefs.tryIncRef()) {
                    removedTasks.add(task);
                    removalRefs.decRef();
                } else {
                    matchedTasks.remove(task);
                    if (matchedTasks.isEmpty()) {
                        future.onResponse(null);
                    }
                }
            };
            taskManager.registerRemovedTaskListener(removedTaskListener);
            final ActionListener<Void> allMatchedTasksRemovedListener = ActionListener.runBefore(
                nodeOperation,
                () -> taskManager.unregisterRemovedTaskListener(removedTaskListener)
            );
            try {
                for (final var task : processTasks(request)) {
                    if (task.getAction().startsWith(ListTasksAction.NAME) == false) {
                        // It doesn't make sense to wait for List Tasks and it can cause an infinite loop of the task waiting
                        // for itself or one of its child tasks
                        matchedTasks.add(task);
                    }
                    operation.accept(task);
                }
            } catch (Exception e) {
                allMatchedTasksRemovedListener.onFailure(e);
                return;
            }
            removalRefs.decRef();
            collectionComplete.set(true);

            final var threadPool = clusterService.threadPool();
            future.addListener(
                new ContextPreservingActionListener<>(
                    threadPool.getThreadContext().newRestorableContext(false),
                    allMatchedTasksRemovedListener
                ),
                threadPool.executor(ThreadPool.Names.MANAGEMENT),
                null
            );
            future.addTimeout(
                requireNonNullElse(request.getTimeout(), DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT),
                threadPool,
                ThreadPool.Names.SAME
            );
        } else {
            super.processTasks(request, operation, nodeOperation);
        }
    }
}
