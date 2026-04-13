/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.tasks.list;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.RemovedTaskListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNullElse;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;

public class TransportListTasksAction extends TransportTasksAction<Task, ListTasksRequest, ListTasksResponse, TaskInfo> {

    public static final ActionType<ListTasksResponse> TYPE = new ActionType<>("cluster:monitor/tasks/lists");

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
            TYPE.name(),
            clusterService,
            transportService,
            actionFilters,
            ListTasksRequest::new,
            TaskInfo::from,
            transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT)
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
    protected void taskOperation(CancellableTask actionTask, ListTasksRequest request, Task task, ActionListener<TaskInfo> listener) {
        listener.onResponse(task.taskInfo(clusterService.localNode().getId(), request.getDetailed()));
    }

    @Override
    protected void doExecute(Task task, ListTasksRequest request, ActionListener<ListTasksResponse> listener) {
        assert task instanceof CancellableTask;
        // Double-list is only needed for relocatable tasks (problem description further down in function comment),
        // and `wait_for_completion=false` (since with a single list we can say we waited for the source task to complete).
        if (requestCannotMatchRelocatableTasks(request) || request.getWaitForCompletion()) {
            super.doExecute(task, request, listener);
            return;
        }
        // relocatable tasks might've relocated during listing, and depending on list fan-out response timing, might be missed, example:
        // 1. the source node from which a task relocates from captures the listing late (in realtime) after task completion, no task
        // 2. the destination node to which the task relocates to captures the listing early (in realtime) before task creation, no task
        // therefore we double-list and de-dupe, which will ensure we see a task *at least* once.
        // we also prevent quick back-to-back relocations during shutdown, to make hitting the race twice vanishly unlikely.
        super.doExecute(task, request, listener.delegateFailureAndWrap((l1, firstResponse) -> {
            super.doExecute(task, request, l1.delegateFailureAndWrap((l2, secondResponse) -> {
                List<TaskInfo> combined = new ArrayList<>(firstResponse.getTasks().size() + secondResponse.getTasks().size());
                combined.addAll(secondResponse.getTasks());
                combined.addAll(firstResponse.getTasks());

                List<TaskOperationFailure> allTaskFailures = new ArrayList<>(
                    firstResponse.getTaskFailures().size() + secondResponse.getTaskFailures().size()
                );
                allTaskFailures.addAll(firstResponse.getTaskFailures());
                allTaskFailures.addAll(secondResponse.getTaskFailures());

                List<ElasticsearchException> allNodeFailures = new ArrayList<>(
                    firstResponse.getNodeFailures().size() + secondResponse.getNodeFailures().size()
                );
                allNodeFailures.addAll(firstResponse.getNodeFailures());
                allNodeFailures.addAll(secondResponse.getNodeFailures());

                l2.onResponse(
                    new ListTasksResponse(
                        deduplicateTasks(combined),
                        deduplicateTaskFailures(allTaskFailures),
                        deduplicateNodeFailures(allNodeFailures)
                    )
                );
            }));
        }));
    }

    // visible for testing
    static boolean requestCannotMatchRelocatableTasks(ListTasksRequest request) {
        String[] actions = request.getActions();
        if (CollectionUtils.isEmpty(actions)) {
            return false;
        }
        // currently only reindex action supports relocation
        return Regex.simpleMatch(actions, ReindexAction.NAME) == false;
    }

    // visible for testing
    static List<TaskInfo> deduplicateTasks(List<TaskInfo> tasks) {
        if (tasks.size() <= 1) {
            return tasks;
        }
        Map<TaskId, TaskInfo> seen = new LinkedHashMap<>(tasks.size());
        for (TaskInfo task : tasks) {
            // todo(szy): update to de-dupe by `original_task_id`
            seen.putIfAbsent(task.taskId(), task);
        }
        return seen.size() == tasks.size() ? tasks : List.copyOf(seen.values());
    }

    // visible for testing
    static List<TaskOperationFailure> deduplicateTaskFailures(List<TaskOperationFailure> failures) {
        if (failures.size() <= 1) {
            return failures;
        }
        Map<String, TaskOperationFailure> seen = new LinkedHashMap<>(failures.size());
        for (TaskOperationFailure f : failures) {
            seen.put(f.getNodeId() + ":" + f.getTaskId(), f);
        }
        return seen.size() == failures.size() ? failures : List.copyOf(seen.values());
    }

    // visible for testing
    static List<ElasticsearchException> deduplicateNodeFailures(List<ElasticsearchException> failures) {
        if (failures.size() <= 1) {
            return failures;
        }
        Map<String, ElasticsearchException> seen = new LinkedHashMap<>(failures.size());
        for (ElasticsearchException f : failures) {
            String nodeId = f instanceof FailedNodeException fne ? fne.nodeId() : f.getMessage();
            seen.put(nodeId, f);
        }
        return seen.size() == failures.size() ? failures : List.copyOf(seen.values());
    }

    @Override
    protected void processTasks(CancellableTask nodeTask, ListTasksRequest request, ActionListener<List<Task>> nodeOperation) {
        if (request.getWaitForCompletion()) {
            final ListenableActionFuture<List<Task>> future = new ListenableActionFuture<>();
            final List<Task> processedTasks = new ArrayList<>();
            final Set<Task> removedTasks = ConcurrentCollections.newConcurrentSet();
            final Set<Task> matchedTasks = ConcurrentCollections.newConcurrentSet();
            final RefCounted removalRefs = AbstractRefCounted.of(() -> {
                matchedTasks.removeAll(removedTasks);
                removedTasks.clear();
                if (matchedTasks.isEmpty()) {
                    future.onResponse(processedTasks);
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
                        future.onResponse(processedTasks);
                    }
                }
            };
            taskManager.registerRemovedTaskListener(removedTaskListener);
            final ActionListener<List<Task>> allMatchedTasksRemovedListener = ActionListener.runBefore(
                nodeOperation,
                () -> taskManager.unregisterRemovedTaskListener(removedTaskListener)
            );
            try {
                for (final var task : processTasks(request)) {
                    if (task.getAction().startsWith(TYPE.name()) == false) {
                        // It doesn't make sense to wait for List Tasks and it can cause an infinite loop of the task waiting
                        // for itself or one of its child tasks
                        matchedTasks.add(task);
                    }
                    processedTasks.add(task);
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
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            );
            nodeTask.addListener(() -> future.onFailure(new TaskCancelledException("task cancelled")));
        } else {
            super.processTasks(nodeTask, request, nodeOperation);
        }
    }
}
