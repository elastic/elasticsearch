/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v 3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksProjectAction;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.ResumeInfo;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Transport action for listing all running reindex tasks.
 * This action filters for reindex tasks and only returns parent tasks (not subtasks).
 * Project scoping is handled by TransportTasksProjectAction, which only returns
 * tasks for the current project.
 */
public class TransportListReindexAction extends TransportTasksProjectAction<Task, ListReindexRequest, ListReindexResponse, TaskInfo> {

    public static final ActionType<ListReindexResponse> TYPE = new ActionType<>("cluster:monitor/reindex/list");

    @Inject
    public TransportListReindexAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ProjectResolver projectResolver
    ) {
        super(
            TYPE.name(),
            clusterService,
            transportService,
            actionFilters,
            ListReindexRequest::new,
            TaskInfo::from,
            transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT),
            projectResolver
        );
    }

    @Override
    protected ListReindexResponse newResponse(
        ListReindexRequest request,
        List<TaskInfo> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {
        return new ListReindexResponse(deduplicateTasks(tasks), taskOperationFailures, failedNodeExceptions);
    }

    /**
     * Deduplicate tasks that share the same {@link TaskId}. During a relocation window both the
     * original task and the relocated task may be visible, each rewritten to carry the original ID.
     * We can't distinguish which is the relocated one, but the status shouldn't be meaningfully different, so pick the first one.
     */
    static List<TaskInfo> deduplicateTasks(final List<TaskInfo> tasks) {
        if (tasks.size() <= 1) {
            return tasks;
        }
        final Map<TaskId, TaskInfo> seen = new LinkedHashMap<>(tasks.size());
        for (final TaskInfo task : tasks) {
            seen.putIfAbsent(task.taskId(), task);
        }
        return seen.size() == tasks.size() ? tasks : List.copyOf(seen.values());
    }

    @Override
    protected void taskOperation(CancellableTask actionTask, ListReindexRequest request, Task task, ActionListener<TaskInfo> listener) {
        assert task instanceof BulkByScrollTask : "task should be a BulkByScrollTask";
        TaskInfo info = task.taskInfo(clusterService.localNode().getId(), request.getDetailed());
        if (task instanceof final BulkByScrollTask bbs) {
            final ResumeInfo.RelocationOrigin origin = bbs.relocationOrigin();
            final TaskId originalId = origin.originalTaskId();
            final long originalStartMillis = origin.originalStartTimeMillis();
            final long adjustedRunningTimeNanos = info.runningTimeNanos() + TimeUnit.MILLISECONDS.toNanos(
                info.startTime() - originalStartMillis
            );
            // is a NOP (doesn't change anything) unless relocated
            info = new TaskInfo(
                originalId,
                info.type(),
                originalId.getNodeId(),
                info.action(),
                info.description(),
                info.status(),
                originalStartMillis,
                adjustedRunningTimeNanos,
                info.cancellable(),
                info.cancelled(),
                info.parentTaskId(),
                info.headers()
            );
        }
        listener.onResponse(info);
    }
}
