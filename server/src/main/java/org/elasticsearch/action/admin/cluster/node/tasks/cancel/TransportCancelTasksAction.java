/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.tasks.cancel;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction.RELOCATABLE_ACTIONS;

/// Transport action that cancels currently running cancellable tasks.
///
/// When the request can match a relocatable action (e.g. reindex) we fan out twice and merge the results, so a task being relocated
/// mid-broadcast can't fall through the cracks.
public class TransportCancelTasksAction extends TransportTasksAction<CancellableTask, CancelTasksRequest, ListTasksResponse, TaskInfo> {

    public static final String NAME = "cluster:admin/tasks/cancel";

    public static final ActionType<ListTasksResponse> TYPE = new ActionType<>(NAME);

    @Inject
    public TransportCancelTasksAction(ClusterService clusterService, TransportService transportService, ActionFilters actionFilters) {
        super(
            NAME,
            clusterService,
            transportService,
            actionFilters,
            CancelTasksRequest::new,
            TaskInfo::from,
            // Cancellation is usually lightweight, and runs on the transport thread if the task didn't even start yet, but some
            // implementations of CancellableTask#onCancelled() are nontrivial so we use GENERIC here. TODO could it be SAME?
            transportService.getThreadPool().executor(ThreadPool.Names.GENERIC)
        );
    }

    @Override
    protected ListTasksResponse newResponse(
        CancelTasksRequest request,
        List<TaskInfo> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {
        if (request.getTargetTaskId().isSet() && tasks.isEmpty() && taskOperationFailures.isEmpty() && failedNodeExceptions.isEmpty()) {
            // For BwC: Now that we fan out to every node (to find potentially relocated targets by their original task id), the node
            // identified by the target task id no longer throws ResourceNotFoundException from its per-node processTasks.
            // Re-surface the same ResourceNotFoundException wrapped in a FailedNodeException if no task is found by the target task Id.
            // The double-broadcast merge filters this out if either pass captured the task.
            final TaskId target = request.getTargetTaskId();
            final FailedNodeException notFound = new FailedNodeException(
                target.getNodeId(),
                "Failed node [" + target.getNodeId() + "]",
                new ResourceNotFoundException("task [{}] is not found", target)
            );
            return new ListTasksResponse(tasks, taskOperationFailures, List.of(notFound));
        }
        return new ListTasksResponse(tasks, taskOperationFailures, failedNodeExceptions);
    }

    /// Fan out to every node so we can find tasks potentially relocated to other nodes.
    @Override
    protected String[] resolveNodes(CancelTasksRequest request, DiscoveryNodes discoveryNodes) {
        return discoveryNodes.resolveNodes(request.getNodes());
    }

    /// Single broadcast for non-relocatable actions; double broadcast otherwise. The double broadcast closes a relocation race where a
    /// single fan-out can hit the destination before the relocated task is created and the source after it has finished — yielding a
    /// 404. The first pass runs with `waitForCompletion=false` so it doesn't block the second pass; the second pass uses the user's
    /// setting. [#mergeResponses] reconciles the two responses so the caller can't tell whether a relocation happened.
    @Override
    protected void doExecute(Task task, CancelTasksRequest request, ActionListener<ListTasksResponse> listener) {
        if (RELOCATABLE_ACTIONS.stream().noneMatch(request::canMatchAction)) {
            super.doExecute(task, request, listener);
            return;
        }
        final CancelTasksRequest firstPassRequest = copyWithoutWaitForCompletion(request);
        super.doExecute(
            task,
            firstPassRequest,
            listener.delegateFailureAndWrap(
                (l1, firstPass) -> super.doExecute(
                    task,
                    request,
                    l1.delegateFailureAndWrap(
                        (l2, secondPass) -> l2.onResponse(mergeResponses(firstPass, secondPass, request.getTargetTaskId()))
                    )
                )
            )
        );
    }

    static CancelTasksRequest copyWithoutWaitForCompletion(final CancelTasksRequest request) {
        final CancelTasksRequest copy = new CancelTasksRequest();
        copy.copyFieldsFrom(request);
        copy.setWaitForCompletion(false);
        return copy;
    }

    /// Merges two cancel-tasks responses into one that's relocation-agnostic to the caller:
    ///
    /// - Captured tasks are deduplicated by `originalTaskId`; second pass wins, [TransportListTasksAction#preferNewer] resolves intra-pass
    ///   ties.
    /// - Per-task failures are kept unless they refer to a logical task we already captured (e.g. the source side's `409 Conflict` is
    ///   irrelevant once the relocated successor was cancelled).
    /// - Node failures are deduplicated; any failure with a [ResourceNotFoundException] in its cause chain is dropped when the merge
    ///   captured any task for a targeted cancel.
    static ListTasksResponse mergeResponses(
        final ListTasksResponse firstPass,
        final ListTasksResponse secondPass,
        final TaskId targetTaskId
    ) {
        final Map<TaskId, TaskInfo> tasksByOriginalId = mergeTasksByOriginalTaskId(firstPass.getTasks(), secondPass.getTasks());
        final List<TaskOperationFailure> taskFailures = mergeTaskFailures(
            tasksByOriginalId,
            firstPass.getTaskFailures(),
            secondPass.getTaskFailures()
        );
        final List<ElasticsearchException> nodeFailures = dropStaleResourceNotFound(
            mergeNodeFailures(firstPass.getNodeFailures(), secondPass.getNodeFailures()),
            targetTaskId,
            tasksByOriginalId
        );
        return new ListTasksResponse(List.copyOf(tasksByOriginalId.values()), taskFailures, nodeFailures);
    }

    /// Deduplicate captured tasks across two passes, keyed by `originalTaskId`. Within a pass, [TransportListTasksAction#preferNewer]
    /// picks the post-relocation task when both physical tasks are visible. Across passes, the second pass wins on collision.
    static Map<TaskId, TaskInfo> mergeTasksByOriginalTaskId(final List<TaskInfo> firstPass, final List<TaskInfo> secondPass) {
        final Map<TaskId, TaskInfo> result = dedupWithinPass(secondPass);
        // Add only first-pass entries the second pass missed; within-pass dedup separately so intra-first-pass collisions still win by
        // preferNewer rather than insertion order.
        dedupWithinPass(firstPass).forEach(result::putIfAbsent);
        return Collections.unmodifiableMap(result);
    }

    private static Map<TaskId, TaskInfo> dedupWithinPass(final List<TaskInfo> tasks) {
        final Map<TaskId, TaskInfo> deduped = new LinkedHashMap<>(tasks.size());
        for (final TaskInfo t : tasks) {
            deduped.merge(t.originalTaskId(), t, TransportListTasksAction::preferNewer);
        }
        return deduped;
    }

    /// Deduplicate task failures across two passes; second pass wins. Drop failures whose physical `taskId` matches an `originalTaskId`
    /// of a captured task — the cancel committed elsewhere for that logical task.
    static List<TaskOperationFailure> mergeTaskFailures(
        final Map<TaskId, TaskInfo> tasksByOriginalId,
        final List<TaskOperationFailure> firstPass,
        final List<TaskOperationFailure> secondPass
    ) {
        final Map<String, TaskOperationFailure> deduped = new LinkedHashMap<>();
        for (final var pass : List.of(secondPass, firstPass)) {
            for (final TaskOperationFailure f : pass) {
                final TaskId failureTaskId = new TaskId(f.getNodeId(), f.getTaskId());
                if (tasksByOriginalId.containsKey(failureTaskId) == false) {
                    deduped.putIfAbsent(failureTaskId.toString(), f);
                }
            }
        }
        return List.copyOf(deduped.values());
    }

    /// Deduplicate node failures across two passes. Keyed by `nodeId` for [FailedNodeException], otherwise by message; second pass wins
    /// on collision.
    static List<ElasticsearchException> mergeNodeFailures(
        final List<ElasticsearchException> firstPass,
        final List<ElasticsearchException> secondPass
    ) {
        final Map<String, ElasticsearchException> deduped = new LinkedHashMap<>();
        for (final var pass : List.of(secondPass, firstPass)) {
            for (final ElasticsearchException f : pass) {
                final String key = f instanceof FailedNodeException fne ? fne.nodeId() : f.getMessage();
                deduped.putIfAbsent(key, f);
            }
        }
        return List.copyOf(deduped.values());
    }

    /// Drop "task not found" node failures (any failure whose cause chain contains a [ResourceNotFoundException]) when the merge
    /// captured at least one task for a targeted cancel.
    static List<ElasticsearchException> dropStaleResourceNotFound(
        final List<ElasticsearchException> failures,
        final TaskId targetTaskId,
        final Map<TaskId, TaskInfo> capturedTasks
    ) {
        if (capturedTasks.isEmpty() || targetTaskId == null || targetTaskId.isSet() == false) {
            return failures;
        }
        return failures.stream().filter(f -> ExceptionsHelper.unwrap(f, ResourceNotFoundException.class) == null).toList();
    }

    @Override
    protected List<CancellableTask> processTasks(CancelTasksRequest request) {
        if (request.getTargetTaskId().isSet()) {
            return findTargetTask(request);
        } else {
            final var tasks = new ArrayList<CancellableTask>();
            for (CancellableTask task : taskManager.getCancellableTasks().values()) {
                if (request.match(task) && match(task)) {
                    tasks.add(task);
                }
            }
            return tasks;
        }
    }

    /// Locates the target task by task id, or by original task id for relocated tasks.
    private List<CancellableTask> findTargetTask(CancelTasksRequest request) {
        final TaskId target = request.getTargetTaskId();
        // match by task id
        if (clusterService.localNode().getId().equals(target.getNodeId())) {
            final CancellableTask task = taskManager.getCancellableTask(target.getId());
            if (task != null) {
                if (request.match(task)) {
                    return List.of(task);
                } else {
                    throw new IllegalArgumentException("task [" + target + "] doesn't support this operation");
                }
            }
            // The task exists, but doesn't support cancellation
            if (taskManager.getTask(target.getId()) != null) {
                throw new IllegalArgumentException("task [" + target + "] doesn't support cancellation");
            }
        }
        // match by original task id for relocated tasks
        for (CancellableTask task : taskManager.getCancellableTasks().values()) {
            if (task.getOriginalTaskId().filter(target::equals).isPresent() && request.match(task)) {
                return List.of(task);
            }
        }
        return List.of();
    }

    @Override
    protected void taskOperation(
        CancellableTask actionTask,
        CancelTasksRequest request,
        CancellableTask cancellableTask,
        ActionListener<TaskInfo> listener
    ) {
        cancellableTask.ensureCancellable();
        String nodeId = clusterService.localNode().getId();
        taskManager.cancelTaskAndDescendants(
            cancellableTask,
            request.getReason(),
            request.waitForCompletion(),
            listener.map(r -> cancellableTask.taskInfo(nodeId, false))
        );
    }
}
