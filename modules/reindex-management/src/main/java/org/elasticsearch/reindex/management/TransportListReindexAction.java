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
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction.TASKS_ORIGIN;

/// Transport action for listing all running reindex tasks.
/// Delegates to {@link TransportListTasksAction} to fan out to all nodes (which handles deduplication if we list a non-relocated and
/// relocated task), then filters for reindex parent tasks and rewrites task identity to reflect the original (pre-relocation) task.
public class TransportListReindexAction extends HandledTransportAction<ListReindexRequest, ListReindexResponse> {

    public static final ActionType<ListReindexResponse> TYPE = new ActionType<>("cluster:monitor/reindex/list");

    private final Client client;

    @Inject
    public TransportListReindexAction(final TransportService transportService, final ActionFilters actionFilters, final Client client) {
        super(TYPE.name(), transportService, actionFilters, ListReindexRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.client = new OriginSettingClient(Objects.requireNonNull(client), TASKS_ORIGIN);
    }

    @Override
    protected void doExecute(final Task task, final ListReindexRequest request, final ActionListener<ListReindexResponse> listener) {
        final ListTasksRequest listTasksRequest = new ListTasksRequest();
        listTasksRequest.setActions(ReindexAction.NAME);
        listTasksRequest.setDetailed(request.getDetailed());

        client.execute(TransportListTasksAction.TYPE, listTasksRequest, listener.delegateFailureAndWrap((l, response) -> {
            final List<TaskInfo> tasks = response.getTasks()
                .stream()
                .filter(t -> t.parentTaskId().isSet() == false)
                .map(TransportListReindexAction::relocatedTaskInfo)
                .toList();
            l.onResponse(new ListReindexResponse(tasks, response.getTaskFailures(), response.getNodeFailures()));
        }));
    }

    /// Rewrite a {@link TaskInfo} so the caller sees the original (pre-relocation) identity.
    /// For non-relocated tasks this is a no-op because {@code originalTaskId == taskId} and
    /// {@code originalStartTimeMillis == startTime}.
    private static TaskInfo relocatedTaskInfo(final TaskInfo info) {
        assert ReindexAction.NAME.equals(info.action()) : "unexpected task action [" + info.action() + "]";
        assert info.parentTaskId().isSet() == false : "unexpected child task with parent [" + info.parentTaskId() + "]";
        final TaskId originalId = info.originalTaskId();
        final long originalStartMillis = info.originalStartTimeMillis();
        final long adjustedRunningTimeNanos = info.runningTimeNanos() + TimeUnit.MILLISECONDS.toNanos(
            info.startTime() - originalStartMillis
        );
        return new TaskInfo(
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
}
