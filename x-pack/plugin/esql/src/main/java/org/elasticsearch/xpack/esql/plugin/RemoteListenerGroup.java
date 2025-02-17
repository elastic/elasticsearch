/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.session.EsqlCCSUtils;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Create group task for this cluster. This group task ensures that two branches of the computation:
 * the exchange sink and the cluster request, belong to the same group and each of them can cancel the other.
 * runAfter listeners below ensure that the group is finalized when both branches are done.
 * The group task is the child of the root task, so if the root task is cancelled, the group task is cancelled too.
 */
class RemoteListenerGroup {
    private final CancellableTask groupTask;
    private final ActionListener<Void> exchangeRequestListener;
    private final ActionListener<List<DriverProfile>> clusterRequestListener;
    private final TaskManager taskManager;
    private final String clusterAlias;
    private final EsqlExecutionInfo executionInfo;
    private final TransportService transportService;

    RemoteListenerGroup(
        TransportService transportService,
        Task rootTask,
        ComputeListener computeListener,
        String clusterAlias,
        EsqlExecutionInfo executionInfo,
        ActionListener<Void> delegate
    ) {
        this.transportService = transportService;
        this.taskManager = transportService.getTaskManager();
        this.clusterAlias = clusterAlias;
        this.executionInfo = executionInfo;
        groupTask = createGroupTask(transportService, rootTask, () -> rootTask.getDescription() + "[" + clusterAlias + "]");
        CountDown countDown = new CountDown(2);
        // The group is done when both the sink and the cluster request are done
        Runnable finishGroup = () -> {
            if (countDown.countDown()) {
                taskManager.unregister(groupTask);
                delegate.onResponse(null);
            }
        };
        // Cancel the group on sink failure
        exchangeRequestListener = createCancellingListener("exchange sink failure", computeListener.acquireAvoid(), finishGroup);

        // Cancel the group on cluster request failure
        clusterRequestListener = createCancellingListener("exchange cluster action failure", computeListener.acquireCompute(), finishGroup);
    }

    /**
     * Create a listener that:
     * 1. Cancels the group task on failure
     * 2. Marks the cluster as partial if the error is ignorable, otherwise propagates the error
     */
    private <T> ActionListener<T> createCancellingListener(String reason, ActionListener<T> delegate, Runnable finishGroup) {
        return ActionListener.runAfter(delegate.delegateResponse((inner, e) -> {
            taskManager.cancelTaskAndDescendants(groupTask, reason, true, ActionListener.running(() -> {
                EsqlCCSUtils.skipUnavailableListener(inner, executionInfo, clusterAlias, EsqlExecutionInfo.Cluster.Status.PARTIAL)
                    .onFailure(e);
            }));
        }), finishGroup);
    }

    public CancellableTask getGroupTask() {
        return groupTask;
    }

    public ActionListener<Void> getExchangeRequestListener() {
        return exchangeRequestListener;
    }

    public ActionListener<List<DriverProfile>> getClusterRequestListener() {
        return clusterRequestListener;
    }

    public static CancellableTask createGroupTask(TransportService transportService, Task parentTask, Supplier<String> description) {
        final TaskManager taskManager = transportService.getTaskManager();
        return (CancellableTask) taskManager.register(
            "transport",
            "esql_compute_group",
            new ComputeGroupTaskRequest(parentTask.taskInfo(transportService.getLocalNode().getId(), false).taskId(), description)
        );
    }

    private static class ComputeGroupTaskRequest extends TransportRequest {
        private final Supplier<String> parentDescription;

        ComputeGroupTaskRequest(TaskId parentTask, Supplier<String> description) {
            this.parentDescription = description;
            setParentTask(parentTask);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            assert parentTaskId.isSet();
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }

        @Override
        public String getDescription() {
            return "group [" + parentDescription.get() + "]";
        }
    }
}
