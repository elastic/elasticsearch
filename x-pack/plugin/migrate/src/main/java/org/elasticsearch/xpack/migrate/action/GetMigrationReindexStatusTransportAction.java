/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Strings;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.migrate.action.GetMigrationReindexStatusAction.Request;
import org.elasticsearch.xpack.migrate.action.GetMigrationReindexStatusAction.Response;

import java.util.Map;
import java.util.Optional;

public class GetMigrationReindexStatusTransportAction extends HandledTransportAction<Request, Response> {
    private final ClusterService clusterService;
    private final TransportService transportService;

    @Inject
    public GetMigrationReindexStatusTransportAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters
    ) {
        super(GetMigrationReindexStatusAction.NAME, transportService, actionFilters, Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.clusterService = clusterService;
        this.transportService = transportService;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        String index = request.getIndex();
        String persistentTaskId = ReindexDataStreamAction.TASK_ID_PREFIX + index;
        PersistentTasksCustomMetadata persistentTasksCustomMetadata = clusterService.state()
            .getMetadata()
            .custom(PersistentTasksCustomMetadata.TYPE);
        PersistentTasksCustomMetadata.PersistentTask<?> persistentTask = persistentTasksCustomMetadata.getTask(persistentTaskId);
        if (persistentTask == null) {
            listener.onFailure(new ResourceNotFoundException("No migration reindex status found for [{}]", index));
        } else if (persistentTask.isAssigned()) {
            String nodeId = persistentTask.getExecutorNode();
            if (clusterService.localNode().getId().equals(nodeId)) {
                getRunningTaskFromNode(persistentTaskId, listener);
            } else {
                runOnNodeWithTaskIfPossible(task, request, nodeId, listener);
            }
        } else {
            listener.onFailure(new ElasticsearchException("Persistent task with id [{}] is not assigned to a node", persistentTaskId));
        }
    }

    private Task getRunningPersistentTaskFromTaskManager(String persistentTaskId) {
        Optional<Map.Entry<Long, CancellableTask>> optionalTask = taskManager.getCancellableTasks()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().getType().equals("persistent"))
            .filter(
                entry -> entry.getValue() instanceof AllocatedPersistentTask
                    && persistentTaskId.equals((((AllocatedPersistentTask) entry.getValue()).getPersistentTaskId()))
            )
            .findAny();
        return optionalTask.<Task>map(Map.Entry::getValue).orElse(null);
    }

    void getRunningTaskFromNode(String persistentTaskId, ActionListener<Response> listener) {
        Task runningTask = getRunningPersistentTaskFromTaskManager(persistentTaskId);
        if (runningTask == null) {
            listener.onFailure(
                new ResourceNotFoundException(
                    Strings.format(
                        "Persistent task [%s] is supposed to be running on node [%s], but the task is not found on that node",
                        persistentTaskId,
                        clusterService.localNode().getId()
                    )
                )
            );
        } else {
            TaskInfo info = runningTask.taskInfo(clusterService.localNode().getId(), true);
            listener.onResponse(new Response(new TaskResult(false, info)));
        }
    }

    private void runOnNodeWithTaskIfPossible(Task thisTask, Request request, String nodeId, ActionListener<Response> listener) {
        DiscoveryNode node = clusterService.state().nodes().get(nodeId);
        if (node == null) {
            listener.onFailure(
                new ResourceNotFoundException(
                    Strings.format(
                        "Persistent task [%s] is supposed to be running on node [%s], but that node is not part of the cluster",
                        request.getIndex(),
                        nodeId
                    )
                )
            );
        } else {
            Request nodeRequest = request.nodeRequest(clusterService.localNode().getId(), thisTask.getId());
            transportService.sendRequest(
                node,
                GetMigrationReindexStatusAction.NAME,
                nodeRequest,
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(listener, Response::new, EsExecutors.DIRECT_EXECUTOR_SERVICE)
            );
        }
    }
}
