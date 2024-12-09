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
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.migrate.action.CancelReindexDataStreamAction.Request;
import org.elasticsearch.xpack.migrate.action.CancelReindexDataStreamAction.Response;
import org.elasticsearch.xpack.migrate.task.ReindexDataStreamTask;

import java.util.Map;
import java.util.Optional;

public class CancelReindexDataStreamTransportAction extends HandledTransportAction<Request, Response> {
    private final ClusterService clusterService;
    private final TransportService transportService;

    @Inject
    public CancelReindexDataStreamTransportAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters
    ) {
        super(CancelReindexDataStreamAction.NAME, transportService, actionFilters, Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
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
            listener.onFailure(new ResourceNotFoundException("No migration reindex task found for [{}]", index));
        } else if (persistentTask.isAssigned()) {
            String nodeId = persistentTask.getExecutorNode();
            if (clusterService.localNode().getId().equals(nodeId)) {
                getRunningTaskFromNode(persistentTaskId, listener);
            } else {
                runOnNodeWithTaskIfPossible(task, request, nodeId, listener);
            }
        } else {
            listener.onFailure(new ResourceNotFoundException("Persistent task with id [{}] is not assigned to a node", persistentTaskId));
        }
    }

    private ReindexDataStreamTask getRunningPersistentTaskFromTaskManager(String persistentTaskId) {
        Optional<Map.Entry<Long, CancellableTask>> optionalTask = taskManager.getCancellableTasks()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().getType().equals("persistent"))
            .filter(
                entry -> entry.getValue() instanceof ReindexDataStreamTask
                    && persistentTaskId.equals((((AllocatedPersistentTask) entry.getValue()).getPersistentTaskId()))
            )
            .findAny();
        return optionalTask.map(entry -> (ReindexDataStreamTask) entry.getValue()).orElse(null);
    }

    void getRunningTaskFromNode(String persistentTaskId, ActionListener<Response> listener) {
        ReindexDataStreamTask runningTask = getRunningPersistentTaskFromTaskManager(persistentTaskId);
        if (runningTask == null) {
            listener.onFailure(
                new ResourceNotFoundException(
                    Strings.format(
                        "Persistent task [{}] is supposed to be running on node [{}], but the task is not found on that node",
                        persistentTaskId,
                        clusterService.localNode().getId()
                    )
                )
            );
        } else {
            runningTask.markAsCompleted();
            listener.onResponse(new Response());
        }
    }

    private void runOnNodeWithTaskIfPossible(Task thisTask, Request request, String nodeId, ActionListener<Response> listener) {
        DiscoveryNode node = clusterService.state().nodes().get(nodeId);
        if (node == null) {
            listener.onFailure(
                new ResourceNotFoundException(
                    Strings.format(
                        "Persistent task [{}] is supposed to be running on node [{}], but that node is not part of the cluster",
                        request.getIndex(),
                        nodeId
                    )
                )
            );
        } else {
            Request nodeRequest = request.nodeRequest(clusterService.localNode().getId(), thisTask.getId());
            transportService.sendRequest(
                node,
                CancelReindexDataStreamAction.NAME,
                nodeRequest,
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(listener, Response::new, EsExecutors.DIRECT_EXECUTOR_SERVICE)
            );
        }
    }
}
