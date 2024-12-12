/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.migrate.action.CancelReindexDataStreamAction.Request;
import org.elasticsearch.xpack.migrate.task.ReindexDataStreamTask;

import java.util.Map;
import java.util.Optional;

public class CancelReindexDataStreamTransportAction extends HandledTransportAction<Request, AcknowledgedResponse> {
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final PersistentTasksService persistentTasksService;

    @Inject
    public CancelReindexDataStreamTransportAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        PersistentTasksService persistentTasksService
    ) {
        super(CancelReindexDataStreamAction.NAME, transportService, actionFilters, Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.persistentTasksService = persistentTasksService;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<AcknowledgedResponse> listener) {
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
                cancelTaskOnThisNode(persistentTaskId, listener);
            } else {
                runActionOnNodeWithTaskIfPossible(task, request, nodeId, listener);
            }
        } else {
            listener.onFailure(new ResourceNotFoundException("Persistent task with id [{}] is not assigned to a node", persistentTaskId));
        }

    }

    /*
     * This method cancels the persistent task with id persistentTaskId. It assumes that the task is running on this node.
     */
    private void cancelTaskOnThisNode(String persistentTaskId, ActionListener<AcknowledgedResponse> listener) {
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
            // Calling sendRemoveRequest results in the task (and its child tasks) being cancelled
            persistentTasksService.sendRemoveRequest(persistentTaskId, TimeValue.MAX_VALUE, new ActionListener<>() {
                @Override
                public void onResponse(PersistentTasksCustomMetadata.PersistentTask<?> persistentTask) {
                    // Calling unregister removes the task from the /_tasks list
                    taskManager.unregister(runningTask);
                    listener.onResponse(AcknowledgedResponse.TRUE);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }
    }

    /*
     * This gets the persistent task from the task manager. It must be run on the node where the persistent task is running.
     */
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

    /*
     * This forwards this CancelReindexDataStreamAction request to the node with id nodeId, since the task can only be canceled on the node
     * where it is running.
     */
    private void runActionOnNodeWithTaskIfPossible(
        Task thisTask,
        Request request,
        String nodeId,
        ActionListener<AcknowledgedResponse> listener
    ) {
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
                new ActionListenerResponseHandler<>(listener, AcknowledgedResponse::readFrom, EsExecutors.DIRECT_EXECUTOR_SERVICE)
            );
        }
    }
}
