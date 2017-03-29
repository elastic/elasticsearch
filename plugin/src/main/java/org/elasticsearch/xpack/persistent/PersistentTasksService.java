/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

/**
 * This service is used by persistent actions to propagate changes in the action state and notify about completion
 */
public class PersistentTasksService extends AbstractComponent {

    private final Client client;
    private final ClusterService clusterService;

    public PersistentTasksService(Settings settings, ClusterService clusterService, Client client) {
        super(settings);
        this.client = client;
        this.clusterService = clusterService;
    }

    /**
     * Creates the specified persistent action. The action is started unless the stopped parameter is equal to true.
     * If removeOnCompletion parameter is equal to true, the task is removed from the cluster state upon completion.
     * Otherwise it will remain there in the stopped state.
     */
    public <Request extends PersistentTaskRequest> void createPersistentActionTask(String action, Request request,
                                                                                   PersistentTaskOperationListener listener) {
        CreatePersistentTaskAction.Request createPersistentActionRequest = new CreatePersistentTaskAction.Request(action, request);
        try {
            client.execute(CreatePersistentTaskAction.INSTANCE, createPersistentActionRequest, ActionListener.wrap(
                    o -> listener.onResponse(o.getTaskId()), listener::onFailure));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Notifies the PersistentTasksClusterService about successful (failure == null) completion of a task or its failure
     *
     */
    public void sendCompletionNotification(long taskId, Exception failure, PersistentTaskOperationListener listener) {
        CompletionPersistentTaskAction.Request restartRequest = new CompletionPersistentTaskAction.Request(taskId, failure);
        try {
            client.execute(CompletionPersistentTaskAction.INSTANCE, restartRequest, ActionListener.wrap(o -> listener.onResponse(taskId),
                    listener::onFailure));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Cancels the persistent task.
     */
    public void sendCancellation(long taskId, PersistentTaskOperationListener listener) {
        DiscoveryNode localNode = clusterService.localNode();
        CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
        cancelTasksRequest.setTaskId(new TaskId(localNode.getId(), taskId));
        cancelTasksRequest.setReason("persistent action was removed");
        try {
            client.admin().cluster().cancelTasks(cancelTasksRequest, ActionListener.wrap(o -> listener.onResponse(taskId),
                    listener::onFailure));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Updates status of the persistent task
     */
    public void updateStatus(long taskId, Task.Status status, PersistentTaskOperationListener listener) {
        UpdatePersistentTaskStatusAction.Request updateStatusRequest = new UpdatePersistentTaskStatusAction.Request(taskId, status);
        try {
            client.execute(UpdatePersistentTaskStatusAction.INSTANCE, updateStatusRequest, ActionListener.wrap(
                    o -> listener.onResponse(taskId), listener::onFailure));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Removes a persistent task
     */
    public void removeTask(long taskId, PersistentTaskOperationListener listener) {
        RemovePersistentTaskAction.Request removeRequest = new RemovePersistentTaskAction.Request(taskId);
        try {
            client.execute(RemovePersistentTaskAction.INSTANCE, removeRequest, ActionListener.wrap(o -> listener.onResponse(taskId),
                    listener::onFailure));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Starts a persistent task
     */
    public void startTask(long taskId, PersistentTaskOperationListener listener) {
        StartPersistentTaskAction.Request startRequest = new StartPersistentTaskAction.Request(taskId);
        try {
            client.execute(StartPersistentTaskAction.INSTANCE, startRequest, ActionListener.wrap(o -> listener.onResponse(taskId),
                    listener::onFailure));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public interface PersistentTaskOperationListener {
        void onResponse(long taskId);
        void onFailure(Exception e);
    }

}