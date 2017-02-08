/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * Service responsible for executing restartable actions that can survive disappearance of a coordinating and executor nodes.
 */
public class PersistentActionService extends AbstractComponent {

    private final Client client;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;

    public PersistentActionService(Settings settings, ThreadPool threadPool, ClusterService clusterService, Client client) {
        super(settings);
        this.client = client;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
    }

    public <Request extends PersistentActionRequest> void sendRequest(String action, Request request,
                                                                      ActionListener<PersistentActionResponse> listener) {
        StartPersistentTaskAction.Request startRequest = new StartPersistentTaskAction.Request(action, request);
        try {
            client.execute(StartPersistentTaskAction.INSTANCE, startRequest, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public void sendCompletionNotification(long taskId, Exception failure,
                                           ActionListener<CompletionPersistentTaskAction.Response> listener) {
        CompletionPersistentTaskAction.Request restartRequest = new CompletionPersistentTaskAction.Request(taskId, failure);
        // Need to fork otherwise: java.lang.AssertionError: should not be called by a cluster state applier.
        // reason [the applied cluster state is not yet available])
        try {
            threadPool.executor(ThreadPool.Names.GENERIC).execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }

                @Override
                protected void doRun() throws Exception {
                    client.execute(CompletionPersistentTaskAction.INSTANCE, restartRequest, listener);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public void sendCancellation(long taskId, ActionListener<CancelTasksResponse> listener) {
        DiscoveryNode localNode = clusterService.localNode();
        CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
        cancelTasksRequest.setTaskId(new TaskId(localNode.getId(), taskId));
        cancelTasksRequest.setReason("persistent action was removed");
        try {
            client.admin().cluster().cancelTasks(cancelTasksRequest, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public void updateStatus(long taskId, Task.Status status, ActionListener<UpdatePersistentTaskStatusAction.Response> listener) {
        UpdatePersistentTaskStatusAction.Request updateStatusRequest = new UpdatePersistentTaskStatusAction.Request(taskId, status);
        try {
            client.execute(UpdatePersistentTaskStatusAction.INSTANCE, updateStatusRequest, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}