/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.persistent;

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
        CreatePersistentTaskAction.Request startRequest = new CreatePersistentTaskAction.Request(action, request);
        try {
            client.execute(CreatePersistentTaskAction.INSTANCE, startRequest, listener);
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
