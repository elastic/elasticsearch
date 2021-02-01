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

package org.elasticsearch.client.node;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskListener;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.Transport;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Client that executes actions on the local node.
 */
public class NodeClient extends AbstractClient {

    @SuppressWarnings("rawtypes")
    private Map<ActionType, TransportAction> actions;

    private TaskManager taskManager;

    /**
     * The id of the local {@link DiscoveryNode}. Useful for generating task ids from tasks returned by
     * {@link #executeLocally(ActionType, ActionRequest, TaskListener)}.
     */
    private Supplier<String> localNodeId;
    private Transport.Connection localConnection;
    private RemoteClusterService remoteClusterService;
    private NamedWriteableRegistry namedWriteableRegistry;

    public NodeClient(Settings settings, ThreadPool threadPool) {
        super(settings, threadPool);
    }

    @SuppressWarnings("rawtypes")
    public void initialize(Map<ActionType, TransportAction> actions, TaskManager taskManager, Supplier<String> localNodeId,
                           Transport.Connection localConnection, RemoteClusterService remoteClusterService,
                           NamedWriteableRegistry namedWriteableRegistry) {
        this.actions = actions;
        this.taskManager = taskManager;
        this.localNodeId = localNodeId;
        this.localConnection = localConnection;
        this.remoteClusterService = remoteClusterService;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    @Override
    public void close() {
        // nothing really to do
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse>
    void doExecute(ActionType<Response> action, Request request, ActionListener<Response> listener) {
        // Discard the task because the Client interface doesn't use it.
        try {
            executeLocally(action, request, listener);
        } catch (TaskCancelledException | IllegalArgumentException | IllegalStateException e) {
            // #executeLocally returns the task and throws TaskCancelledException if it fails to register the task because the parent
            // task has been cancelled, IllegalStateException if the client was not in a state to execute the request because it was not
            // yet properly initialized or IllegalArgumentException if header validation fails we forward them to listener since this API
            // does not concern itself with the specifics of the task handling
            listener.onFailure(e);
        }
    }

    /**
     * Execute an {@link ActionType} locally, returning that {@link Task} used to track it, and linking an {@link ActionListener}.
     * Prefer this method if you don't need access to the task when listening for the response. This is the method used to
     * implement the {@link Client} interface.
     *
     * @throws TaskCancelledException if the request's parent task has been cancelled already
     */
    public <    Request extends ActionRequest,
                Response extends ActionResponse
            > Task executeLocally(ActionType<Response> action, Request request, ActionListener<Response> listener) {
        return taskManager.registerAndExecute("transport", transportAction(action), request, localConnection,
                (t, r) -> {
                    try {
                        listener.onResponse(r);
                    } catch (Exception e) {
                        assert false : new AssertionError("callback must handle its own exceptions", e);
                        throw e;
                    }
                }, (t, e) -> {
                    try {
                        listener.onFailure(e);
                    } catch (Exception ex) {
                        ex.addSuppressed(e);
                        assert false : new AssertionError("callback must handle its own exceptions", ex);
                        throw ex;
                    }
                });
    }

    /**
     * Execute an {@link ActionType} locally, returning that {@link Task} used to track it, and linking an {@link TaskListener}.
     * Prefer this method if you need access to the task when listening for the response.
     *
     * @throws TaskCancelledException if the request's parent task has been cancelled already
     */
    public <    Request extends ActionRequest,
                Response extends ActionResponse
            > Task executeLocally(ActionType<Response> action, Request request, TaskListener<Response> listener) {
        return taskManager.registerAndExecute("transport", transportAction(action), request, localConnection,
            listener::onResponse, listener::onFailure);
    }

    /**
     * The id of the local {@link DiscoveryNode}. Useful for generating task ids from tasks returned by
     * {@link #executeLocally(ActionType, ActionRequest, TaskListener)}.
     */
    public String getLocalNodeId() {
        return localNodeId.get();
    }

    /**
     * Get the {@link TransportAction} for an {@link ActionType}, throwing exceptions if the action isn't available.
     */
    @SuppressWarnings("unchecked")
    private <    Request extends ActionRequest,
                Response extends ActionResponse
            > TransportAction<Request, Response> transportAction(ActionType<Response> action) {
        if (actions == null) {
            throw new IllegalStateException("NodeClient has not been initialized");
        }
        TransportAction<Request, Response> transportAction = actions.get(action);
        if (transportAction == null) {
            throw new IllegalStateException("failed to find action [" + action + "] to execute");
        }
        return transportAction;
    }

    @Override
    public Client getRemoteClusterClient(String clusterAlias) {
        return remoteClusterService.getRemoteClusterClient(threadPool(), clusterAlias);
    }


    public NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }
}
