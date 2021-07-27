/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.node;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterService;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Client that executes actions on the local node.
 */
public class NodeClient extends AbstractClient {

    private Map<ActionType<? extends ActionResponse>, TransportAction<? extends ActionRequest, ? extends ActionResponse>> actions;
    /**
     * The id of the local {@link DiscoveryNode}. Useful for generating task ids from tasks returned by
     * {@link #executeLocally(ActionType, ActionRequest, TaskListener)}.
     */
    private Supplier<String> localNodeId;
    private RemoteClusterService remoteClusterService;
    private NamedWriteableRegistry namedWriteableRegistry;

    public NodeClient(Settings settings, ThreadPool threadPool) {
        super(settings, threadPool);
    }

    public void initialize(
        Map<ActionType<? extends ActionResponse>, TransportAction<? extends ActionRequest, ? extends ActionResponse>> actions,
        Supplier<String> localNodeId,
        RemoteClusterService remoteClusterService,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        this.actions = actions;
        this.localNodeId = localNodeId;
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
     * Prefer this method if you don't need access to the task when listening for the response. This is the method used to implement
     * the {@link Client} interface.
     *
     * @throws TaskCancelledException if the request's parent task has been cancelled already
     */
    public <    Request extends ActionRequest,
                Response extends ActionResponse
            > Task executeLocally(ActionType<Response> action, Request request, ActionListener<Response> listener) {
        return transportAction(action).execute(request, listener);
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
        return transportAction(action).execute(request, listener);
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
    private <    Request extends ActionRequest,
                Response extends ActionResponse
            > TransportAction<Request, Response> transportAction(ActionType<Response> action) {
        if (actions == null) {
            throw new IllegalStateException("NodeClient has not been initialized");
        }
        @SuppressWarnings("unchecked")
        TransportAction<Request, Response> transportAction = (TransportAction<Request, Response>) actions.get(action);
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
