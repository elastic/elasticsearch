/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.client.internal;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportResponse;

import java.util.concurrent.Executor;

/**
 * A {@linkplain Client} that sets the parent task on all requests that it makes. Use this to conveniently implement actions that cause
 * many other actions.
 */
public class ParentTaskAssigningClient extends FilterClient {
    private final TaskId parentTask;

    /**
     * Standard constructor.
     */
    public ParentTaskAssigningClient(Client in, TaskId parentTask) {
        super(in);
        this.parentTask = parentTask;
    }

    /**
     * Convenience constructor for building the TaskId out of what is usually at hand.
     */
    public ParentTaskAssigningClient(Client in, DiscoveryNode localNode, Task parentTask) {
        this(in, new TaskId(localNode.getId(), parentTask.getId()));
    }

    public TaskId getParentTask() {
        return parentTask;
    }

    /**
     * Fetch the wrapped client. Use this to make calls that don't set {@link ActionRequest#setParentTask(TaskId)}.
     */
    public Client unwrap() {
        return in();
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        request.setParentTask(parentTask);
        super.doExecute(action, request, listener);
    }

    @Override
    public RemoteClusterClient getRemoteClusterClient(
        String clusterAlias,
        Executor responseExecutor,
        RemoteClusterService.DisconnectedStrategy disconnectedStrategy
    ) {
        final var delegate = super.getRemoteClusterClient(clusterAlias, responseExecutor, disconnectedStrategy);
        return new RemoteClusterClient() {
            @Override
            public <Request extends ActionRequest, Response extends TransportResponse> void execute(
                Transport.Connection connection,
                RemoteClusterActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                request.setParentTask(parentTask);
                delegate.execute(connection, action, request, listener);
            }

            @Override
            public <Request extends ActionRequest> void getConnection(Request request, ActionListener<Transport.Connection> listener) {
                delegate.getConnection(request, listener);
            }
        };
    }
}
