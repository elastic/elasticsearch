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
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportResponse;

/**
 * A client which can execute requests on a specific remote cluster.
 */
public interface RemoteClusterClient {
    /**
     * Executes an action, denoted by an {@link ActionType}, on the remote cluster.
     */
    default <Request extends ActionRequest, Response extends TransportResponse> void execute(
        RemoteClusterActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        getConnection(
            request,
            listener.delegateFailureAndWrap((responseListener, connection) -> execute(connection, action, request, responseListener))
        );
    }

    /**
     * Executes an action, denoted by an {@link ActionType}, using a connection to the remote cluster obtained using {@link #getConnection}.
     */
    <Request extends ActionRequest, Response extends TransportResponse> void execute(
        Transport.Connection connection,
        RemoteClusterActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    );

    /**
     * Obtain a connection to the remote cluster for use with the {@link #execute} override that allows to specify the connection. Useful
     * for cases where you need to inspect {@link Transport.Connection#getVersion} before deciding the exact remote action to invoke.
     */
    <Request extends ActionRequest> void getConnection(@Nullable Request request, ActionListener<Transport.Connection> listener);
}
