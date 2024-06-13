/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.internal;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.transport.TransportResponse;

/**
 * A client which can execute requests on a specific remote cluster.
 */
public interface RemoteClusterClient {
    /**
     * Executes an action, denoted by an {@link ActionType}, on the remote cluster.
     *
     * @param action           The action type to execute.
     * @param request          The action request.
     * @param listener         A listener for the response
     * @param <Request>        The request type.
     * @param <Response>       the response type.
     */
    <Request extends ActionRequest, Response extends TransportResponse> void execute(
        RemoteClusterActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    );
}
