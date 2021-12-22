/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.internal;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.threadpool.ThreadPool;

public interface ElasticsearchClient {

    /**
     * Executes a generic action, denoted by an {@link ActionType}.
     *
     * @param action           The action type to execute.
     * @param request          The action request.
     * @param <Request>        The request type.
     * @param <Response>       the response type.
     * @return A future allowing to get back the response.
     */
    <Request extends ActionRequest, Response extends ActionResponse> ActionFuture<Response> execute(
        ActionType<Response> action,
        Request request
    );

    /**
     * Executes a generic action, denoted by an {@link ActionType}.
     *
     * @param action           The action type to execute.
     * @param request          The action request.
     * @param listener         The listener to receive the response back.
     * @param <Request>        The request type.
     * @param <Response>       The response type.
     */
    <Request extends ActionRequest, Response extends ActionResponse> void execute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    );

    /**
     * Returns the threadpool used to execute requests on this client
     */
    ThreadPool threadPool();

}
