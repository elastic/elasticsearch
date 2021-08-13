/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.core.TimeValue;

import java.util.Objects;

public abstract class ActionRequestBuilder<Request extends ActionRequest, Response extends ActionResponse> {

    protected final ActionType<Response> action;
    protected final Request request;
    protected final ElasticsearchClient client;

    protected ActionRequestBuilder(ElasticsearchClient client, ActionType<Response> action, Request request) {
        Objects.requireNonNull(action, "action must not be null");
        this.action = action;
        this.request = request;
        this.client = client;
    }

    public Request request() {
        return this.request;
    }

    public ActionFuture<Response> execute() {
        return client.execute(action, request);
    }

    /**
     * Short version of execute().actionGet().
     */
    public Response get() {
        return execute().actionGet();
    }

    /**
     * Short version of execute().actionGet().
     */
    public Response get(TimeValue timeout) {
        return execute().actionGet(timeout);
    }

    /**
     * Short version of execute().actionGet().
     */
    public Response get(String timeout) {
        return execute().actionGet(timeout);
    }

    public void execute(ActionListener<Response> listener) {
        client.execute(action, request, listener);
    }
}
