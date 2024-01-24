/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.core.TimeValue;

import java.util.Objects;

/**
 * This class is similar to ActionRequestBuilder, except that it does not build the request until the request() method is called.
 * @param <Request>
 * @param <Response>
 */
public abstract class ActionRequestLazyBuilder<Request extends ActionRequest, Response extends ActionResponse> {

    protected final ActionType<Response> action;
    protected final ElasticsearchClient client;

    protected ActionRequestLazyBuilder(ElasticsearchClient client, ActionType<Response> action) {
        Objects.requireNonNull(action, "action must not be null");
        this.action = action;
        this.client = client;
    }

    /**
     * This method creates the request. The caller of this method is responsible for calling Request#decRef.
     * @return
     */
    public Request request() {
        Request request = newEmptyInstance();
        try {
            apply(request);
            return request;
        } catch (Exception e) {
            request.decRef();
            throw e;
        }
    }

    public ActionFuture<Response> execute() {
        return client.execute(action, request());
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

    public void execute(ActionListener<Response> listener) {
        client.execute(action, request(), listener);
    }

    /**
     * This method is meant to be implemented by sub-classes. If the default request() method is not overriden, this method will be
     * handed a newly-created instance of the request for the builder to populate.
     * @param request
     */
    protected void apply(Request request) {}

    /**
     * This method is meant to be implemented by sub-classes. If it is not possible to safely create a new empty instance of the request,
     * then the request() method must be overriden.
     * @return
     */
    protected abstract Request newEmptyInstance();
}
