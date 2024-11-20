/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
public abstract class ActionRequestLazyBuilder<Request extends ActionRequest, Response extends ActionResponse>
    implements
        RequestBuilder<Request, Response> {

    protected final ActionType<Response> action;
    protected final ElasticsearchClient client;

    protected ActionRequestLazyBuilder(ElasticsearchClient client, ActionType<Response> action) {
        Objects.requireNonNull(action, "action must not be null");
        this.action = action;
        this.client = client;
    }

    /**
     * This method creates the request. The caller of this method is responsible for calling Request#decRef.
     * @return A newly-built Request, fully initialized by this builder.
     */
    public abstract Request request();

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
}
