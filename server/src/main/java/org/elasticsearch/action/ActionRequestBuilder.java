/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.TimeValue;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

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
        Request request = request();
        RefCountedFuture<Response> future = new RefCountedFuture<>();
        client.execute(action, request, ActionListener.runAfter(future, request::decRef));
        // TODO: previously we were using RefCountedFuture for the response -- do we need that here?
        return future;
    }

    /**
     * Short version of execute().actionGet().
     */
    public Response get() {
        Request request = request();
        try {
            return execute().actionGet();
        } finally {
            request.decRef();
        }
    }

    /**
     * Short version of execute().actionGet().
     */
    public Response get(TimeValue timeout) {
        Request request = request();
        try {
            return execute().actionGet(timeout);
        } finally {
            request.decRef();
        }
    }

    public void execute(ActionListener<Response> listener) {
        Request request = request();
        client.execute(action, request, ActionListener.runAfter(listener, request::decRef));
    }

    private static class RefCountedFuture<R extends RefCounted> extends PlainActionFuture<R> {

        @Override
        public final void onResponse(R result) {
            result.mustIncRef();
            if (set(result) == false) {
                result.decRef();
            }
        }

        private final AtomicBoolean getCalled = new AtomicBoolean(false);

        @Override
        public R get() throws InterruptedException, ExecutionException {
            final boolean firstCall = getCalled.compareAndSet(false, true);
            if (firstCall == false) {
                final IllegalStateException ise = new IllegalStateException("must only call .get() once per instance to avoid leaks");
                assert false : ise;
                throw ise;
            }
            return super.get();
        }
    }
}
