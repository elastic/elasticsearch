/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.node.NodeClient;

import java.util.List;

/**
 * Default implementation of {@link RestInterceptorChain} that executes a fixed list of {@link RestInterceptor}s sequentially before
 * invoking the target {@link RestHandler}.
 */
public class DefaultRestInterceptorChain implements RestInterceptorChain {

    private final RestRequest request;
    private final RestChannel channel;
    private final RestHandler handler;
    private final NodeClient client;

    private final List<RestInterceptor> interceptors;
    private final int index;

    public DefaultRestInterceptorChain(
        RestRequest request,
        RestChannel channel,
        RestHandler handler,
        NodeClient client,
        List<RestInterceptor> interceptors
    ) {
        this(request, channel, handler, client, interceptors, 0);
    }

    private DefaultRestInterceptorChain(
        RestRequest request,
        RestChannel channel,
        RestHandler handler,
        NodeClient client,
        List<RestInterceptor> interceptors,
        int index
    ) {
        this.request = request;
        this.channel = channel;
        this.handler = handler;
        this.client = client;
        this.interceptors = interceptors;
        this.index = index;
    }

    @Override
    public RestRequest request() {
        return request;
    }

    @Override
    public RestChannel channel() {
        return channel;
    }

    @Override
    public RestHandler handler() {
        return handler;
    }

    @Override
    public void proceed(RestRequest request, RestChannel channel, RestHandler handler, ActionListener<Void> listener) {
        assert index <= interceptors.size();

        if (index == interceptors.size()) {
            try {
                handler.handleRequest(request, channel, client);
                listener.onResponse(null);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        } else {
            var interceptor = interceptors.get(index);
            interceptor.intercept(next(request, channel, handler), listener);
        }
    }

    private DefaultRestInterceptorChain next(RestRequest request, RestChannel channel, RestHandler handler) {
        return new DefaultRestInterceptorChain(request, channel, handler, client, interceptors, index + 1);
    }
}
