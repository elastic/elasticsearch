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

/**
 * Represents the current position of a request within a {@link RestInterceptor} chain.
 *
 * <p>An interceptor receives a {@code RestInterceptorChain} that exposes the request, channel, and
 * target handler as they stand at that point in the chain. To pass control to the next interceptor
 * (or to the target handler if no interceptors remain), call one of the {@code proceed} methods.
 * To substitute the request, channel, or handler for downstream interceptors and the final handler,
 * use the four-argument {@link #proceed(RestRequest, RestChannel, RestHandler, ActionListener)}
 * overload.
 */
public interface RestInterceptorChain {

    /** Returns the REST request being processed at this point in the chain. */
    RestRequest request();

    /** Returns the channel through which the response will be sent at this point in the chain. */
    RestChannel channel();

    /** Returns the target {@link RestHandler} that will handle the request once all interceptors have proceeded. */
    RestHandler handler();

    /**
     * Advances to the next interceptor in the chain, or invokes the target handler if no interceptors remain, using the supplied request,
     * channel, and handler. Downstream interceptors and the final handler will see the values passed here.
     *
     * @param request  the request to pass downstream
     * @param channel  the channel to pass downstream
     * @param handler  the handler to invoke when the chain is exhausted
     * @param listener completed when the chain (including the final handler) finishes
     */
    void proceed(RestRequest request, RestChannel channel, RestHandler handler, ActionListener<Void> listener);

    /** Advances the chain using the current {@link #request()}, {@link #channel()}, and {@link #handler()}. */
    default void proceed(ActionListener<Void> listener) {
        proceed(request(), channel(), handler(), listener);
    }
}
