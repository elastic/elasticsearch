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
 * Intercepts the execution of a {@link RestHandler} as part of a {@link RestInterceptorChain}.
 *
 * <p>Interceptors are arranged in a chain and executed in ascending {@link #order()} before the target handler runs. Each interceptor
 * receives the chain and a listener and must do exactly one of the following:
 * <ul>
 *   <li>Call {@link RestInterceptorChain#proceed(ActionListener)} (or the overload that allows substituting channel and/or handler) to pass
 *       control to the next interceptor (or to the target handler if this is the last interceptor in the chain).
 *   <li>Complete the listener directly: {@code listener.onResponse(null)} to indicate success, or {@code listener.onFailure(e)} to signal
 *       an error, without calling {@code chain.proceed}, thereby short-circuiting the remainder of the chain.
 * </ul>
 */
@FunctionalInterface
public interface RestInterceptor {

    /**
     * Intercepts a REST request.
     *
     * <p>Implementations must either call {@link RestInterceptorChain#proceed(ActionListener)} to continue the chain, or complete
     * the {@code listener} directly to short-circuit it.
     *
     * @param chain    the current position in the interceptor chain, exposing the request, channel, and target handler at this point in
     *                 execution
     * @param listener must either be completed directly or passed to {@code chain.proceed()}
     */
    void intercept(RestInterceptorChain chain, ActionListener<Void> listener);

    /**
     * The position of the interceptor in the chain. Execution proceeds from the lowest order to the highest. Interceptors with equal order
     * are sorted in an unspecified but stable order.
     */
    default int order() {
        return 0;
    }
}
