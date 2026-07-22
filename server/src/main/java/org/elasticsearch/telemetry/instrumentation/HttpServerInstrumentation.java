/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.instrumentation;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;

/**
 * Lifecycle hooks for instrumenting the HTTP server request/response cycle.
 */
public interface HttpServerInstrumentation {

    /**
     * Marks the start of HTTP request processing and starts a span. Called once at the beginning of the request processing.
     *
     * @param threadContext the active thread context, used to propagate trace state
     * @param request       the incoming REST request
     * @param matchedRoute  the path template matched by the router (e.g. {@code /{index}/_search}),
     *                      or {@code null} if no handler was found for this request
     */
    void start(ThreadContext threadContext, RestRequest request, String matchedRoute);

    /**
     * Records an exception that occurred during request dispatch. May be called multiple times before {@link #end}.
     *
     * @param request the incoming REST request
     * @param t       the exception to record
     */
    void recordException(RestRequest request, Throwable t);

    /**
     * Finalizes the trace span for this request after the response has been sent.
     *
     * @param request  the incoming REST request
     * @param response the REST response that was sent to the client
     */
    void end(RestRequest request, RestResponse response);

    /** A no-op implementation of {@link HttpServerInstrumentation}. */
    HttpServerInstrumentation NOOP = new HttpServerInstrumentation() {
        @Override
        public void start(ThreadContext threadContext, RestRequest request, String matchedRoute) {}

        @Override
        public void recordException(RestRequest request, Throwable t) {}

        @Override
        public void end(RestRequest request, RestResponse response) {}
    };
}
