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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;

/**
 * Lifecycle hooks for instrumenting the HTTP server request/response cycle.
 */
public interface HttpServerInstrumentation {

    /**
     * Marks the start of HTTP request processing, starts a span and puts it into the {@link ThreadContext}.
     * Called once at the beginning of the request processing; the caller must dispatch the request before restoring the stored context.
     *
     * @param request       the incoming REST request
     * @param matchedRoute  the path template matched by the router (e.g. {@code /{index}/_search}),
     *                      or {@code null} if no handler was found for this request
     * @return the stored context that restores the thread context to what it was before this method was called
     */
    ThreadContext.StoredContext start(RestRequest request, @Nullable String matchedRoute);

    /**
     * Records an exception that occurred during request dispatch. May be called multiple times before
     * {@link #prepareEnd(RestRequest, RestResponse)}, does not mark the HTTP request handling as finished on its own.
     *
     * <p>This method <b>MUST</b> be called within the {@link ThreadContext} opened by the {@link #start} method.
     *
     * @param request the incoming REST request
     * @param t       the exception to record
     */
    void recordException(RestRequest request, Throwable t);

    /**
     * Captures any thread-local state needed for finalizing the instrumented operation, and returns a {@link Releasable} that records
     * metrics and ends the span for this request after the response has been sent.
     *
     * <p>This method <b>MUST</b> be called within the {@link ThreadContext} opened by the {@link #start} method.
     *
     * <p>The returned {@link Releasable} <b>MUST</b> be closed for the instrumented operation to end.
     *
     * @param request       the incoming REST request
     * @param response      the REST response that was sent to the client
     */
    Releasable prepareEnd(RestRequest request, RestResponse response);

    /** A no-op implementation of {@link HttpServerInstrumentation}. */
    HttpServerInstrumentation NOOP = new HttpServerInstrumentation() {
        @Override
        public ThreadContext.StoredContext start(RestRequest request, @Nullable String matchedRoute) {
            return () -> {};
        }

        @Override
        public void recordException(RestRequest request, Throwable t) {}

        @Override
        public Releasable prepareEnd(RestRequest request, RestResponse response) {
            return () -> {};
        }
    };
}
