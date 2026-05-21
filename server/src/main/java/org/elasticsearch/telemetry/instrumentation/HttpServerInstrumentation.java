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
 * Instrumentation hook for HTTP server spans. Implementations are responsible for starting,
 * annotating, and ending a tracing span over the lifetime of a single REST request.
 * <p>
 * The interface is defined entirely in ES types so that the {@code server} module remains
 * free of any direct OpenTelemetry library dependency; attribute extraction details live
 * in the APM module implementation.
 * <p>
 * {@link #NOOP} is available for environments where tracing is disabled.
 */
public interface HttpServerInstrumentation {

    /**
     * Called when a REST request is received and a trace span should be started.
     *
     * @param threadContext the current thread context (used to propagate span context)
     * @param request       the incoming REST request
     * @param matchedRoute  the matched route pattern (e.g. {@code "_index/_search"}), or {@code null}
     *                      when the route is not yet known (e.g. bad-request path)
     */
    void start(ThreadContext threadContext, RestRequest request, String matchedRoute);

    /**
     * Called when an exception occurs during request handling. Implementations should
     * record the error on the active span.
     */
    void recordException(RestRequest request, Throwable t);

    /**
     * Called when the response is sent and the span should be ended. Implementations
     * should record response attributes (status code, response headers) and close the span.
     */
    void end(RestRequest request, RestResponse response);

    HttpServerInstrumentation NOOP = new HttpServerInstrumentation() {
        @Override
        public void start(ThreadContext threadContext, RestRequest request, String matchedRoute) {}

        @Override
        public void recordException(RestRequest request, Throwable t) {}

        @Override
        public void end(RestRequest request, RestResponse response) {}
    };
}
