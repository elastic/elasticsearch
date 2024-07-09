/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.interceptor;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestInterceptor;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.usage.UsageService;

import java.util.function.UnaryOperator;

/**
 * An action plugin that intercepts incoming the REST requests.
 */
public interface RestServerActionPlugin extends ActionPlugin {

    /**
     * Returns a function used to intercept each rest request before handling the request.
     * The returned {@link UnaryOperator} is called for every incoming rest request and receives
     * the original rest handler as it's input. This allows adding arbitrary functionality around
     * rest request handlers to do for instance logging or authentication.
     * A simple example of how to only allow GET request is here:
     * <pre>
     * {@code
     *    UnaryOperator<RestHandler> getRestHandlerInterceptor(ThreadContext threadContext) {
     *      return originalHandler -> (RestHandler) (request, channel, client) -> {
     *        if (request.method() != Method.GET) {
     *          throw new IllegalStateException("only GET requests are allowed");
     *        }
     *        originalHandler.handleRequest(request, channel, client);
     *      };
     *    }
     * }
     * </pre>
     *
     * Note: Only one installed plugin may implement a rest interceptor.
     */
    RestInterceptor getRestHandlerInterceptor(ThreadContext threadContext);

    /**
     * Returns a replacement {@link RestController} to be used in the server.
     * Note: Only one installed plugin may override the rest controller.
     */
    @Nullable
    default RestController getRestController(
        @Nullable RestInterceptor interceptor,
        NodeClient client,
        CircuitBreakerService circuitBreakerService,
        UsageService usageService,
        TelemetryProvider telemetryProvider
    ) {
        return null;
    }
}
