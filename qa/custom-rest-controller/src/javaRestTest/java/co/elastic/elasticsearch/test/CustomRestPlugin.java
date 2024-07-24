/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.test;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.interceptor.RestServerActionPlugin;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestInterceptor;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.usage.UsageService;

public class CustomRestPlugin extends Plugin implements RestServerActionPlugin {

    private static final Logger logger = LogManager.getLogger(CustomRestPlugin.class);

    private static void echoHeader(String name, RestRequest request, ThreadContext threadContext) {
        var value = request.header(name);
        if (value != null) {
            threadContext.addResponseHeader(name, value);
        }
    }

    public static class CustomInterceptor implements RestInterceptor {

        private final ThreadContext threadContext;

        public CustomInterceptor(ThreadContext threadContext) {
            this.threadContext = threadContext;
        }

        @Override
        public void intercept(RestRequest request, RestChannel channel, RestHandler targetHandler, ActionListener<Boolean> listener)
            throws Exception {
            logger.info("intercept request {} {}", request.method(), request.uri());
            echoHeader("x-test-interceptor", request, threadContext);
            listener.onResponse(Boolean.TRUE);
        }

    }

    public static class CustomController extends RestController {
        public CustomController(
            RestInterceptor interceptor,
            NodeClient client,
            CircuitBreakerService circuitBreakerService,
            UsageService usageService,
            TelemetryProvider telemetryProvider
        ) {
            super(interceptor, client, circuitBreakerService, usageService, telemetryProvider);
        }

        @Override
        public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
            logger.info("dispatch request {} {}", request.method(), request.uri());
            echoHeader("x-test-controller", request, threadContext);
            super.dispatchRequest(request, channel, threadContext);
        }
    }

    @Override
    public RestInterceptor getRestHandlerInterceptor(ThreadContext threadContext) {
        return new CustomInterceptor(threadContext);
    }

    @Override
    public RestController getRestController(
        RestInterceptor interceptor,
        NodeClient client,
        CircuitBreakerService circuitBreakerService,
        UsageService usageService,
        TelemetryProvider telemetryProvider
    ) {
        return new CustomController(interceptor, client, circuitBreakerService, usageService, telemetryProvider);
    }

}
