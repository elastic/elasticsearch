/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import org.elasticsearch.rest.RestContentTypePolicy;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestInterceptor;
import org.elasticsearch.rest.RestInterceptorChain;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.usage.UsageService;

import java.util.List;

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
        public void intercept(RestInterceptorChain chain, ActionListener<Void> listener) {
            logger.info("intercept request {} {}", chain.request().method(), chain.request().uri());
            echoHeader("x-test-interceptor", chain.request(), threadContext);
            chain.proceed(listener);
        }

    }

    public static class CustomController extends RestController {
        public CustomController(
            List<RestInterceptor> interceptors,
            RestContentTypePolicy contentTypePolicy,
            NodeClient client,
            CircuitBreakerService circuitBreakerService,
            UsageService usageService,
            TelemetryProvider telemetryProvider
        ) {
            super(interceptors, contentTypePolicy, client, circuitBreakerService, usageService, telemetryProvider);
        }

        @Override
        public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
            logger.info("dispatch request {} {}", request.method(), request.uri());
            echoHeader("x-test-controller", request, threadContext);
            super.dispatchRequest(request, channel, threadContext);
        }
    }

    @Override
    public List<RestInterceptor> getRestHandlerInterceptors(ThreadContext threadContext) {
        return List.of(new CustomInterceptor(threadContext));
    }

    @Override
    public RestController getRestController(
        List<RestInterceptor> interceptors,
        RestContentTypePolicy contentTypePolicy,
        NodeClient client,
        CircuitBreakerService circuitBreakerService,
        UsageService usageService,
        TelemetryProvider telemetryProvider
    ) {
        return new CustomController(interceptors, contentTypePolicy, client, circuitBreakerService, usageService, telemetryProvider);
    }
}
