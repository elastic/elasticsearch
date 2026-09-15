/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins.interceptor;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.rest.RestContentTypePolicy;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestInterceptor;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.usage.UsageService;

import java.util.List;

/**
 * Extension point for Elastic-internal plugins that need to hook into the REST layer.
 *
 * <p><strong>This interface is restricted to internal plugins.</strong>
 * At startup, {@code ActionModule} verifies that every plugin implementing this interface belongs to the {@code org.elasticsearch.xpack}
 * or {@code co.elastic.elasticsearch} package hierarchy.
 */
public interface RestServerActionPlugin extends ActionPlugin {

    /**
     * Returns {@link RestInterceptor}s contributed by this plugin.
     *
     * <p>Interceptors from all installed internal plugins are collected and sorted by {@link RestInterceptor#order()}. Multiple plugins may
     * each contribute interceptors; the lists are merged and sorted globally.
     */
    default List<RestInterceptor> getRestHandlerInterceptors(ThreadContext threadContext) {
        return List.of();
    }

    /**
     * Returns a {@link RestContentTypePolicy} that decides whether a request may use browser-safelisted content types such as
     * {@code application/x-www-form-urlencoded}.
     *
     * <p>At most one installed plugin may return a non-{@code null} value; a second non-null policy causes startup to fail.
     */
    @Nullable
    default RestContentTypePolicy getRestContentTypePolicy(ThreadContext threadContext) {
        return null;
    }

    /**
     * Returns a replacement {@link RestController} to be used instead of the default one.
     *
     * <p>At most one installed plugin may return a non-{@code null} value; a second non-null controller causes startup to fail.
     */
    @Nullable
    default RestController getRestController(
        List<RestInterceptor> interceptors,
        RestContentTypePolicy contentTypePolicy,
        NodeClient client,
        CircuitBreakerService circuitBreakerService,
        UsageService usageService,
        TelemetryProvider telemetryProvider
    ) {
        return null;
    }
}
