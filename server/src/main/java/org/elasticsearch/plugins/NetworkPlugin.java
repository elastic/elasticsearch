/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.plugins;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpPreRequest;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Plugin for extending network and transport related classes
 */
public interface NetworkPlugin {

    /**
     * Returns a list of {@link TransportInterceptor} instances that are used to intercept incoming and outgoing
     * transport (inter-node) requests. This must not return <code>null</code>
     *
     * @param namedWriteableRegistry registry of all named writeables registered
     * @param threadContext a {@link ThreadContext} of the current nodes or clients {@link ThreadPool} that can be used to set additional
     *                      headers in the interceptors
     */
    default List<TransportInterceptor> getTransportInterceptors(
        NamedWriteableRegistry namedWriteableRegistry,
        ThreadContext threadContext
    ) {
        return Collections.emptyList();
    }

    /**
     * Returns a map of {@link Transport} suppliers.
     * See {@link org.elasticsearch.common.network.NetworkModule#TRANSPORT_TYPE_KEY} to configure a specific implementation.
     */
    default Map<String, Supplier<Transport>> getTransports(
        Settings settings,
        ThreadPool threadPool,
        PageCacheRecycler pageCacheRecycler,
        CircuitBreakerService circuitBreakerService,
        NamedWriteableRegistry namedWriteableRegistry,
        NetworkService networkService
    ) {
        return Collections.emptyMap();
    }

    /**
     * Returns a map of {@link HttpServerTransport} suppliers.
     * See {@link org.elasticsearch.common.network.NetworkModule#HTTP_TYPE_SETTING} to configure a specific implementation.
     */
    default Map<String, Supplier<HttpServerTransport>> getHttpTransports(
        Settings settings,
        ThreadPool threadPool,
        BigArrays bigArrays,
        PageCacheRecycler pageCacheRecycler,
        CircuitBreakerService circuitBreakerService,
        NamedXContentRegistry xContentRegistry,
        NetworkService networkService,
        HttpServerTransport.Dispatcher dispatcher,
        BiConsumer<HttpPreRequest, ThreadContext> perRequestThreadContext,
        ClusterSettings clusterSettings
    ) {
        return Collections.emptyMap();
    }
}
