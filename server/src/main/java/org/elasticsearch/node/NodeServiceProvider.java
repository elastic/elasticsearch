/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.node;

import org.elasticsearch.action.search.OnlinePrewarmingService;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.indices.ExecutorSelector;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.PluginsLoader;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.readiness.ReadinessService;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Provides various service implementations to {@link NodeConstruction}
 */
class NodeServiceProvider {

    PluginsService newPluginService(Environment initialEnvironment, PluginsLoader pluginsLoader) {
        // this creates a PluginsService with an empty list of classpath plugins
        return new PluginsService(initialEnvironment.settings(), initialEnvironment.configDir(), pluginsLoader);
    }

    ScriptService newScriptService(
        PluginsService pluginsService,
        Settings settings,
        Map<String, ScriptEngine> engines,
        Map<String, ScriptContext<?>> contexts,
        LongSupplier timeProvider
    ) {
        return new ScriptService(settings, engines, contexts, timeProvider);
    }

    ClusterInfoService newClusterInfoService(
        PluginsService pluginsService,
        Settings settings,
        ClusterService clusterService,
        ThreadPool threadPool,
        NodeClient client
    ) {
        final InternalClusterInfoService service = new InternalClusterInfoService(settings, clusterService, threadPool, client);
        if (DiscoveryNode.isMasterNode(settings)) {
            // listen for state changes (this node starts/stops being the elected master, or new nodes are added)
            clusterService.addListener(service);
        }
        return service;
    }

    PageCacheRecycler newPageCacheRecycler(PluginsService pluginsService, Settings settings) {
        return new PageCacheRecycler(settings);
    }

    BigArrays newBigArrays(
        PluginsService pluginsService,
        PageCacheRecycler pageCacheRecycler,
        CircuitBreakerService circuitBreakerService
    ) {
        return new BigArrays(pageCacheRecycler, circuitBreakerService, CircuitBreaker.REQUEST);
    }

    TransportService newTransportService(
        PluginsService pluginsService,
        Settings settings,
        Transport transport,
        ThreadPool threadPool,
        TransportInterceptor interceptor,
        Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
        ClusterSettings clusterSettings,
        TaskManager taskManager,
        Tracer tracer
    ) {
        return new TransportService(settings, transport, threadPool, interceptor, localNodeFactory, clusterSettings, taskManager, tracer);
    }

    HttpServerTransport newHttpTransport(PluginsService pluginsService, NetworkModule networkModule) {
        return networkModule.getHttpServerTransportSupplier().get();
    }

    SearchService newSearchService(
        PluginsService pluginsService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ScriptService scriptService,
        BigArrays bigArrays,
        FetchPhase fetchPhase,
        CircuitBreakerService circuitBreakerService,
        ExecutorSelector executorSelector,
        Tracer tracer,
        OnlinePrewarmingService onlinePrewarmingService
    ) {
        return new SearchService(
            clusterService,
            indicesService,
            threadPool,
            scriptService,
            bigArrays,
            fetchPhase,
            circuitBreakerService,
            executorSelector,
            tracer,
            onlinePrewarmingService
        );
    }

    void processRecoverySettings(PluginsService pluginsService, ClusterSettings clusterSettings, RecoverySettings recoverySettings) {
        // Noop in production, overridden by tests
    }

    ReadinessService newReadinessService(PluginsService pluginsService, ClusterService clusterService, Environment environment) {
        return new ReadinessService(clusterService, environment);
    }
}
