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
import org.elasticsearch.cluster.MockInternalClusterInfoService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.indices.ExecutorSelector;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.MockPluginsService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsLoader;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.readiness.MockReadinessService;
import org.elasticsearch.readiness.ReadinessService;
import org.elasticsearch.script.MockScriptService;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockHttpTransport;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportSettings;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * A node for testing which allows:
 * <ul>
 *   <li>Overriding Version.CURRENT</li>
 *   <li>Adding test plugins that exist on the classpath</li>
 *   <li>Swapping in various mock services</li>
 * </ul>
 */
public class MockNode extends Node {

    private static class MockServiceProvider extends NodeServiceProvider {
        @Override
        BigArrays newBigArrays(
            PluginsService pluginsService,
            PageCacheRecycler pageCacheRecycler,
            CircuitBreakerService circuitBreakerService
        ) {
            if (pluginsService.filterPlugins(NodeMocksPlugin.class).findAny().isEmpty()) {
                return super.newBigArrays(pluginsService, pageCacheRecycler, circuitBreakerService);
            }
            return new MockBigArrays(pageCacheRecycler, circuitBreakerService);
        }

        @Override
        PageCacheRecycler newPageCacheRecycler(PluginsService pluginsService, Settings settings) {
            if (pluginsService.filterPlugins(NodeMocksPlugin.class).findAny().isEmpty()) {
                return super.newPageCacheRecycler(pluginsService, settings);
            }
            return new MockPageCacheRecycler(settings);
        }

        @Override
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
            if (pluginsService.filterPlugins(MockSearchService.TestPlugin.class).findAny().isEmpty()) {
                return super.newSearchService(
                    pluginsService,
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

            return new MockSearchService(
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

        @Override
        ScriptService newScriptService(
            PluginsService pluginsService,
            Settings settings,
            Map<String, ScriptEngine> engines,
            Map<String, ScriptContext<?>> contexts,
            LongSupplier timeProvider
        ) {
            if (pluginsService.filterPlugins(MockScriptService.TestPlugin.class).findAny().isEmpty()) {
                return super.newScriptService(pluginsService, settings, engines, contexts, timeProvider);
            }
            return new MockScriptService(settings, engines, contexts);
        }

        @Override
        ReadinessService newReadinessService(PluginsService pluginsService, ClusterService clusterService, Environment environment) {
            if (pluginsService.filterPlugins(MockReadinessService.TestPlugin.class).findAny().isEmpty()) {
                return super.newReadinessService(pluginsService, clusterService, environment);
            }
            return new MockReadinessService(clusterService, environment);
        }

        @Override
        protected TransportService newTransportService(
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
            // we use the MockTransportService.TestPlugin class as a marker to create a network
            // module with this MockNetworkService. NetworkService is such an integral part of the systme
            // we don't allow to plug it in from plugins or anything. this is a test-only override and
            // can't be done in a production env.
            if (pluginsService.filterPlugins(MockTransportService.TestPlugin.class).findAny().isEmpty()) {
                return super.newTransportService(
                    pluginsService,
                    settings,
                    transport,
                    threadPool,
                    interceptor,
                    localNodeFactory,
                    clusterSettings,
                    taskManager,
                    tracer
                );
            } else {
                return new MockTransportService(
                    settings,
                    transport,
                    threadPool,
                    interceptor,
                    localNodeFactory,
                    clusterSettings,
                    taskManager.getTaskHeaders()
                );
            }
        }

        @Override
        protected ClusterInfoService newClusterInfoService(
            PluginsService pluginsService,
            Settings settings,
            ClusterService clusterService,
            ThreadPool threadPool,
            NodeClient client
        ) {
            if (pluginsService.filterPlugins(MockInternalClusterInfoService.TestPlugin.class).findAny().isEmpty()) {
                return super.newClusterInfoService(pluginsService, settings, clusterService, threadPool, client);
            } else {
                final MockInternalClusterInfoService service = new MockInternalClusterInfoService(
                    settings,
                    clusterService,
                    threadPool,
                    client
                );
                clusterService.addListener(service);
                return service;
            }
        }

        @Override
        protected HttpServerTransport newHttpTransport(PluginsService pluginsService, NetworkModule networkModule) {
            if (pluginsService.filterPlugins(MockHttpTransport.TestPlugin.class).findAny().isEmpty()) {
                return super.newHttpTransport(pluginsService, networkModule);
            } else {
                return new MockHttpTransport();
            }
        }
    }

    private final Collection<Class<? extends Plugin>> classpathPlugins;

    public MockNode(final Settings settings, final Collection<Class<? extends Plugin>> classpathPlugins) {
        this(settings, classpathPlugins, true);
    }

    public MockNode(
        final Settings settings,
        final Collection<Class<? extends Plugin>> classpathPlugins,
        final boolean forbidPrivateIndexSettings
    ) {
        this(settings, classpathPlugins, null, forbidPrivateIndexSettings);
    }

    public MockNode(
        final Settings settings,
        final Collection<Class<? extends Plugin>> classpathPlugins,
        final Path configPath,
        final boolean forbidPrivateIndexSettings
    ) {
        this(
            InternalSettingsPreparer.prepareEnvironment(
                Settings.builder().put(TransportSettings.PORT.getKey(), ESTestCase.getPortRange()).put(settings).build(),
                Collections.emptyMap(),
                configPath,
                () -> "mock_ node"
            ),
            classpathPlugins,
            forbidPrivateIndexSettings
        );
    }

    private MockNode(
        final Environment environment,
        final Collection<Class<? extends Plugin>> classpathPlugins,
        final boolean forbidPrivateIndexSettings
    ) {
        super(NodeConstruction.prepareConstruction(environment, null, new MockServiceProvider() {

            @Override
            PluginsService newPluginService(Environment environment, PluginsLoader pluginsLoader) {
                return new MockPluginsService(environment.settings(), environment, classpathPlugins);
            }
        }, forbidPrivateIndexSettings));

        this.classpathPlugins = classpathPlugins;
    }

    /**
     * The classpath plugins this node was constructed with.
     */
    public Collection<Class<? extends Plugin>> getClasspathPlugins() {
        return classpathPlugins;
    }

    @Override
    protected void configureNodeAndClusterIdStateListener(ClusterService clusterService) {
        // do not configure this in tests as this is causing SetOnce to throw exceptions when jvm is used for multiple tests
    }

    public NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }
}
