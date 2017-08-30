/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.node;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.MockInternalClusterInfoService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportService;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A node for testing which allows:
 * <ul>
 *   <li>Overriding Version.CURRENT</li>
 *   <li>Adding test plugins that exist on the classpath</li>
 * </ul>
 */
public class MockNode extends Node {
    private final Collection<Class<? extends Plugin>> classpathPlugins;

    public MockNode(Settings settings, Collection<Class<? extends Plugin>> classpathPlugins) {
        this(settings, classpathPlugins, null);
    }

    public MockNode(Settings settings, Collection<Class<? extends Plugin>> classpathPlugins, Path configPath) {
        this(InternalSettingsPreparer.prepareEnvironment(settings, null, Collections.emptyMap(), configPath), classpathPlugins);
    }

    public MockNode(Environment environment, Collection<Class<? extends Plugin>> classpathPlugins) {
        super(environment, classpathPlugins);
        this.classpathPlugins = classpathPlugins;
    }

    /**
     * The classpath plugins this node was constructed with.
     */
    public Collection<Class<? extends Plugin>> getClasspathPlugins() {
        return classpathPlugins;
    }

    @Override
    protected BigArrays createBigArrays(Settings settings, CircuitBreakerService circuitBreakerService) {
        if (getPluginsService().filterPlugins(NodeMocksPlugin.class).isEmpty()) {
            return super.createBigArrays(settings, circuitBreakerService);
        }
        return new MockBigArrays(settings, circuitBreakerService);
    }


    @Override
    protected SearchService newSearchService(ClusterService clusterService, IndicesService indicesService,
                                             ThreadPool threadPool, ScriptService scriptService, BigArrays bigArrays,
                                             FetchPhase fetchPhase, ResponseCollectorService responseCollectorService) {
        if (getPluginsService().filterPlugins(MockSearchService.TestPlugin.class).isEmpty()) {
            return super.newSearchService(clusterService, indicesService, threadPool, scriptService, bigArrays, fetchPhase,
                responseCollectorService);
        }
        return new MockSearchService(clusterService, indicesService, threadPool, scriptService, bigArrays, fetchPhase);
    }

    @Override
    protected TransportService newTransportService(Settings settings, Transport transport, ThreadPool threadPool,
                                                   TransportInterceptor interceptor,
                                                   Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
                                                   ClusterSettings clusterSettings) {
        // we use the MockTransportService.TestPlugin class as a marker to create a network
        // module with this MockNetworkService. NetworkService is such an integral part of the systme
        // we don't allow to plug it in from plugins or anything. this is a test-only override and
        // can't be done in a production env.
        if (getPluginsService().filterPlugins(MockTransportService.TestPlugin.class).isEmpty()) {
            return super.newTransportService(settings, transport, threadPool, interceptor, localNodeFactory, clusterSettings);
        } else {
            return new MockTransportService(settings, transport, threadPool, interceptor, localNodeFactory, clusterSettings);
        }
    }

    @Override
    protected void processRecoverySettings(ClusterSettings clusterSettings, RecoverySettings recoverySettings) {
        if (false == getPluginsService().filterPlugins(RecoverySettingsChunkSizePlugin.class).isEmpty()) {
            clusterSettings.addSettingsUpdateConsumer(RecoverySettingsChunkSizePlugin.CHUNK_SIZE_SETTING, recoverySettings::setChunkSize);
        }
    }

    @Override
    protected ClusterInfoService newClusterInfoService(Settings settings, ClusterService clusterService,
                                                       ThreadPool threadPool, NodeClient client, Consumer<ClusterInfo> listener) {
        if (getPluginsService().filterPlugins(MockInternalClusterInfoService.TestPlugin.class).isEmpty()) {
            return super.newClusterInfoService(settings, clusterService, threadPool, client, listener);
        } else {
            return new MockInternalClusterInfoService(settings, clusterService, threadPool, client, listener);
        }
    }
}

