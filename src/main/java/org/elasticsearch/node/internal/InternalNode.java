/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.node.internal;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.cache.NodeCache;
import org.elasticsearch.cache.NodeCacheModule;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClientModule;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterNameModule;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.RoutingService;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Injectors;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.io.CachedStreams;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.util.concurrent.ThreadLocals;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.DiscoveryService;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.EnvironmentModule;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.NodeEnvironmentModule;
import org.elasticsearch.gateway.GatewayModule;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.http.HttpServer;
import org.elasticsearch.http.HttpServerModule;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.cache.filter.IndicesFilterCache;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.memory.IndexingMemoryController;
import org.elasticsearch.indices.ttl.IndicesTTLService;
import org.elasticsearch.jmx.JmxModule;
import org.elasticsearch.jmx.JmxService;
import org.elasticsearch.monitor.MonitorModule;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.PluginsModule;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.river.RiversManager;
import org.elasticsearch.river.RiversModule;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolModule;
import org.elasticsearch.transport.TransportModule;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public final class InternalNode implements Node {

    private final Lifecycle lifecycle = new Lifecycle();

    private final Injector injector;

    private final Settings settings;

    private final Environment environment;

    private final PluginsService pluginsService;

    private final Client client;

    public InternalNode() throws ElasticSearchException {
        this(ImmutableSettings.Builder.EMPTY_SETTINGS, true);
    }

    public InternalNode(Settings pSettings, boolean loadConfigSettings) throws ElasticSearchException {
        Tuple<Settings, Environment> tuple = InternalSettingsPerparer.prepareSettings(pSettings, loadConfigSettings);

        ESLogger logger = Loggers.getLogger(Node.class, tuple.v1().get("name"));
        logger.info("{{}}[{}]: initializing ...", Version.CURRENT, JvmInfo.jvmInfo().pid());

        this.pluginsService = new PluginsService(tuple.v1(), tuple.v2());
        this.settings = pluginsService.updatedSettings();
        this.environment = tuple.v2();

        CompressorFactory.configure(settings);

        NodeEnvironment nodeEnvironment = new NodeEnvironment(this.settings, this.environment);

        ModulesBuilder modules = new ModulesBuilder();
        modules.add(new PluginsModule(settings, pluginsService));
        modules.add(new SettingsModule(settings));
        modules.add(new NodeModule(this));
        modules.add(new NetworkModule());
        modules.add(new NodeCacheModule(settings));
        modules.add(new ScriptModule(settings));
        modules.add(new JmxModule(settings));
        modules.add(new EnvironmentModule(environment));
        modules.add(new NodeEnvironmentModule(nodeEnvironment));
        modules.add(new ClusterNameModule(settings));
        modules.add(new ThreadPoolModule(settings));
        modules.add(new DiscoveryModule(settings));
        modules.add(new ClusterModule(settings));
        modules.add(new RestModule(settings));
        modules.add(new TransportModule(settings));
        if (settings.getAsBoolean("http.enabled", true)) {
            modules.add(new HttpServerModule(settings));
        }
        modules.add(new RiversModule(settings));
        modules.add(new IndicesModule(settings));
        modules.add(new SearchModule());
        modules.add(new ActionModule(false));
        modules.add(new MonitorModule(settings));
        modules.add(new GatewayModule(settings));
        modules.add(new NodeClientModule());

        injector = modules.createInjector();

        client = injector.getInstance(Client.class);

        logger.info("{{}}[{}]: initialized", Version.CURRENT, JvmInfo.jvmInfo().pid());
    }

    @Override
    public Settings settings() {
        return this.settings;
    }

    @Override
    public Client client() {
        return client;
    }

    public Node start() {
        if (!lifecycle.moveToStarted()) {
            return this;
        }

        ESLogger logger = Loggers.getLogger(Node.class, settings.get("name"));
        logger.info("{{}}[{}]: starting ...", Version.CURRENT, JvmInfo.jvmInfo().pid());

        for (Class<? extends LifecycleComponent> plugin : pluginsService.services()) {
            injector.getInstance(plugin).start();
        }

        injector.getInstance(IndicesService.class).start();
        injector.getInstance(IndexingMemoryController.class).start();
        injector.getInstance(IndicesClusterStateService.class).start();
        injector.getInstance(IndicesTTLService.class).start();
        injector.getInstance(RiversManager.class).start();
        injector.getInstance(ClusterService.class).start();
        injector.getInstance(RoutingService.class).start();
        injector.getInstance(SearchService.class).start();
        injector.getInstance(MonitorService.class).start();
        injector.getInstance(RestController.class).start();
        injector.getInstance(TransportService.class).start();
        DiscoveryService discoService = injector.getInstance(DiscoveryService.class).start();

        // gateway should start after disco, so it can try and recovery from gateway on "start"
        injector.getInstance(GatewayService.class).start();

        if (settings.getAsBoolean("http.enabled", true)) {
            injector.getInstance(HttpServer.class).start();
        }
        injector.getInstance(JmxService.class).connectAndRegister(discoService.nodeDescription(), injector.getInstance(NetworkService.class));

        logger.info("{{}}[{}]: started", Version.CURRENT, JvmInfo.jvmInfo().pid());

        return this;
    }

    @Override
    public Node stop() {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        ESLogger logger = Loggers.getLogger(Node.class, settings.get("name"));
        logger.info("{{}}[{}]: stopping ...", Version.CURRENT, JvmInfo.jvmInfo().pid());

        if (settings.getAsBoolean("http.enabled", true)) {
            injector.getInstance(HttpServer.class).stop();
        }

        injector.getInstance(RiversManager.class).stop();

        // stop any changes happening as a result of cluster state changes
        injector.getInstance(IndicesClusterStateService.class).stop();
        // we close indices first, so operations won't be allowed on it
        injector.getInstance(IndexingMemoryController.class).stop();
        injector.getInstance(IndicesTTLService.class).stop();
        injector.getInstance(IndicesService.class).stop();
        // sleep a bit to let operations finish with indices service
//        try {
//            Thread.sleep(500);
//        } catch (InterruptedException e) {
//            // ignore
//        }
        injector.getInstance(RoutingService.class).stop();
        injector.getInstance(ClusterService.class).stop();
        injector.getInstance(DiscoveryService.class).stop();
        injector.getInstance(MonitorService.class).stop();
        injector.getInstance(GatewayService.class).stop();
        injector.getInstance(SearchService.class).stop();
        injector.getInstance(RestController.class).stop();
        injector.getInstance(TransportService.class).stop();
        injector.getInstance(JmxService.class).close();

        for (Class<? extends LifecycleComponent> plugin : pluginsService.services()) {
            injector.getInstance(plugin).stop();
        }

        logger.info("{{}}[{}]: stopped", Version.CURRENT, JvmInfo.jvmInfo().pid());

        return this;
    }

    public void close() {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }

        ESLogger logger = Loggers.getLogger(Node.class, settings.get("name"));
        logger.info("{{}}[{}]: closing ...", Version.CURRENT, JvmInfo.jvmInfo().pid());

        StopWatch stopWatch = new StopWatch("node_close");
        stopWatch.start("http");
        if (settings.getAsBoolean("http.enabled", true)) {
            injector.getInstance(HttpServer.class).close();
        }

        stopWatch.stop().start("rivers");
        injector.getInstance(RiversManager.class).close();

        stopWatch.stop().start("client");
        injector.getInstance(Client.class).close();
        stopWatch.stop().start("indices_cluster");
        injector.getInstance(IndicesClusterStateService.class).close();
        stopWatch.stop().start("indices");
        injector.getInstance(IndicesFilterCache.class).close();
        injector.getInstance(IndexingMemoryController.class).close();
        injector.getInstance(IndicesTTLService.class).close();
        injector.getInstance(IndicesService.class).close();
        stopWatch.stop().start("routing");
        injector.getInstance(RoutingService.class).close();
        stopWatch.stop().start("cluster");
        injector.getInstance(ClusterService.class).close();
        stopWatch.stop().start("discovery");
        injector.getInstance(DiscoveryService.class).close();
        stopWatch.stop().start("monitor");
        injector.getInstance(MonitorService.class).close();
        stopWatch.stop().start("gateway");
        injector.getInstance(GatewayService.class).close();
        stopWatch.stop().start("search");
        injector.getInstance(SearchService.class).close();
        stopWatch.stop().start("rest");
        injector.getInstance(RestController.class).close();
        stopWatch.stop().start("transport");
        injector.getInstance(TransportService.class).close();

        for (Class<? extends LifecycleComponent> plugin : pluginsService.services()) {
            stopWatch.stop().start("plugin(" + plugin.getName() + ")");
            injector.getInstance(plugin).close();
        }

        stopWatch.stop().start("node_cache");
        injector.getInstance(NodeCache.class).close();

        stopWatch.stop().start("script");
        injector.getInstance(ScriptService.class).close();

        stopWatch.stop().start("thread_pool");
        injector.getInstance(ThreadPool.class).shutdown();
        try {
            injector.getInstance(ThreadPool.class).awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ignore
        }
        stopWatch.stop().start("thread_pool_force_shutdown");
        try {
            injector.getInstance(ThreadPool.class).shutdownNow();
        } catch (Exception e) {
            // ignore
        }
        stopWatch.stop();

        CacheRecycler.clear();
        CachedStreams.clear();
        ThreadLocals.clearReferencesThreadLocals();

        if (logger.isTraceEnabled()) {
            logger.trace("Close times for each service:\n{}", stopWatch.prettyPrint());
        }

        injector.getInstance(NodeEnvironment.class).close();
        Injectors.close(injector);

        logger.info("{{}}[{}]: closed", Version.CURRENT, JvmInfo.jvmInfo().pid());
    }

    @Override
    public boolean isClosed() {
        return lifecycle.closed();
    }

    public Injector injector() {
        return this.injector;
    }

    public static void main(String[] args) throws Exception {
        final InternalNode node = new InternalNode();
        node.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                node.close();
            }
        });
    }
}