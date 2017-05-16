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

package org.elasticsearch.legacy.node.internal;

import org.elasticsearch.legacy.Build;
import org.elasticsearch.legacy.ElasticsearchException;
import org.elasticsearch.legacy.Version;
import org.elasticsearch.legacy.action.ActionModule;
import org.elasticsearch.legacy.bulk.udp.BulkUdpModule;
import org.elasticsearch.legacy.bulk.udp.BulkUdpService;
import org.elasticsearch.legacy.cache.recycler.CacheRecycler;
import org.elasticsearch.legacy.cache.recycler.CacheRecyclerModule;
import org.elasticsearch.legacy.cache.recycler.PageCacheRecycler;
import org.elasticsearch.legacy.cache.recycler.PageCacheRecyclerModule;
import org.elasticsearch.legacy.client.Client;
import org.elasticsearch.legacy.client.node.NodeClientModule;
import org.elasticsearch.legacy.cluster.ClusterModule;
import org.elasticsearch.legacy.cluster.ClusterNameModule;
import org.elasticsearch.legacy.cluster.ClusterService;
import org.elasticsearch.legacy.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.legacy.cluster.routing.RoutingService;
import org.elasticsearch.legacy.cluster.routing.allocation.AllocationService;
import org.elasticsearch.legacy.common.StopWatch;
import org.elasticsearch.legacy.common.collect.Tuple;
import org.elasticsearch.legacy.common.component.Lifecycle;
import org.elasticsearch.legacy.common.component.LifecycleComponent;
import org.elasticsearch.legacy.common.compress.CompressorFactory;
import org.elasticsearch.legacy.common.inject.Injector;
import org.elasticsearch.legacy.common.inject.Injectors;
import org.elasticsearch.legacy.common.inject.ModulesBuilder;
import org.elasticsearch.legacy.common.io.CachedStreams;
import org.elasticsearch.legacy.common.lease.Releasables;
import org.elasticsearch.legacy.common.logging.ESLogger;
import org.elasticsearch.legacy.common.logging.Loggers;
import org.elasticsearch.legacy.common.network.NetworkModule;
import org.elasticsearch.legacy.common.settings.ImmutableSettings;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.common.settings.SettingsModule;
import org.elasticsearch.legacy.common.util.BigArraysModule;
import org.elasticsearch.legacy.discovery.Discovery;
import org.elasticsearch.legacy.discovery.DiscoveryModule;
import org.elasticsearch.legacy.discovery.DiscoveryService;
import org.elasticsearch.legacy.env.Environment;
import org.elasticsearch.legacy.env.EnvironmentModule;
import org.elasticsearch.legacy.env.NodeEnvironment;
import org.elasticsearch.legacy.env.NodeEnvironmentModule;
import org.elasticsearch.legacy.gateway.GatewayModule;
import org.elasticsearch.legacy.gateway.GatewayService;
import org.elasticsearch.legacy.http.HttpServer;
import org.elasticsearch.legacy.http.HttpServerModule;
import org.elasticsearch.legacy.index.search.shape.ShapeModule;
import org.elasticsearch.legacy.indices.IndicesModule;
import org.elasticsearch.legacy.indices.IndicesService;
import org.elasticsearch.legacy.indices.cache.filter.IndicesFilterCache;
import org.elasticsearch.legacy.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.legacy.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.legacy.indices.memory.IndexingMemoryController;
import org.elasticsearch.legacy.indices.ttl.IndicesTTLService;
import org.elasticsearch.legacy.monitor.MonitorModule;
import org.elasticsearch.legacy.monitor.MonitorService;
import org.elasticsearch.legacy.monitor.jvm.JvmInfo;
import org.elasticsearch.legacy.node.Node;
import org.elasticsearch.legacy.percolator.PercolatorModule;
import org.elasticsearch.legacy.percolator.PercolatorService;
import org.elasticsearch.legacy.plugins.PluginsModule;
import org.elasticsearch.legacy.plugins.PluginsService;
import org.elasticsearch.legacy.repositories.RepositoriesModule;
import org.elasticsearch.legacy.rest.RestController;
import org.elasticsearch.legacy.rest.RestModule;
import org.elasticsearch.legacy.river.RiversManager;
import org.elasticsearch.legacy.river.RiversModule;
import org.elasticsearch.legacy.script.ScriptModule;
import org.elasticsearch.legacy.script.ScriptService;
import org.elasticsearch.legacy.search.SearchModule;
import org.elasticsearch.legacy.search.SearchService;
import org.elasticsearch.legacy.snapshots.SnapshotsService;
import org.elasticsearch.legacy.threadpool.ThreadPool;
import org.elasticsearch.legacy.threadpool.ThreadPoolModule;
import org.elasticsearch.legacy.transport.TransportModule;
import org.elasticsearch.legacy.transport.TransportService;
import org.elasticsearch.legacy.tribe.TribeModule;
import org.elasticsearch.legacy.tribe.TribeService;
import org.elasticsearch.legacy.watcher.ResourceWatcherModule;
import org.elasticsearch.legacy.watcher.ResourceWatcherService;

import java.util.Arrays;
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

    public InternalNode() throws ElasticsearchException {
        this(ImmutableSettings.Builder.EMPTY_SETTINGS, true);
    }

    public InternalNode(Settings pSettings, boolean loadConfigSettings) throws ElasticsearchException {
        Tuple<Settings, Environment> tuple = InternalSettingsPreparer.prepareSettings(pSettings, loadConfigSettings);
        tuple = new Tuple<>(TribeService.processSettings(tuple.v1()), tuple.v2());

        // The only place we can actually fake the version a node is running on:
        Version version = pSettings.getAsVersion("tests.mock.version", Version.CURRENT);

        ESLogger logger = Loggers.getLogger(Node.class, tuple.v1().get("name"));
        logger.info("version[{}], pid[{}], build[{}/{}]", version, JvmInfo.jvmInfo().pid(), Build.CURRENT.hashShort(), Build.CURRENT.timestamp());

        logger.info("initializing ...");

        if (logger.isDebugEnabled()) {
            Environment env = tuple.v2();
            logger.debug("using home [{}], config [{}], data [{}], logs [{}], work [{}], plugins [{}]",
                    env.homeFile(), env.configFile(), Arrays.toString(env.dataFiles()), env.logsFile(),
                    env.workFile(), env.pluginsFile());
        }

        this.pluginsService = new PluginsService(tuple.v1(), tuple.v2());
        this.settings = pluginsService.updatedSettings();
        // create the environment based on the finalized (processed) view of the settings
        this.environment = new Environment(this.settings());

        CompressorFactory.configure(settings);

        NodeEnvironment nodeEnvironment = new NodeEnvironment(this.settings, this.environment);

        boolean success = false;
        try {
            ModulesBuilder modules = new ModulesBuilder();
            modules.add(new Version.Module(version));
            modules.add(new CacheRecyclerModule(settings));
            modules.add(new PageCacheRecyclerModule(settings));
            modules.add(new BigArraysModule(settings));
            modules.add(new PluginsModule(settings, pluginsService));
            modules.add(new SettingsModule(settings));
            modules.add(new NodeModule(this));
            modules.add(new NetworkModule());
            modules.add(new ScriptModule(settings));
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
            modules.add(new BulkUdpModule());
            modules.add(new ShapeModule());
            modules.add(new PercolatorModule());
            modules.add(new ResourceWatcherModule());
            modules.add(new RepositoriesModule());
            modules.add(new TribeModule());

            injector = modules.createInjector();

            client = injector.getInstance(Client.class);
            success = true;
        } finally {
            if (!success) {
                nodeEnvironment.close();
            }
        }

        logger.info("initialized");
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
        logger.info("starting ...");

        // hack around dependency injection problem (for now...)
        injector.getInstance(Discovery.class).setAllocationService(injector.getInstance(AllocationService.class));

        for (Class<? extends LifecycleComponent> plugin : pluginsService.services()) {
            injector.getInstance(plugin).start();
        }

        injector.getInstance(MappingUpdatedAction.class).start();
        injector.getInstance(IndicesService.class).start();
        injector.getInstance(IndexingMemoryController.class).start();
        injector.getInstance(IndicesClusterStateService.class).start();
        injector.getInstance(IndicesTTLService.class).start();
        injector.getInstance(RiversManager.class).start();
        injector.getInstance(SnapshotsService.class).start();
        injector.getInstance(ClusterService.class).start();
        injector.getInstance(RoutingService.class).start();
        injector.getInstance(SearchService.class).start();
        injector.getInstance(MonitorService.class).start();
        injector.getInstance(RestController.class).start();
        injector.getInstance(TransportService.class).start();
        DiscoveryService discoService = injector.getInstance(DiscoveryService.class).start();
        discoService.waitForInitialState();

        // gateway should start after disco, so it can try and recovery from gateway on "start"
        injector.getInstance(GatewayService.class).start();

        if (settings.getAsBoolean("http.enabled", true)) {
            injector.getInstance(HttpServer.class).start();
        }
        injector.getInstance(BulkUdpService.class).start();
        injector.getInstance(ResourceWatcherService.class).start();
        injector.getInstance(TribeService.class).start();

        logger.info("started");

        return this;
    }

    @Override
    public Node stop() {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        ESLogger logger = Loggers.getLogger(Node.class, settings.get("name"));
        logger.info("stopping ...");

        injector.getInstance(TribeService.class).stop();
        injector.getInstance(BulkUdpService.class).stop();
        injector.getInstance(ResourceWatcherService.class).stop();
        if (settings.getAsBoolean("http.enabled", true)) {
            injector.getInstance(HttpServer.class).stop();
        }

        injector.getInstance(MappingUpdatedAction.class).stop();
        injector.getInstance(RiversManager.class).stop();

        injector.getInstance(SnapshotsService.class).stop();
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

        for (Class<? extends LifecycleComponent> plugin : pluginsService.services()) {
            injector.getInstance(plugin).stop();
        }

        logger.info("stopped");

        return this;
    }

    // During concurrent close() calls we want to make sure that all of them return after the node has completed it's shutdown cycle.
    // If not, the hook that is added in Bootstrap#setup() will be useless: close() might not be executed, in case another (for example api) call
    // to close() has already set some lifecycles to stopped. In this case the process will be terminated even if the first call to close() has not finished yet.
    public synchronized void close() {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }

        ESLogger logger = Loggers.getLogger(Node.class, settings.get("name"));
        logger.info("closing ...");

        StopWatch stopWatch = new StopWatch("node_close");
        stopWatch.start("tribe");
        injector.getInstance(TribeService.class).close();
        stopWatch.stop().start("bulk.udp");
        injector.getInstance(BulkUdpService.class).close();
        stopWatch.stop().start("http");
        if (settings.getAsBoolean("http.enabled", true)) {
            injector.getInstance(HttpServer.class).close();
        }

        stopWatch.stop().start("rivers");
        injector.getInstance(RiversManager.class).close();

        stopWatch.stop().start("snapshot_service");
        injector.getInstance(SnapshotsService.class).close();
        stopWatch.stop().start("client");
        Releasables.close(injector.getInstance(Client.class));
        stopWatch.stop().start("indices_cluster");
        injector.getInstance(IndicesClusterStateService.class).close();
        stopWatch.stop().start("indices");
        injector.getInstance(IndicesFilterCache.class).close();
        injector.getInstance(IndicesFieldDataCache.class).close();
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
        stopWatch.stop().start("percolator_service");
        injector.getInstance(PercolatorService.class).close();

        for (Class<? extends LifecycleComponent> plugin : pluginsService.services()) {
            stopWatch.stop().start("plugin(" + plugin.getName() + ")");
            injector.getInstance(plugin).close();
        }

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

        if (logger.isTraceEnabled()) {
            logger.trace("Close times for each service:\n{}", stopWatch.prettyPrint());
        }

        injector.getInstance(NodeEnvironment.class).close();
        injector.getInstance(CacheRecycler.class).close();
        injector.getInstance(PageCacheRecycler.class).close();
        Injectors.close(injector);

        CachedStreams.clear();

        logger.info("closed");
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