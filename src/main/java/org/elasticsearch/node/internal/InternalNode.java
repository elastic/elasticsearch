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

package org.elasticsearch.node.internal;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.bench.BenchmarkModule;
import org.elasticsearch.bulk.udp.BulkUdpModule;
import org.elasticsearch.bulk.udp.BulkUdpService;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cache.recycler.CacheRecyclerModule;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecyclerModule;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClientModule;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterNameModule;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.routing.RoutingService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Injectors;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.io.CachedStreams;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.util.BigArraysModule;
import org.elasticsearch.discovery.Discovery;
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
import org.elasticsearch.index.search.shape.ShapeModule;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.cache.filter.IndicesFilterCache;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.memory.IndexingMemoryController;
import org.elasticsearch.indices.ttl.IndicesTTLService;
import org.elasticsearch.monitor.MonitorModule;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.node.Node;
import org.elasticsearch.percolator.PercolatorModule;
import org.elasticsearch.percolator.PercolatorService;
import org.elasticsearch.plugins.PluginsModule;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.RepositoriesModule;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.river.RiversManager;
import org.elasticsearch.river.RiversModule;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolModule;
import org.elasticsearch.transport.TransportModule;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.tribe.TribeModule;
import org.elasticsearch.tribe.TribeService;
import org.elasticsearch.watcher.ResourceWatcherModule;
import org.elasticsearch.watcher.ResourceWatcherService;

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
        modules.add(new BenchmarkModule(settings));

        injector = modules.createInjector();

        client = injector.getInstance(Client.class);

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

    public void close() {
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