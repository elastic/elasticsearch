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

import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionModule;
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
import org.elasticsearch.common.inject.ModulesBuilder;
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
import org.elasticsearch.indices.breaker.CircuitBreakerModule;
import org.elasticsearch.indices.cache.filter.IndicesFilterCache;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.memory.IndexingMemoryController;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.indices.ttl.IndicesTTLService;
import org.elasticsearch.monitor.MonitorModule;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.node.internal.NodeModule;
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

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

/**
 * A node represent a node within a cluster (<tt>cluster.name</tt>). The {@link #client()} can be used
 * in order to use a {@link Client} to perform actions/operations against the cluster.
 * <p/>
 * <p>In order to create a node, the {@link NodeBuilder} can be used. When done with it, make sure to
 * call {@link #close()} on it.
 */
public class Node implements Releasable {

    private static final String CLIENT_TYPE = "node";
    public static final String HTTP_ENABLED = "http.enabled";
    private final Lifecycle lifecycle = new Lifecycle();
    private final Injector injector;
    private final Settings settings;
    private final Environment environment;
    private final PluginsService pluginsService;
    private final Client client;

    public Node() throws ElasticsearchException {
        this(ImmutableSettings.Builder.EMPTY_SETTINGS, true);
    }

    public Node(Settings preparedSettings, boolean loadConfigSettings) throws ElasticsearchException {
        final Settings pSettings = settingsBuilder().put(preparedSettings)
                .put(Client.CLIENT_TYPE_SETTING, CLIENT_TYPE).build();
        Tuple<Settings, Environment> tuple = InternalSettingsPreparer.prepareSettings(pSettings, loadConfigSettings);
        tuple = new Tuple<>(TribeService.processSettings(tuple.v1()), tuple.v2());

        // The only place we can actually fake the version a node is running on:
        Version version = pSettings.getAsVersion("tests.mock.version", Version.CURRENT);

        ESLogger logger = Loggers.getLogger(Node.class, tuple.v1().get("name"));
        logger.info("version[{}], pid[{}], build[{}/{}]", version, JvmInfo.jvmInfo().pid(), Build.CURRENT.hashShort(), Build.CURRENT.timestamp());

        logger.info("initializing ...");

        if (logger.isDebugEnabled()) {
            Environment env = tuple.v2();
            logger.debug("using home [{}], config [{}], data [{}], logs [{}], plugins [{}]",
                    env.homeFile(), env.configFile(), Arrays.toString(env.dataFiles()), env.logsFile(), env.pluginsFile());
        }

        this.pluginsService = new PluginsService(tuple.v1(), tuple.v2());
        this.settings = pluginsService.updatedSettings();
        // create the environment based on the finalized (processed) view of the settings
        this.environment = new Environment(this.settings());

        CompressorFactory.configure(settings);
        final NodeEnvironment nodeEnvironment;
        try {
            nodeEnvironment = new NodeEnvironment(this.settings, this.environment);
        } catch (IOException ex) {
            throw new ElasticsearchIllegalStateException("Failed to created node environment", ex);
        }

        boolean success = false;
        try {
            ModulesBuilder modules = new ModulesBuilder();
            modules.add(new Version.Module(version));
            modules.add(new PageCacheRecyclerModule(settings));
            modules.add(new CircuitBreakerModule(settings));
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
            if (settings.getAsBoolean(HTTP_ENABLED, true)) {
                modules.add(new HttpServerModule(settings));
            }
            modules.add(new RiversModule(settings));
            modules.add(new IndicesModule(settings));
            modules.add(new SearchModule(settings));
            modules.add(new ActionModule(false));
            modules.add(new MonitorModule(settings));
            modules.add(new GatewayModule());
            modules.add(new NodeClientModule());
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

    /**
     * The settings that were used to create the node.
     */
    public Settings settings() {
        return this.settings;
    }

    /**
     * A client that can be used to execute actions (operations) against the cluster.
     */
    public Client client() {
        return client;
    }

    /**
     * Start the node. If the node is already started, this method is no-op.
     */
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

        injector.getInstance(MappingUpdatedAction.class).setClient(client);
        injector.getInstance(IndicesService.class).start();
        injector.getInstance(IndexingMemoryController.class).start();
        injector.getInstance(IndicesClusterStateService.class).start();
        injector.getInstance(IndicesTTLService.class).start();
        injector.getInstance(RiversManager.class).start();
        injector.getInstance(SnapshotsService.class).start();
        injector.getInstance(TransportService.class).start();
        injector.getInstance(ClusterService.class).start();
        injector.getInstance(RoutingService.class).start();
        injector.getInstance(SearchService.class).start();
        injector.getInstance(MonitorService.class).start();
        injector.getInstance(RestController.class).start();
        DiscoveryService discoService = injector.getInstance(DiscoveryService.class).start();
        discoService.waitForInitialState();

        // gateway should start after disco, so it can try and recovery from gateway on "start"
        injector.getInstance(GatewayService.class).start();

        if (settings.getAsBoolean("http.enabled", true)) {
            injector.getInstance(HttpServer.class).start();
        }
        injector.getInstance(ResourceWatcherService.class).start();
        injector.getInstance(TribeService.class).start();

        logger.info("started");

        return this;
    }

    private Node stop() {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        ESLogger logger = Loggers.getLogger(Node.class, settings.get("name"));
        logger.info("stopping ...");

        injector.getInstance(TribeService.class).stop();
        injector.getInstance(ResourceWatcherService.class).stop();
        if (settings.getAsBoolean("http.enabled", true)) {
            injector.getInstance(HttpServer.class).stop();
        }

        injector.getInstance(RiversManager.class).stop();

        injector.getInstance(SnapshotsService.class).stop();
        // stop any changes happening as a result of cluster state changes
        injector.getInstance(IndicesClusterStateService.class).stop();
        // we close indices first, so operations won't be allowed on it
        injector.getInstance(IndexingMemoryController.class).stop();
        injector.getInstance(IndicesTTLService.class).stop();
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
        // we should stop this last since it waits for resources to get released
        // if we had scroll searchers etc or recovery going on we wait for to finish.
        injector.getInstance(IndicesService.class).stop();
        logger.info("stopped");

        return this;
    }

    // During concurrent close() calls we want to make sure that all of them return after the node has completed it's shutdown cycle.
    // If not, the hook that is added in Bootstrap#setup() will be useless: close() might not be executed, in case another (for example api) call
    // to close() has already set some lifecycles to stopped. In this case the process will be terminated even if the first call to close() has not finished yet.
    @Override
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
        injector.getInstance(IndicesStore.class).close();
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
        try {
            injector.getInstance(ScriptService.class).close();
        } catch(IOException e) {
            logger.warn("ScriptService close failed", e);
        }

        stopWatch.stop().start("thread_pool");
        // TODO this should really use ThreadPool.terminate()
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
        injector.getInstance(PageCacheRecycler.class).close();

        logger.info("closed");
    }


    /**
     * Returns <tt>true</tt> if the node is closed.
     */
    public boolean isClosed() {
        return lifecycle.closed();
    }

    public Injector injector() {
        return this.injector;
    }

    public static void main(String[] args) throws Exception {
        final Node node = new Node();
        node.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                node.close();
            }
        });
    }
}
