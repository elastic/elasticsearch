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

package org.elasticsearch.client.transport;

import org.elasticsearch.Version;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.client.support.Headers;
import org.elasticsearch.client.transport.support.TransportProxyClient;
import org.elasticsearch.cluster.ClusterNameModule;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.EnvironmentModule;
import org.elasticsearch.indices.breaker.CircuitBreakerModule;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsModule;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolModule;
import org.elasticsearch.transport.TransportModule;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty.NettyTransport;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;

/**
 * The transport client allows to create a client that is not part of the cluster, but simply connects to one
 * or more nodes directly by adding their respective addresses using {@link #addTransportAddress(org.elasticsearch.common.transport.TransportAddress)}.
 * <p/>
 * <p>The transport client important modules used is the {@link org.elasticsearch.transport.TransportModule} which is
 * started in client mode (only connects, no bind).
 */
public class TransportClient extends AbstractClient {

    /**
     * Handy method ot create a {@link org.elasticsearch.client.transport.TransportClient.Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder used to create an instance of the transport client.
     */
    public static class Builder {

        private Settings settings = Settings.EMPTY;
        private List<Class<? extends Plugin>> pluginClasses = new ArrayList<>();

        /**
         * The settings to configure the transport client with.
         */
        public Builder settings(Settings.Builder settings) {
            return settings(settings.build());
        }

        /**
         * The settings to configure the transport client with.
         */
        public Builder settings(Settings settings) {
            this.settings = settings;
            return this;
        }

        /**
         * Add the given plugin to the client when it is created.
         */
        public Builder addPlugin(Class<? extends Plugin> pluginClass) {
            pluginClasses.add(pluginClass);
            return this;
        }

        /**
         * Builds a new instance of the transport client.
         */
        public TransportClient build() {
            Settings settings = InternalSettingsPreparer.prepareSettings(this.settings);
            settings = settingsBuilder()
                    .put(NettyTransport.PING_SCHEDULE, "5s") // enable by default the transport schedule ping interval
                    .put(settings)
                    .put("network.server", false)
                    .put("node.client", true)
                    .put(CLIENT_TYPE_SETTING, CLIENT_TYPE)
                    .build();

            PluginsService pluginsService = new PluginsService(settings, null, pluginClasses);
            this.settings = pluginsService.updatedSettings();

            Version version = Version.CURRENT;

            final ThreadPool threadPool = new ThreadPool(settings);

            boolean success = false;
            try {
                ModulesBuilder modules = new ModulesBuilder();
                modules.add(new Version.Module(version));
                // plugin modules must be added here, before others or we can get crazy injection errors...
                for (Module pluginModule : pluginsService.nodeModules()) {
                    modules.add(pluginModule);
                }
                modules.add(new PluginsModule(pluginsService));
                modules.add(new SettingsModule(this.settings));
                modules.add(new NetworkModule());
                modules.add(new ClusterNameModule(this.settings));
                modules.add(new ThreadPoolModule(threadPool));
                modules.add(new TransportModule(this.settings));
                modules.add(new SearchModule(this.settings) {
                    @Override
                    protected void configure() {
                        // noop
                    }
                });
                modules.add(new ActionModule(true));
                modules.add(new ClientTransportModule());
                modules.add(new CircuitBreakerModule(this.settings));

                pluginsService.processModules(modules);

                Injector injector = modules.createInjector();
                injector.getInstance(TransportService.class).start();
                TransportClient transportClient = new TransportClient(injector);
                success = true;
                return transportClient;
            } finally {
                if (!success) {
                    ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
                }
            }
        }
    }

    public static final String CLIENT_TYPE = "transport";

    final Injector injector;

    private final TransportClientNodesService nodesService;
    private final TransportProxyClient proxy;

    private TransportClient(Injector injector) {
        super(injector.getInstance(Settings.class), injector.getInstance(ThreadPool.class), injector.getInstance(Headers.class));
        this.injector = injector;
        nodesService = injector.getInstance(TransportClientNodesService.class);
        proxy = injector.getInstance(TransportProxyClient.class);
    }

    TransportClientNodesService nodeService() {
        return nodesService;
    }

    /**
     * Returns the current registered transport addresses to use (added using
     * {@link #addTransportAddress(org.elasticsearch.common.transport.TransportAddress)}.
     */
    public List<TransportAddress> transportAddresses() {
        return nodesService.transportAddresses();
    }

    /**
     * Returns the current connected transport nodes that this client will use.
     * <p/>
     * <p>The nodes include all the nodes that are currently alive based on the transport
     * addresses provided.
     */
    public List<DiscoveryNode> connectedNodes() {
        return nodesService.connectedNodes();
    }

    /**
     * The list of filtered nodes that were not connected to, for example, due to
     * mismatch in cluster name.
     */
    public List<DiscoveryNode> filteredNodes() {
        return nodesService.filteredNodes();
    }

    /**
     * Returns the listed nodes in the transport client (ones added to it).
     */
    public List<DiscoveryNode> listedNodes() {
        return nodesService.listedNodes();
    }

    /**
     * Adds a transport address that will be used to connect to.
     * <p/>
     * <p>The Node this transport address represents will be used if its possible to connect to it.
     * If it is unavailable, it will be automatically connected to once it is up.
     * <p/>
     * <p>In order to get the list of all the current connected nodes, please see {@link #connectedNodes()}.
     */
    public TransportClient addTransportAddress(TransportAddress transportAddress) {
        nodesService.addTransportAddresses(transportAddress);
        return this;
    }

    /**
     * Adds a list of transport addresses that will be used to connect to.
     * <p/>
     * <p>The Node this transport address represents will be used if its possible to connect to it.
     * If it is unavailable, it will be automatically connected to once it is up.
     * <p/>
     * <p>In order to get the list of all the current connected nodes, please see {@link #connectedNodes()}.
     */
    public TransportClient addTransportAddresses(TransportAddress... transportAddress) {
        nodesService.addTransportAddresses(transportAddress);
        return this;
    }

    /**
     * Removes a transport address from the list of transport addresses that are used to connect to.
     */
    public TransportClient removeTransportAddress(TransportAddress transportAddress) {
        nodesService.removeTransportAddress(transportAddress);
        return this;
    }

    /**
     * Closes the client.
     */
    @Override
    public void close() {
        injector.getInstance(TransportClientNodesService.class).close();
        injector.getInstance(TransportService.class).close();
        try {
            injector.getInstance(MonitorService.class).close();
        } catch (Exception e) {
            // ignore, might not be bounded
        }

        for (Class<? extends LifecycleComponent> plugin : injector.getInstance(PluginsService.class).nodeServices()) {
            injector.getInstance(plugin).close();
        }
        try {
            ThreadPool.terminate(injector.getInstance(ThreadPool.class), 10, TimeUnit.SECONDS);
        } catch (Exception e) {
            // ignore
        }

        injector.getInstance(PageCacheRecycler.class).close();
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
        proxy.execute(action, request, listener);
    }
}
