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

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.client.transport.support.TransportProxyClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TransportService;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * The transport client allows to create a client that is not part of the cluster, but simply connects to one
 * or more nodes directly by adding their respective addresses using {@link #addTransportAddress(org.elasticsearch.common.transport.TransportAddress)}.
 * <p>
 * The transport client important modules used is the {@link org.elasticsearch.common.network.NetworkModule} which is
 * started in client mode (only connects, no bind).
 */
public abstract class TransportClient extends AbstractClient {

    private static PluginsService newPluginService(final Settings settings, Collection<Class<? extends Plugin>> plugins) {
        final Settings.Builder settingsBuilder = Settings.builder()
                .put(TcpTransport.PING_SCHEDULE.getKey(), "5s") // enable by default the transport schedule ping interval
                .put(InternalSettingsPreparer.prepareSettings(settings))
                .put(NetworkService.NETWORK_SERVER.getKey(), false)
                .put(CLIENT_TYPE_SETTING_S.getKey(), CLIENT_TYPE);
        return new PluginsService(settingsBuilder.build(), null, null, plugins);
    }

    protected static Collection<Class<? extends Plugin>> addPlugins(Collection<Class<? extends Plugin>> collection,
                                                                    Class<? extends Plugin>... plugins) {
        return addPlugins(collection, Arrays.asList(plugins));
    }

    protected static Collection<Class<? extends Plugin>> addPlugins(Collection<Class<? extends Plugin>> collection,
            Collection<Class<? extends Plugin>> plugins) {
        ArrayList<Class<? extends Plugin>> list = new ArrayList<>(collection);
        for (Class<? extends Plugin> p : plugins) {
            if (list.contains(p)) {
                throw new IllegalArgumentException("plugin already exists: " + p);
            }
            list.add(p);
        }
        return list;
    }

    private static ClientTemplate buildTemplate(Settings providedSettings, Settings defaultSettings,
                                                Collection<Class<? extends Plugin>> plugins) {
        if (Node.NODE_NAME_SETTING.exists(providedSettings) == false) {
            providedSettings = Settings.builder().put(providedSettings).put(Node.NODE_NAME_SETTING.getKey(), "_client_").build();
        }
        final PluginsService pluginsService = newPluginService(providedSettings, plugins);
        final Settings settings = Settings.builder().put(defaultSettings).put(pluginsService.updatedSettings()).build();
        final List<Closeable> resourcesToClose = new ArrayList<>();
        final ThreadPool threadPool = new ThreadPool(settings);
        resourcesToClose.add(() -> ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));
        final NetworkService networkService = new NetworkService(settings, Collections.emptyList());
        try {
            final List<Setting<?>> additionalSettings = new ArrayList<>();
            final List<String> additionalSettingsFilter = new ArrayList<>();
            additionalSettings.addAll(pluginsService.getPluginSettings());
            additionalSettingsFilter.addAll(pluginsService.getPluginSettingsFilter());
            for (final ExecutorBuilder<?> builder : threadPool.builders()) {
                additionalSettings.addAll(builder.getRegisteredSettings());
            }
            SettingsModule settingsModule = new SettingsModule(settings, additionalSettings, additionalSettingsFilter);

            NetworkModule networkModule = new NetworkModule(networkService, settings, true);
            SearchModule searchModule = new SearchModule(settings, true, pluginsService.filterPlugins(SearchPlugin.class));
            List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
            entries.addAll(networkModule.getNamedWriteables());
            entries.addAll(searchModule.getNamedWriteables());
            entries.addAll(pluginsService.filterPlugins(Plugin.class).stream()
                                         .flatMap(p -> p.getNamedWriteables().stream())
                                         .collect(Collectors.toList()));
            NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(entries);

            ModulesBuilder modules = new ModulesBuilder();
            // plugin modules must be added here, before others or we can get crazy injection errors...
            for (Module pluginModule : pluginsService.createGuiceModules()) {
                modules.add(pluginModule);
            }
            modules.add(networkModule);
            modules.add(b -> b.bind(ThreadPool.class).toInstance(threadPool));
            modules.add(searchModule);
            ActionModule actionModule = new ActionModule(false, true, settings, null, settingsModule.getClusterSettings(),
                pluginsService.filterPlugins(ActionPlugin.class));
            modules.add(actionModule);

            pluginsService.processModules(modules);
            CircuitBreakerService circuitBreakerService = Node.createCircuitBreakerService(settingsModule.getSettings(),
                settingsModule.getClusterSettings());
            resourcesToClose.add(circuitBreakerService);
            BigArrays bigArrays = new BigArrays(settings, circuitBreakerService);
            resourcesToClose.add(bigArrays);
            modules.add(settingsModule);
            modules.add((b -> {
                b.bind(BigArrays.class).toInstance(bigArrays);
                b.bind(PluginsService.class).toInstance(pluginsService);
                b.bind(CircuitBreakerService.class).toInstance(circuitBreakerService);
                b.bind(NamedWriteableRegistry.class).toInstance(namedWriteableRegistry);
            }));

            Injector injector = modules.createInjector();
            final TransportService transportService = injector.getInstance(TransportService.class);
            final TransportClientNodesService nodesService =
                new TransportClientNodesService(settings, transportService, threadPool);
            final TransportProxyClient proxy = new TransportProxyClient(settings, transportService, nodesService,
                actionModule.getActions().values().stream().map(x -> x.getAction()).collect(Collectors.toList()));

            List<LifecycleComponent> pluginLifecycleComponents = new ArrayList<>();
            pluginLifecycleComponents.addAll(pluginsService.getGuiceServiceClasses().stream()
                .map(injector::getInstance).collect(Collectors.toList()));
            resourcesToClose.addAll(pluginLifecycleComponents);

            transportService.start();
            transportService.acceptIncomingRequests();

            ClientTemplate transportClient = new ClientTemplate(injector, pluginLifecycleComponents, nodesService, proxy, namedWriteableRegistry);
            resourcesToClose.clear();
            return transportClient;
        } finally {
            IOUtils.closeWhileHandlingException(resourcesToClose);
        }
    }

    private static final class ClientTemplate {
        final Injector injector;
        private final List<LifecycleComponent> pluginLifecycleComponents;
        private final TransportClientNodesService nodesService;
        private final TransportProxyClient proxy;
        private final NamedWriteableRegistry namedWriteableRegistry;

        private ClientTemplate(Injector injector, List<LifecycleComponent> pluginLifecycleComponents,
                TransportClientNodesService nodesService, TransportProxyClient proxy, NamedWriteableRegistry namedWriteableRegistry) {
            this.injector = injector;
            this.pluginLifecycleComponents = pluginLifecycleComponents;
            this.nodesService = nodesService;
            this.proxy = proxy;
            this.namedWriteableRegistry = namedWriteableRegistry;
        }

        Settings getSettings() {
            return injector.getInstance(Settings.class);
        }

        ThreadPool getThreadPool() {
            return injector.getInstance(ThreadPool.class);
        }
    }

    public static final String CLIENT_TYPE = "transport";

    final Injector injector;
    final NamedWriteableRegistry namedWriteableRegistry;

    private final List<LifecycleComponent> pluginLifecycleComponents;
    private final TransportClientNodesService nodesService;
    private final TransportProxyClient proxy;

    /**
     * Creates a new TransportClient with the given settings and plugins
     */
    public TransportClient(Settings settings, Collection<Class<? extends Plugin>> plugins) {
        this(buildTemplate(settings, Settings.EMPTY, plugins));
    }

    /**
     * Creates a new TransportClient with the given settings, defaults and plugins.
     * @param settings the client settings
     * @param defaultSettings default settings that are merged after the plugins have added it's additional settings.
     * @param plugins the client plugins
     */
    protected TransportClient(Settings settings, Settings defaultSettings, Collection<Class<? extends Plugin>> plugins) {
        this(buildTemplate(settings, defaultSettings, plugins));
    }

    private TransportClient(ClientTemplate template) {
        super(template.getSettings(), template.getThreadPool());
        this.injector = template.injector;
        this.pluginLifecycleComponents = Collections.unmodifiableList(template.pluginLifecycleComponents);
        this.nodesService = template.nodesService;
        this.proxy = template.proxy;
        this.namedWriteableRegistry = template.namedWriteableRegistry;
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
     * <p>
     * The nodes include all the nodes that are currently alive based on the transport
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
     * <p>
     * The Node this transport address represents will be used if its possible to connect to it.
     * If it is unavailable, it will be automatically connected to once it is up.
     * <p>
     * In order to get the list of all the current connected nodes, please see {@link #connectedNodes()}.
     */
    public TransportClient addTransportAddress(TransportAddress transportAddress) {
        nodesService.addTransportAddresses(transportAddress);
        return this;
    }

    /**
     * Adds a list of transport addresses that will be used to connect to.
     * <p>
     * The Node this transport address represents will be used if its possible to connect to it.
     * If it is unavailable, it will be automatically connected to once it is up.
     * <p>
     * In order to get the list of all the current connected nodes, please see {@link #connectedNodes()}.
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
        List<Closeable> closeables = new ArrayList<>();
        closeables.add(nodesService);
        closeables.add(injector.getInstance(TransportService.class));

        for (LifecycleComponent plugin : pluginLifecycleComponents) {
            closeables.add(plugin);
        }
        closeables.add(() -> ThreadPool.terminate(injector.getInstance(ThreadPool.class), 10, TimeUnit.SECONDS));
        closeables.add(injector.getInstance(BigArrays.class));
        IOUtils.closeWhileHandlingException(closeables);
    }

    @Override
    protected <Request extends ActionRequest<Request>, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
        proxy.execute(action, request, listener);
    }
}
