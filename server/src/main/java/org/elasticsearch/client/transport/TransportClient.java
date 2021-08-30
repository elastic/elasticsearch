/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportSettings;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;

/**
 * The transport client allows to create a client that is not part of the cluster, but simply connects to one
 * or more nodes directly by adding their respective addresses using
 * {@link #addTransportAddress(org.elasticsearch.common.transport.TransportAddress)}.
 * <p>
 * The transport client important modules used is the {@link org.elasticsearch.common.network.NetworkModule} which is
 * started in client mode (only connects, no bind).
 *
 * @deprecated {@link TransportClient} is deprecated in favour of the High Level REST client and will
 * be removed in Elasticsearch 8.0.
 */
@Deprecated
public abstract class TransportClient extends AbstractClient {

    public static final Setting<TimeValue> CLIENT_TRANSPORT_NODES_SAMPLER_INTERVAL =
        Setting.positiveTimeSetting("client.transport.nodes_sampler_interval", timeValueSeconds(5), Setting.Property.NodeScope);
    public static final Setting<TimeValue> CLIENT_TRANSPORT_PING_TIMEOUT =
        Setting.positiveTimeSetting("client.transport.ping_timeout", timeValueSeconds(5), Setting.Property.NodeScope);
    public static final Setting<Boolean> CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME =
        Setting.boolSetting("client.transport.ignore_cluster_name", false, Setting.Property.NodeScope);
    public static final Setting<Boolean> CLIENT_TRANSPORT_SNIFF =
        Setting.boolSetting("client.transport.sniff", false, Setting.Property.NodeScope);

    public static final String TRANSPORT_CLIENT_FEATURE = "transport_client";

    private static PluginsService newPluginService(final Settings settings, Collection<Class<? extends Plugin>> plugins) {
        final Settings.Builder settingsBuilder = Settings.builder()
                .put(TransportSettings.PING_SCHEDULE.getKey(), "5s") // enable by default the transport schedule ping interval
                .put(InternalSettingsPreparer.prepareSettings(settings))
                .put(NetworkService.NETWORK_SERVER.getKey(), false)
                .put(CLIENT_TYPE_SETTING_S.getKey(), CLIENT_TYPE);
        return new PluginsService(settingsBuilder.build(), null, null, null, plugins);
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
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
                                                Collection<Class<? extends Plugin>> plugins, HostFailureListener failureListner) {
        if (Node.NODE_NAME_SETTING.exists(providedSettings) == false) {
            providedSettings = Settings.builder().put(providedSettings).put(Node.NODE_NAME_SETTING.getKey(), "_client_").build();
        }
        final PluginsService pluginsService = newPluginService(providedSettings, plugins);
        final List<Closeable> resourcesToClose = new ArrayList<>();
        final Settings settings =
            Settings.builder()
                .put(defaultSettings)
                .put(pluginsService.updatedSettings())
                .put(TransportSettings.FEATURE_PREFIX + "." + TRANSPORT_CLIENT_FEATURE, true)
                .build();
        final ThreadPool threadPool = new ThreadPool(settings);
        resourcesToClose.add(() -> ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));
        final NetworkService networkService = new NetworkService(emptyList());
        try {
            final List<Setting<?>> additionalSettings = new ArrayList<>(pluginsService.getPluginSettings());
            final List<String> additionalSettingsFilter = new ArrayList<>(pluginsService.getPluginSettingsFilter());
            for (final ExecutorBuilder<?> builder : threadPool.builders()) {
                additionalSettings.addAll(builder.getRegisteredSettings());
            }
            SettingsModule settingsModule =
                    new SettingsModule(settings, additionalSettings, additionalSettingsFilter, emptySet());

            SearchModule searchModule = new SearchModule(settings, true, pluginsService.filterPlugins(SearchPlugin.class));
            IndicesModule indicesModule = new IndicesModule(emptyList());
            List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
            entries.addAll(NetworkModule.getNamedWriteables());
            entries.addAll(searchModule.getNamedWriteables());
            entries.addAll(indicesModule.getNamedWriteables());
            entries.addAll(ClusterModule.getNamedWriteables());
            entries.addAll(pluginsService.filterPlugins(Plugin.class).stream()
                                         .flatMap(p -> p.getNamedWriteables().stream())
                                         .collect(Collectors.toList()));
            NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(entries);
            NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(Stream.of(
                    searchModule.getNamedXContents().stream(),
                    pluginsService.filterPlugins(Plugin.class).stream()
                            .flatMap(p -> p.getNamedXContent().stream())
                    ).flatMap(Function.identity()).collect(toList()));

            ModulesBuilder modules = new ModulesBuilder();
            // plugin modules must be added here, before others or we can get crazy injection errors...
            for (Module pluginModule : pluginsService.createGuiceModules()) {
                modules.add(pluginModule);
            }
            modules.add(b -> b.bind(ThreadPool.class).toInstance(threadPool));
            ActionModule actionModule = new ActionModule(true, settings, null, settingsModule.getIndexScopedSettings(),
                    settingsModule.getClusterSettings(), settingsModule.getSettingsFilter(), threadPool,
                    pluginsService.filterPlugins(ActionPlugin.class), null, null, null, new SystemIndices(emptyMap()));
            modules.add(actionModule);

            CircuitBreakerService circuitBreakerService = Node.createCircuitBreakerService(settingsModule.getSettings(),
                emptyList(),
                settingsModule.getClusterSettings());
            resourcesToClose.add(circuitBreakerService);
            PageCacheRecycler pageCacheRecycler = new PageCacheRecycler(settings);
            BigArrays bigArrays = new BigArrays(pageCacheRecycler, circuitBreakerService, CircuitBreaker.REQUEST);
            modules.add(settingsModule);
            NetworkModule networkModule = new NetworkModule(settings, true, pluginsService.filterPlugins(NetworkPlugin.class), threadPool,
                bigArrays, pageCacheRecycler, circuitBreakerService, namedWriteableRegistry, xContentRegistry, networkService, null,
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
            final Transport transport = networkModule.getTransportSupplier().get();
            final TransportService transportService = new TransportService(settings, transport, threadPool,
                networkModule.getTransportInterceptor(),
                boundTransportAddress -> DiscoveryNode.createLocal(settings, new TransportAddress(TransportAddress.META_ADDRESS, 0),
                    UUIDs.randomBase64UUID()), null, emptySet());
            modules.add((b -> {
                b.bind(BigArrays.class).toInstance(bigArrays);
                b.bind(PageCacheRecycler.class).toInstance(pageCacheRecycler);
                b.bind(PluginsService.class).toInstance(pluginsService);
                b.bind(CircuitBreakerService.class).toInstance(circuitBreakerService);
                b.bind(NamedWriteableRegistry.class).toInstance(namedWriteableRegistry);
                b.bind(Transport.class).toInstance(transport);
                b.bind(TransportService.class).toInstance(transportService);
                b.bind(NetworkService.class).toInstance(networkService);
            }));

            Injector injector = modules.createInjector();
            final TransportClientNodesService nodesService =
                new TransportClientNodesService(settings, transportService, threadPool, failureListner == null
                    ? (t, e) -> {} : failureListner);

            // construct the list of client actions
            final List<ActionPlugin> actionPlugins = pluginsService.filterPlugins(ActionPlugin.class);
            final List<ActionType<?>> clientActions =
                    actionPlugins.stream().flatMap(p -> p.getClientActions().stream()).collect(Collectors.toList());
            // add all the base actions
            final List<? extends ActionType<?>> baseActions =
                    actionModule.getActions().values().stream().map(ActionPlugin.ActionHandler::getAction).collect(Collectors.toList());
            clientActions.addAll(baseActions);
            final TransportProxyClient proxy = new TransportProxyClient(transportService, nodesService, clientActions);

            List<LifecycleComponent> pluginLifecycleComponents = new ArrayList<>(pluginsService.getGuiceServiceClasses().stream()
                .map(injector::getInstance).collect(Collectors.toList()));
            resourcesToClose.addAll(pluginLifecycleComponents);

            transportService.start();
            transportService.acceptIncomingRequests();

            ClientTemplate transportClient = new ClientTemplate(injector, pluginLifecycleComponents, nodesService, proxy,
                namedWriteableRegistry);
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
    protected final NamedWriteableRegistry namedWriteableRegistry;

    private final List<LifecycleComponent> pluginLifecycleComponents;
    private final TransportClientNodesService nodesService;
    private final TransportProxyClient proxy;

    /**
     * Creates a new TransportClient with the given settings and plugins
     */
    public TransportClient(Settings settings, Collection<Class<? extends Plugin>> plugins) {
        this(buildTemplate(settings, Settings.EMPTY, plugins, null));
    }

    /**
     * Creates a new TransportClient with the given settings, defaults and plugins.
     * @param settings the client settings
     * @param defaultSettings default settings that are merged after the plugins have added it's additional settings.
     * @param plugins the client plugins
     */
    protected TransportClient(Settings settings, Settings defaultSettings, Collection<Class<? extends Plugin>> plugins,
                              HostFailureListener hostFailureListener) {
        this(buildTemplate(settings, defaultSettings, plugins, hostFailureListener));
    }

    private TransportClient(ClientTemplate template) {
        super(template.getSettings(), template.getThreadPool());
        this.injector = template.injector;
        this.pluginLifecycleComponents = unmodifiableList(template.pluginLifecycleComponents);
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
        IOUtils.closeWhileHandlingException(closeables);
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse>
    void doExecute(ActionType<Response> action, Request request, ActionListener<Response> listener) {
        proxy.execute(action, request, listener);
    }

    /**
     * Listener that allows to be notified whenever a node failure / disconnect happens
     */
    @FunctionalInterface
    public interface HostFailureListener {
        /**
         * Called once a node disconnect is detected.
         * @param node the node that has been disconnected
         * @param ex the exception causing the disconnection
         */
        void onNodeDisconnected(DiscoveryNode node, Exception ex);
    }

    // pkg private for testing
    TransportClientNodesService getNodesService() {
        return nodesService;
    }
}
