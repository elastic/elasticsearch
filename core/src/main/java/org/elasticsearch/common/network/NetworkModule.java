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

package org.elasticsearch.common.network;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.Headers;
import org.elasticsearch.client.transport.TransportClientNodesService;
import org.elasticsearch.client.transport.support.TransportProxyClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.ExtensionPoint;
import org.elasticsearch.http.HttpServer;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.netty.NettyHttpServerTransport;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestGlobalContext;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.admin.cluster.health.RestClusterHealthAction;
import org.elasticsearch.rest.action.admin.cluster.node.hotthreads.RestNodesHotThreadsAction;
import org.elasticsearch.rest.action.admin.cluster.node.info.RestNodesInfoAction;
import org.elasticsearch.rest.action.admin.cluster.node.stats.RestNodesStatsAction;
import org.elasticsearch.rest.action.admin.cluster.repositories.delete.RestDeleteRepositoryAction;
import org.elasticsearch.rest.action.admin.cluster.repositories.get.RestGetRepositoriesAction;
import org.elasticsearch.rest.action.admin.cluster.repositories.put.RestPutRepositoryAction;
import org.elasticsearch.rest.action.admin.cluster.repositories.verify.RestVerifyRepositoryAction;
import org.elasticsearch.rest.action.admin.cluster.reroute.RestClusterRerouteAction;
import org.elasticsearch.rest.action.admin.cluster.settings.RestClusterGetSettingsAction;
import org.elasticsearch.rest.action.admin.cluster.settings.RestClusterUpdateSettingsAction;
import org.elasticsearch.rest.action.admin.cluster.shards.RestClusterSearchShardsAction;
import org.elasticsearch.rest.action.admin.cluster.snapshots.create.RestCreateSnapshotAction;
import org.elasticsearch.rest.action.admin.cluster.snapshots.delete.RestDeleteSnapshotAction;
import org.elasticsearch.rest.action.admin.cluster.snapshots.get.RestGetSnapshotsAction;
import org.elasticsearch.rest.action.admin.cluster.snapshots.restore.RestRestoreSnapshotAction;
import org.elasticsearch.rest.action.admin.cluster.snapshots.status.RestSnapshotsStatusAction;
import org.elasticsearch.rest.action.admin.cluster.state.RestClusterStateAction;
import org.elasticsearch.rest.action.admin.cluster.stats.RestClusterStatsAction;
import org.elasticsearch.rest.action.admin.cluster.tasks.RestPendingClusterTasksAction;
import org.elasticsearch.rest.action.admin.indices.alias.RestIndicesAliasesAction;
import org.elasticsearch.rest.action.admin.indices.alias.delete.RestIndexDeleteAliasesAction;
import org.elasticsearch.rest.action.admin.indices.alias.get.RestGetAliasesAction;
import org.elasticsearch.rest.action.admin.indices.alias.get.RestGetIndicesAliasesAction;
import org.elasticsearch.rest.action.admin.indices.alias.head.RestAliasesExistAction;
import org.elasticsearch.rest.action.admin.indices.alias.put.RestIndexPutAliasAction;
import org.elasticsearch.rest.action.admin.indices.analyze.RestAnalyzeAction;
import org.elasticsearch.rest.action.admin.indices.cache.clear.RestClearIndicesCacheAction;
import org.elasticsearch.rest.action.admin.indices.close.RestCloseIndexAction;
import org.elasticsearch.rest.action.admin.indices.create.RestCreateIndexAction;
import org.elasticsearch.rest.action.admin.indices.delete.RestDeleteIndexAction;
import org.elasticsearch.rest.action.admin.indices.exists.indices.RestIndicesExistsAction;
import org.elasticsearch.rest.action.admin.indices.exists.types.RestTypesExistsAction;
import org.elasticsearch.rest.action.admin.indices.flush.RestFlushAction;
import org.elasticsearch.rest.action.admin.indices.flush.RestSyncedFlushAction;
import org.elasticsearch.rest.action.admin.indices.forcemerge.RestForceMergeAction;
import org.elasticsearch.rest.action.admin.indices.get.RestGetIndicesAction;
import org.elasticsearch.rest.action.admin.indices.mapping.get.RestGetFieldMappingAction;
import org.elasticsearch.rest.action.admin.indices.mapping.get.RestGetMappingAction;
import org.elasticsearch.rest.action.admin.indices.mapping.put.RestPutMappingAction;
import org.elasticsearch.rest.action.admin.indices.open.RestOpenIndexAction;
import org.elasticsearch.rest.action.admin.indices.recovery.RestRecoveryAction;
import org.elasticsearch.rest.action.admin.indices.refresh.RestRefreshAction;
import org.elasticsearch.rest.action.admin.indices.segments.RestIndicesSegmentsAction;
import org.elasticsearch.rest.action.admin.indices.settings.RestGetSettingsAction;
import org.elasticsearch.rest.action.admin.indices.settings.RestUpdateSettingsAction;
import org.elasticsearch.rest.action.admin.indices.shards.RestIndicesShardStoresAction;
import org.elasticsearch.rest.action.admin.indices.stats.RestIndicesStatsAction;
import org.elasticsearch.rest.action.admin.indices.template.delete.RestDeleteIndexTemplateAction;
import org.elasticsearch.rest.action.admin.indices.template.get.RestGetIndexTemplateAction;
import org.elasticsearch.rest.action.admin.indices.template.head.RestHeadIndexTemplateAction;
import org.elasticsearch.rest.action.admin.indices.template.put.RestPutIndexTemplateAction;
import org.elasticsearch.rest.action.admin.indices.upgrade.RestUpgradeAction;
import org.elasticsearch.rest.action.admin.indices.validate.query.RestValidateQueryAction;
import org.elasticsearch.rest.action.admin.indices.validate.template.RestRenderSearchTemplateAction;
import org.elasticsearch.rest.action.bulk.RestBulkAction;
import org.elasticsearch.rest.action.cat.AbstractCatAction;
import org.elasticsearch.rest.action.cat.RestAliasAction;
import org.elasticsearch.rest.action.cat.RestAllocationAction;
import org.elasticsearch.rest.action.cat.RestCatAction;
import org.elasticsearch.rest.action.cat.RestCountCatAction;
import org.elasticsearch.rest.action.cat.RestFielddataAction;
import org.elasticsearch.rest.action.cat.RestHealthAction;
import org.elasticsearch.rest.action.cat.RestIndicesAction;
import org.elasticsearch.rest.action.cat.RestMasterAction;
import org.elasticsearch.rest.action.cat.RestNodeAttrsAction;
import org.elasticsearch.rest.action.cat.RestNodesAction;
import org.elasticsearch.rest.action.cat.RestPendingClusterTasksCatAction;
import org.elasticsearch.rest.action.cat.RestPluginsAction;
import org.elasticsearch.rest.action.cat.RestRecoveryCatAction;
import org.elasticsearch.rest.action.cat.RestRepositoriesAction;
import org.elasticsearch.rest.action.cat.RestSegmentsAction;
import org.elasticsearch.rest.action.cat.RestShardsAction;
import org.elasticsearch.rest.action.cat.RestSnapshotAction;
import org.elasticsearch.rest.action.cat.RestThreadPoolAction;
import org.elasticsearch.rest.action.count.RestCountAction;
import org.elasticsearch.rest.action.delete.RestDeleteAction;
import org.elasticsearch.rest.action.explain.RestExplainAction;
import org.elasticsearch.rest.action.fieldstats.RestFieldStatsAction;
import org.elasticsearch.rest.action.get.RestGetAction;
import org.elasticsearch.rest.action.get.RestGetSourceAction;
import org.elasticsearch.rest.action.get.RestHeadAction;
import org.elasticsearch.rest.action.get.RestMultiGetAction;
import org.elasticsearch.rest.action.index.RestIndexAction;
import org.elasticsearch.rest.action.main.RestMainAction;
import org.elasticsearch.rest.action.percolate.RestMultiPercolateAction;
import org.elasticsearch.rest.action.percolate.RestPercolateAction;
import org.elasticsearch.rest.action.script.RestDeleteIndexedScriptAction;
import org.elasticsearch.rest.action.script.RestGetIndexedScriptAction;
import org.elasticsearch.rest.action.script.RestPutIndexedScriptAction;
import org.elasticsearch.rest.action.search.RestClearScrollAction;
import org.elasticsearch.rest.action.search.RestMultiSearchAction;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.rest.action.search.RestSearchScrollAction;
import org.elasticsearch.rest.action.suggest.RestSuggestAction;
import org.elasticsearch.rest.action.template.RestDeleteSearchTemplateAction;
import org.elasticsearch.rest.action.template.RestGetSearchTemplateAction;
import org.elasticsearch.rest.action.template.RestPutSearchTemplateAction;
import org.elasticsearch.rest.action.termvectors.RestMultiTermVectorsAction;
import org.elasticsearch.rest.action.termvectors.RestTermVectorsAction;
import org.elasticsearch.rest.action.update.RestUpdateAction;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.local.LocalTransport;
import org.elasticsearch.transport.netty.NettyTransport;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * A module to handle registering and binding all network related classes.
 */
public class NetworkModule extends AbstractModule {

    public static final String TRANSPORT_TYPE_KEY = "transport.type";
    public static final String TRANSPORT_SERVICE_TYPE_KEY = "transport.service.type";

    public static final String LOCAL_TRANSPORT = "local";
    public static final String NETTY_TRANSPORT = "netty";

    public static final String HTTP_TYPE_KEY = "http.type";
    public static final String HTTP_ENABLED = "http.enabled";

    private final NetworkService networkService;
    private final RestController controller;
    private final Settings settings;
    private final boolean transportClient;

    private final ExtensionPoint.SelectedType<TransportService> transportServiceTypes = new ExtensionPoint.SelectedType<>("transport_service", TransportService.class);
    private final ExtensionPoint.SelectedType<Transport> transportTypes = new ExtensionPoint.SelectedType<>("transport", Transport.class);
    private final ExtensionPoint.SelectedType<HttpServerTransport> httpTransportTypes = new ExtensionPoint.SelectedType<>("http_transport", HttpServerTransport.class);
    private final Map<Class<? extends BaseRestHandler>, Function<RestGlobalContext, ? extends BaseRestHandler>> actions = new HashMap<>();

    private RestGlobalContext context;
    /**
     * Used only to build the RestClusterGetSettingsAction.
     */
    private ClusterSettings clusterSettings;
    /**
     * Used only to build RestMainAction.
     */
    private ClusterName clusterName;
    /**
     * Used only to build RestMainAction.
     */
    private ClusterService clusterService;

    /**
     * Creates a network module that custom networking classes can be plugged into.
     *
     * @param networkService A constructed network service object to bind.
     * @param settings The settings for the node
     * @param transportClient True if only transport classes should be allowed to be registered, false otherwise.
     */
    public NetworkModule(NetworkService networkService, Settings settings, boolean transportClient, Version version) {
        this.networkService = networkService;
        this.settings = settings;
        this.transportClient = transportClient;
        registerTransportService(NETTY_TRANSPORT, TransportService.class);
        registerTransport(LOCAL_TRANSPORT, LocalTransport.class);
        registerTransport(NETTY_TRANSPORT, NettyTransport.class);

        if (transportClient) {
            controller = null;
        } else {
            registerHttpTransport(NETTY_TRANSPORT, NettyHttpServerTransport.class);
            controller = new RestController(settings);
            registerRestAction(RestMainAction.class, c -> new RestMainAction(c, version, clusterName, clusterService));
            registerRestAction(RestAnalyzeAction.class, RestAnalyzeAction::new);
            registerRestAction(RestFieldStatsAction.class, RestFieldStatsAction::new);
            registerRestAction(RestSuggestAction.class, RestSuggestAction::new);
            registerRestAction(RestTypesExistsAction.class, RestTypesExistsAction::new);

            registerAliasActions();
            registerCatActions();
            registerClusterActions();
            registerDocumentActions();
            registerIndexActions();
            registerIndexTemplateActions();
            registerIndexedScriptsActions();
            registerMappingActions();
            registerNodeActions();
            registerPercolatorActions();
            registerSearchActions();
            registerSearchTemplateActions();
            registerSettingsActions();
            registerSnapshotActions();
        }
    }

    /** Adds a transport service implementation that can be selected by setting {@link #TRANSPORT_SERVICE_TYPE_KEY}. */
    public void registerTransportService(String name, Class<? extends TransportService> clazz) {
        transportServiceTypes.registerExtension(name, clazz);
    }

    /** Adds a transport implementation that can be selected by setting {@link #TRANSPORT_TYPE_KEY}. */
    public void registerTransport(String name, Class<? extends Transport> clazz) {
        transportTypes.registerExtension(name, clazz);
    }

    /** Adds an http transport implementation that can be selected by setting {@link #HTTP_TYPE_KEY}. */
    // TODO: we need another name than "http transport"....so confusing with transportClient...
    public void registerHttpTransport(String name, Class<? extends HttpServerTransport> clazz) {
        if (transportClient) {
            throw new IllegalArgumentException("Cannot register http transport " + clazz.getName() + " for transport client");
        }
        httpTransportTypes.registerExtension(name, clazz);
    }

    public <T extends BaseRestHandler> void registerRestAction(Class<T> type, Function<RestGlobalContext, T> builder) {
        requireNonNull(type, "Must define the action type being registered");
        requireNonNull(builder, "Must define the builder to register");
        Object old = actions.putIfAbsent(type, builder);
        if (old != null) {
            throw new IllegalArgumentException("Action [" + type + "] is already defined.");
        }
    }

    @Override
    protected void configure() {
        bind(NetworkService.class).toInstance(networkService);
        bind(NamedWriteableRegistry.class).asEagerSingleton();

        transportServiceTypes.bindType(binder(), settings, TRANSPORT_SERVICE_TYPE_KEY, NETTY_TRANSPORT);
        String defaultTransport = DiscoveryNode.localNode(settings) ? LOCAL_TRANSPORT : NETTY_TRANSPORT;
        transportTypes.bindType(binder(), settings, TRANSPORT_TYPE_KEY, defaultTransport);

        if (transportClient) {
            bind(Headers.class).asEagerSingleton();
            bind(TransportProxyClient.class).asEagerSingleton();
            bind(TransportClientNodesService.class).asEagerSingleton();
        } else {
            // Bind the controller so the lifecycle stuff still works
            bind(RestController.class).toInstance(controller);
            if (settings.getAsBoolean(HTTP_ENABLED, true)) {
                bind(HttpServer.class).asEagerSingleton();
                httpTransportTypes.bindType(binder(), settings, HTTP_TYPE_KEY, NETTY_TRANSPORT);
            }
        }
    }

    /**
     * Called by Guice to setup REST actions when their dependencies are ready.
     * To remove guice we need to remove parameters from this method and move
     * them to the constructor.
     */
    @Inject
    public void injectRestActionDependencies(Client client, IndicesQueriesRegistry indicesQueriesRegistry,
            IndexNameExpressionResolver indexNameExpressionResolver, SettingsFilter settingsFilter, ClusterSettings clusterSettings,
            ClusterName clusterName, ClusterService clusterService) {
        this.clusterSettings = clusterSettings;
        this.clusterName = clusterName;
        this.clusterService = clusterService;
        context = new RestGlobalContext(settings, controller, client, indicesQueriesRegistry, indexNameExpressionResolver,
                settingsFilter);
    }

    /**
     * Setup the rest actions after all dependencies are available. This should
     * be called after guice has been called or plugins that depend on some
     * guice injected thing to setup their actions won't have them available.
     */
    public void setupRestActions() {
        if (transportClient) {
            throw new IllegalStateException("It doesn't make any sense to setup REST actions for the rest client!");
        }
        requireNonNull(context, "Rest actions cannot be setup before their dependencies have been injected");
        Set<AbstractCatAction> catActions = new HashSet<>();

        for (Map.Entry<Class<? extends BaseRestHandler>, Function<RestGlobalContext, ? extends BaseRestHandler>> t : actions.entrySet()) {
            BaseRestHandler handler = t.getValue().apply(context);
            if (t.getKey() != handler.getClass()) {
                throw new IllegalStateException(
                        "REST handler registered as [" + t.getKey() + "] but actually build a [" + handler.getClass() + "]");
            }
            if (handler instanceof AbstractCatAction) {
                catActions.add((AbstractCatAction) handler);
            }
            registerHandlerWithController(handler);
        }
        registerHandlerWithController(new RestCatAction(context, catActions));
    }

    private void registerHandlerWithController(BaseRestHandler handler) {
        Collection<Tuple<RestRequest.Method, String>> registrations = handler.registrations();
        if (registrations.isEmpty()) {
            throw new IllegalArgumentException(
                    "It doesn't make any sense to register a REST [ " + handler + "] handler without any registrations!");
        }
        for (Tuple<RestRequest.Method, String> registration: registrations) {
            controller.registerHandler(registration.v1(), registration.v2(), handler);
        }
    }

    private void registerAliasActions() {
        registerRestAction(RestAliasesExistAction.class, RestAliasesExistAction::new);
        registerRestAction(RestCreateIndexAction.class, RestCreateIndexAction::new);
        registerRestAction(RestDeleteIndexAction.class, RestDeleteIndexAction::new);
        registerRestAction(RestGetAliasesAction.class, RestGetAliasesAction::new);
        registerRestAction(RestGetIndicesAliasesAction.class, RestGetIndicesAliasesAction::new);
        registerRestAction(RestIndexDeleteAliasesAction.class, RestIndexDeleteAliasesAction::new);
        registerRestAction(RestIndexPutAliasAction.class, RestIndexPutAliasAction::new);
        registerRestAction(RestIndicesAliasesAction.class, RestIndicesAliasesAction::new);
    }

    private void registerCatActions() {
        registerRestAction(RestAliasAction.class, RestAliasAction::new);
        registerRestAction(RestAllocationAction.class, RestAllocationAction::new);
        registerRestAction(RestCountCatAction.class, RestCountCatAction::new);
        registerRestAction(RestFielddataAction.class, RestFielddataAction::new);
        registerRestAction(RestHealthAction.class, RestHealthAction::new);
        registerRestAction(RestIndicesAction.class, RestIndicesAction::new);
        registerRestAction(RestMasterAction.class, RestMasterAction::new);
        registerRestAction(RestNodeAttrsAction.class, RestNodeAttrsAction::new);
        registerRestAction(RestNodesAction.class, RestNodesAction::new);
        registerRestAction(RestPendingClusterTasksCatAction.class, RestPendingClusterTasksCatAction::new);
        registerRestAction(RestPluginsAction.class, RestPluginsAction::new);
        registerRestAction(RestRecoveryCatAction.class, RestRecoveryCatAction::new);
        registerRestAction(RestRepositoriesAction.class, RestRepositoriesAction::new);
        registerRestAction(RestSegmentsAction.class, RestSegmentsAction::new);
        registerRestAction(RestShardsAction.class, RestShardsAction::new);
        registerRestAction(RestSnapshotAction.class, RestSnapshotAction::new);
        registerRestAction(RestThreadPoolAction.class, RestThreadPoolAction::new);
    }

    private void registerClusterActions() {
        registerRestAction(RestClusterHealthAction.class, RestClusterHealthAction::new);
        registerRestAction(RestClusterRerouteAction.class, RestClusterRerouteAction::new);
        registerRestAction(RestClusterSearchShardsAction.class, RestClusterSearchShardsAction::new);
        registerRestAction(RestClusterStateAction.class, RestClusterStateAction::new);
        registerRestAction(RestClusterStatsAction.class, RestClusterStatsAction::new);
        registerRestAction(RestClusterGetSettingsAction.class, c -> new RestClusterGetSettingsAction(c, clusterSettings));
        registerRestAction(RestClusterUpdateSettingsAction.class, RestClusterUpdateSettingsAction::new);
        registerRestAction(RestPendingClusterTasksAction.class, RestPendingClusterTasksAction::new);
    }

    private void registerDocumentActions() {
        registerRestAction(RestBulkAction.class, RestBulkAction::new);
        registerRestAction(RestCountAction.class, RestCountAction::new);
        registerRestAction(RestDeleteAction.class, RestDeleteAction::new);
        registerRestAction(RestGetAction.class, RestGetAction::new);
        registerRestAction(RestGetSourceAction.class, RestGetSourceAction::new);
        registerRestAction(RestHeadAction.class, RestHeadAction::new);
        registerRestAction(RestIndexAction.class, RestIndexAction::new);
        registerRestAction(RestMultiGetAction.class, RestMultiGetAction::new);
        registerRestAction(RestMultiTermVectorsAction.class, RestMultiTermVectorsAction::new);
        registerRestAction(RestTermVectorsAction.class, RestTermVectorsAction::new);
        registerRestAction(RestUpdateAction.class, RestUpdateAction::new);
    }

    private void registerIndexActions() {
        registerRestAction(RestCloseIndexAction.class, RestCloseIndexAction::new);
        registerRestAction(RestClearIndicesCacheAction.class, RestClearIndicesCacheAction::new);
        registerRestAction(RestFlushAction.class, RestFlushAction::new);
        registerRestAction(RestForceMergeAction.class, RestForceMergeAction::new);
        registerRestAction(RestGetIndicesAction.class, RestGetIndicesAction::new);
        registerRestAction(RestIndicesExistsAction.class, RestIndicesExistsAction::new);
        registerRestAction(RestIndicesSegmentsAction.class, RestIndicesSegmentsAction::new);
        registerRestAction(RestIndicesShardStoresAction.class, RestIndicesShardStoresAction::new);
        registerRestAction(RestIndicesStatsAction.class, RestIndicesStatsAction::new);
        registerRestAction(RestOpenIndexAction.class, RestOpenIndexAction::new);
        registerRestAction(RestRecoveryAction.class, RestRecoveryAction::new);
        registerRestAction(RestRefreshAction.class, RestRefreshAction::new);
        registerRestAction(RestSyncedFlushAction.class, RestSyncedFlushAction::new);
        registerRestAction(RestUpgradeAction.class, RestUpgradeAction::new);
    }

    private void registerIndexTemplateActions() {
        registerRestAction(RestDeleteIndexTemplateAction.class, RestDeleteIndexTemplateAction::new);
        registerRestAction(RestGetIndexTemplateAction.class, RestGetIndexTemplateAction::new);
        registerRestAction(RestHeadIndexTemplateAction.class, RestHeadIndexTemplateAction::new);
        registerRestAction(RestPutIndexTemplateAction.class, RestPutIndexTemplateAction::new);
    }

    private void registerIndexedScriptsActions() {
        registerRestAction(RestDeleteIndexedScriptAction.class, RestDeleteIndexedScriptAction::new);
        registerRestAction(RestGetIndexedScriptAction.class, RestGetIndexedScriptAction::new);
        registerRestAction(RestPutIndexedScriptAction.class, RestPutIndexedScriptAction::new);
    }

    private void registerMappingActions() {
        registerRestAction(RestGetFieldMappingAction.class, RestGetFieldMappingAction::new);
        registerRestAction(RestGetMappingAction.class, RestGetMappingAction::new);
        registerRestAction(RestPutMappingAction.class, RestPutMappingAction::new);
    }

    private void registerNodeActions() {
        registerRestAction(RestNodesHotThreadsAction.class, RestNodesHotThreadsAction::new);
        registerRestAction(RestNodesInfoAction.class, RestNodesInfoAction::new);
        registerRestAction(RestNodesStatsAction.class, RestNodesStatsAction::new);
    }

    private void registerPercolatorActions() {
        registerRestAction(RestPercolateAction.class, RestPercolateAction::new);
        registerRestAction(RestPercolateAction.RestCountPercolateDocHandler.class, RestPercolateAction.RestCountPercolateDocHandler::new);
        registerRestAction(RestPercolateAction.RestPercolateExistingDocHandler.class,
                RestPercolateAction.RestPercolateExistingDocHandler::new);
        registerRestAction(RestPercolateAction.RestCountPercolateExistingDocHandler.class,
                RestPercolateAction.RestCountPercolateExistingDocHandler::new);
        registerRestAction(RestMultiPercolateAction.class, RestMultiPercolateAction::new);
    }

    private void registerSettingsActions() {
        registerRestAction(RestGetSettingsAction.class, RestGetSettingsAction::new);
        registerRestAction(RestUpdateSettingsAction.class, RestUpdateSettingsAction::new);
    }

    private void registerSearchActions() {
        registerRestAction(RestClearScrollAction.class, RestClearScrollAction::new);
        registerRestAction(RestExplainAction.class, RestExplainAction::new);
        registerRestAction(RestSearchAction.class, RestSearchAction::new);
        registerRestAction(RestSearchScrollAction.class, RestSearchScrollAction::new);
        registerRestAction(RestMultiSearchAction.class, RestMultiSearchAction::new);
        registerRestAction(RestValidateQueryAction.class, RestValidateQueryAction::new);
    }

    private void registerSearchTemplateActions() {
        registerRestAction(RestGetSearchTemplateAction.class, RestGetSearchTemplateAction::new);
        registerRestAction(RestDeleteSearchTemplateAction.class, RestDeleteSearchTemplateAction::new);
        registerRestAction(RestRenderSearchTemplateAction.class, RestRenderSearchTemplateAction::new);
        registerRestAction(RestPutSearchTemplateAction.class, RestPutSearchTemplateAction::new);
    }

    private void registerSnapshotActions() {
        registerRestAction(RestGetSnapshotsAction.class, RestGetSnapshotsAction::new);
        registerRestAction(RestCreateSnapshotAction.class, RestCreateSnapshotAction::new);
        registerRestAction(RestDeleteRepositoryAction.class, RestDeleteRepositoryAction::new);
        registerRestAction(RestDeleteSnapshotAction.class, RestDeleteSnapshotAction::new);
        registerRestAction(RestGetRepositoriesAction.class, RestGetRepositoriesAction::new);
        registerRestAction(RestPutRepositoryAction.class, RestPutRepositoryAction::new);
        registerRestAction(RestRestoreSnapshotAction.class, RestRestoreSnapshotAction::new);
        registerRestAction(RestSnapshotsStatusAction.class, RestSnapshotsStatusAction::new);
        registerRestAction(RestVerifyRepositoryAction.class, RestVerifyRepositoryAction::new);
    }
}
