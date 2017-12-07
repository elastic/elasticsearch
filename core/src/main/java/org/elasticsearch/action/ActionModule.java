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

package org.elasticsearch.action;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.node.liveness.TransportLivenessAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;
import org.elasticsearch.plugins.ClientActionPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.action.RestFieldCapabilitiesAction;
import org.elasticsearch.rest.action.RestMainAction;
import org.elasticsearch.rest.action.admin.cluster.RestCancelTasksAction;
import org.elasticsearch.rest.action.admin.cluster.RestClusterAllocationExplainAction;
import org.elasticsearch.rest.action.admin.cluster.RestClusterGetSettingsAction;
import org.elasticsearch.rest.action.admin.cluster.RestClusterHealthAction;
import org.elasticsearch.rest.action.admin.cluster.RestClusterRerouteAction;
import org.elasticsearch.rest.action.admin.cluster.RestClusterSearchShardsAction;
import org.elasticsearch.rest.action.admin.cluster.RestClusterStateAction;
import org.elasticsearch.rest.action.admin.cluster.RestClusterStatsAction;
import org.elasticsearch.rest.action.admin.cluster.RestClusterUpdateSettingsAction;
import org.elasticsearch.rest.action.admin.cluster.RestCreateSnapshotAction;
import org.elasticsearch.rest.action.admin.cluster.RestDeleteRepositoryAction;
import org.elasticsearch.rest.action.admin.cluster.RestDeleteSnapshotAction;
import org.elasticsearch.rest.action.admin.cluster.RestDeleteStoredScriptAction;
import org.elasticsearch.rest.action.admin.cluster.RestGetRepositoriesAction;
import org.elasticsearch.rest.action.admin.cluster.RestGetSnapshotsAction;
import org.elasticsearch.rest.action.admin.cluster.RestGetStoredScriptAction;
import org.elasticsearch.rest.action.admin.cluster.RestGetTaskAction;
import org.elasticsearch.rest.action.admin.cluster.RestListTasksAction;
import org.elasticsearch.rest.action.admin.cluster.RestNodesHotThreadsAction;
import org.elasticsearch.rest.action.admin.cluster.RestNodesInfoAction;
import org.elasticsearch.rest.action.admin.cluster.RestNodesStatsAction;
import org.elasticsearch.rest.action.admin.cluster.RestNodesUsageAction;
import org.elasticsearch.rest.action.admin.cluster.RestPendingClusterTasksAction;
import org.elasticsearch.rest.action.admin.cluster.RestPutRepositoryAction;
import org.elasticsearch.rest.action.admin.cluster.RestPutStoredScriptAction;
import org.elasticsearch.rest.action.admin.cluster.RestRemoteClusterInfoAction;
import org.elasticsearch.rest.action.admin.cluster.RestRestoreSnapshotAction;
import org.elasticsearch.rest.action.admin.cluster.RestSnapshotsStatusAction;
import org.elasticsearch.rest.action.admin.cluster.RestVerifyRepositoryAction;
import org.elasticsearch.rest.action.admin.indices.RestAnalyzeAction;
import org.elasticsearch.rest.action.admin.indices.RestClearIndicesCacheAction;
import org.elasticsearch.rest.action.admin.indices.RestCloseIndexAction;
import org.elasticsearch.rest.action.admin.indices.RestCreateIndexAction;
import org.elasticsearch.rest.action.admin.indices.RestDeleteIndexAction;
import org.elasticsearch.rest.action.admin.indices.RestDeleteIndexTemplateAction;
import org.elasticsearch.rest.action.admin.indices.RestFlushAction;
import org.elasticsearch.rest.action.admin.indices.RestForceMergeAction;
import org.elasticsearch.rest.action.admin.indices.RestGetAliasesAction;
import org.elasticsearch.rest.action.admin.indices.RestGetAllAliasesAction;
import org.elasticsearch.rest.action.admin.indices.RestGetAllMappingsAction;
import org.elasticsearch.rest.action.admin.indices.RestGetAllSettingsAction;
import org.elasticsearch.rest.action.admin.indices.RestGetFieldMappingAction;
import org.elasticsearch.rest.action.admin.indices.RestGetIndexTemplateAction;
import org.elasticsearch.rest.action.admin.indices.RestGetIndicesAction;
import org.elasticsearch.rest.action.admin.indices.RestGetMappingAction;
import org.elasticsearch.rest.action.admin.indices.RestGetSettingsAction;
import org.elasticsearch.rest.action.admin.indices.RestIndexDeleteAliasesAction;
import org.elasticsearch.rest.action.admin.indices.RestIndexPutAliasAction;
import org.elasticsearch.rest.action.admin.indices.RestIndicesAliasesAction;
import org.elasticsearch.rest.action.admin.indices.RestIndicesSegmentsAction;
import org.elasticsearch.rest.action.admin.indices.RestIndicesShardStoresAction;
import org.elasticsearch.rest.action.admin.indices.RestIndicesStatsAction;
import org.elasticsearch.rest.action.admin.indices.RestOpenIndexAction;
import org.elasticsearch.rest.action.admin.indices.RestPutIndexTemplateAction;
import org.elasticsearch.rest.action.admin.indices.RestPutMappingAction;
import org.elasticsearch.rest.action.admin.indices.RestRecoveryAction;
import org.elasticsearch.rest.action.admin.indices.RestRefreshAction;
import org.elasticsearch.rest.action.admin.indices.RestRolloverIndexAction;
import org.elasticsearch.rest.action.admin.indices.RestShrinkIndexAction;
import org.elasticsearch.rest.action.admin.indices.RestSplitIndexAction;
import org.elasticsearch.rest.action.admin.indices.RestSyncedFlushAction;
import org.elasticsearch.rest.action.admin.indices.RestUpdateSettingsAction;
import org.elasticsearch.rest.action.admin.indices.RestUpgradeAction;
import org.elasticsearch.rest.action.admin.indices.RestValidateQueryAction;
import org.elasticsearch.rest.action.cat.AbstractCatAction;
import org.elasticsearch.rest.action.cat.RestAliasAction;
import org.elasticsearch.rest.action.cat.RestAllocationAction;
import org.elasticsearch.rest.action.cat.RestCatAction;
import org.elasticsearch.rest.action.cat.RestFielddataAction;
import org.elasticsearch.rest.action.cat.RestHealthAction;
import org.elasticsearch.rest.action.cat.RestIndicesAction;
import org.elasticsearch.rest.action.cat.RestMasterAction;
import org.elasticsearch.rest.action.cat.RestNodeAttrsAction;
import org.elasticsearch.rest.action.cat.RestNodesAction;
import org.elasticsearch.rest.action.cat.RestPluginsAction;
import org.elasticsearch.rest.action.cat.RestRepositoriesAction;
import org.elasticsearch.rest.action.cat.RestSegmentsAction;
import org.elasticsearch.rest.action.cat.RestShardsAction;
import org.elasticsearch.rest.action.cat.RestSnapshotAction;
import org.elasticsearch.rest.action.cat.RestTasksAction;
import org.elasticsearch.rest.action.cat.RestTemplatesAction;
import org.elasticsearch.rest.action.cat.RestThreadPoolAction;
import org.elasticsearch.rest.action.document.RestBulkAction;
import org.elasticsearch.rest.action.document.RestDeleteAction;
import org.elasticsearch.rest.action.document.RestGetAction;
import org.elasticsearch.rest.action.document.RestGetSourceAction;
import org.elasticsearch.rest.action.document.RestIndexAction;
import org.elasticsearch.rest.action.document.RestMultiGetAction;
import org.elasticsearch.rest.action.document.RestMultiTermVectorsAction;
import org.elasticsearch.rest.action.document.RestTermVectorsAction;
import org.elasticsearch.rest.action.document.RestUpdateAction;
import org.elasticsearch.rest.action.ingest.RestDeletePipelineAction;
import org.elasticsearch.rest.action.ingest.RestGetPipelineAction;
import org.elasticsearch.rest.action.ingest.RestPutPipelineAction;
import org.elasticsearch.rest.action.ingest.RestSimulatePipelineAction;
import org.elasticsearch.rest.action.search.RestClearScrollAction;
import org.elasticsearch.rest.action.search.RestExplainAction;
import org.elasticsearch.rest.action.search.RestMultiSearchAction;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.rest.action.search.RestSearchScrollAction;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.usage.UsageService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * Builds and binds the generic action map, all {@link TransportAction}s, and {@link ActionFilters}.
 */
public class ActionModule extends AbstractModule {

    private static final Logger logger = ESLoggerFactory.getLogger(ActionModule.class);

    private final boolean transportClient;
    private final Settings settings;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final IndexScopedSettings indexScopedSettings;
    private final ClusterSettings clusterSettings;
    private final SettingsFilter settingsFilter;
    private final List<ActionPlugin> actionPlugins;
    private final ActionRegistrar actionRegistrar;
    private final ActionFilters actionFilters;
    private final AutoCreateIndex autoCreateIndex;
    private final DestructiveOperations destructiveOperations;
    private final RestController restController;

    public ActionModule(boolean transportClient, Settings settings, IndexNameExpressionResolver indexNameExpressionResolver,
                        IndexScopedSettings indexScopedSettings, ClusterSettings clusterSettings, SettingsFilter settingsFilter,
                        ThreadPool threadPool, List<ActionPlugin> actionPlugins, List<ClientActionPlugin> clientActionPlugins,
                        NodeClient nodeClient, CircuitBreakerService circuitBreakerService, UsageService usageService) {
        this.transportClient = transportClient;
        this.settings = settings;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.indexScopedSettings = indexScopedSettings;
        this.clusterSettings = clusterSettings;
        this.settingsFilter = settingsFilter;
        this.actionPlugins = actionPlugins;
        actionRegistrar = new ActionRegistrar(actionPlugins, clientActionPlugins);
        actionFilters = setupActionFilters(actionPlugins);
        autoCreateIndex = transportClient ? null : new AutoCreateIndex(settings, clusterSettings, indexNameExpressionResolver);
        destructiveOperations = new DestructiveOperations(settings, clusterSettings);
        Set<String> headers = actionPlugins.stream().flatMap(p -> p.getRestHeaders().stream()).collect(Collectors.toSet());
        UnaryOperator<RestHandler> restWrapper = null;
        for (ActionPlugin plugin : actionPlugins) {
            UnaryOperator<RestHandler> newRestWrapper = plugin.getRestHandlerWrapper(threadPool.getThreadContext());
            if (newRestWrapper != null) {
                logger.debug("Using REST wrapper from plugin " + plugin.getClass().getName());
                if (restWrapper != null) {
                    throw new IllegalArgumentException("Cannot have more than one plugin implementing a REST wrapper");
                }
                restWrapper = newRestWrapper;
            }
        }
        if (transportClient) {
            restController = null;
        } else {
            restController = new RestController(settings, headers, restWrapper, nodeClient, circuitBreakerService, usageService);
        }
    }

    public Collection<GenericAction<? extends ActionRequest, ? extends ActionResponse>> getClientActions() {
        return actionRegistrar.getClientActions().values();
    }

    private ActionFilters setupActionFilters(List<ActionPlugin> actionPlugins) {
        return new ActionFilters(
            Collections.unmodifiableSet(actionPlugins.stream().flatMap(p -> p.getActionFilters().stream()).collect(Collectors.toSet())));
    }

    public void initRestHandlers(Supplier<DiscoveryNodes> nodesInCluster) {
        List<AbstractCatAction> catActions = new ArrayList<>();
        Consumer<RestHandler> registerHandler = a -> {
            if (a instanceof AbstractCatAction) {
                catActions.add((AbstractCatAction) a);
            }
        };
        registerHandler.accept(new RestMainAction(settings, restController));
        registerHandler.accept(new RestNodesInfoAction(settings, restController, settingsFilter));
        registerHandler.accept(new RestRemoteClusterInfoAction(settings, restController));
        registerHandler.accept(new RestNodesStatsAction(settings, restController));
        registerHandler.accept(new RestNodesUsageAction(settings, restController));
        registerHandler.accept(new RestNodesHotThreadsAction(settings, restController));
        registerHandler.accept(new RestClusterAllocationExplainAction(settings, restController));
        registerHandler.accept(new RestClusterStatsAction(settings, restController));
        registerHandler.accept(new RestClusterStateAction(settings, restController, settingsFilter));
        registerHandler.accept(new RestClusterHealthAction(settings, restController));
        registerHandler.accept(new RestClusterUpdateSettingsAction(settings, restController));
        registerHandler.accept(new RestClusterGetSettingsAction(settings, restController, clusterSettings, settingsFilter));
        registerHandler.accept(new RestClusterRerouteAction(settings, restController, settingsFilter));
        registerHandler.accept(new RestClusterSearchShardsAction(settings, restController));
        registerHandler.accept(new RestPendingClusterTasksAction(settings, restController));
        registerHandler.accept(new RestPutRepositoryAction(settings, restController));
        registerHandler.accept(new RestGetRepositoriesAction(settings, restController, settingsFilter));
        registerHandler.accept(new RestDeleteRepositoryAction(settings, restController));
        registerHandler.accept(new RestVerifyRepositoryAction(settings, restController));
        registerHandler.accept(new RestGetSnapshotsAction(settings, restController));
        registerHandler.accept(new RestCreateSnapshotAction(settings, restController));
        registerHandler.accept(new RestRestoreSnapshotAction(settings, restController));
        registerHandler.accept(new RestDeleteSnapshotAction(settings, restController));
        registerHandler.accept(new RestSnapshotsStatusAction(settings, restController));

        registerHandler.accept(new RestGetAllAliasesAction(settings, restController));
        registerHandler.accept(new RestGetAllMappingsAction(settings, restController));
        registerHandler.accept(new RestGetAllSettingsAction(settings, restController, indexScopedSettings, settingsFilter));
        registerHandler.accept(new RestGetIndicesAction(settings, restController, indexScopedSettings, settingsFilter));
        registerHandler.accept(new RestIndicesStatsAction(settings, restController));
        registerHandler.accept(new RestIndicesSegmentsAction(settings, restController));
        registerHandler.accept(new RestIndicesShardStoresAction(settings, restController));
        registerHandler.accept(new RestGetAliasesAction(settings, restController));
        registerHandler.accept(new RestIndexDeleteAliasesAction(settings, restController));
        registerHandler.accept(new RestIndexPutAliasAction(settings, restController));
        registerHandler.accept(new RestIndicesAliasesAction(settings, restController));
        registerHandler.accept(new RestCreateIndexAction(settings, restController));
        registerHandler.accept(new RestShrinkIndexAction(settings, restController));
        registerHandler.accept(new RestSplitIndexAction(settings, restController));
        registerHandler.accept(new RestRolloverIndexAction(settings, restController));
        registerHandler.accept(new RestDeleteIndexAction(settings, restController));
        registerHandler.accept(new RestCloseIndexAction(settings, restController));
        registerHandler.accept(new RestOpenIndexAction(settings, restController));

        registerHandler.accept(new RestUpdateSettingsAction(settings, restController));
        registerHandler.accept(new RestGetSettingsAction(settings, restController, indexScopedSettings, settingsFilter));

        registerHandler.accept(new RestAnalyzeAction(settings, restController));
        registerHandler.accept(new RestGetIndexTemplateAction(settings, restController));
        registerHandler.accept(new RestPutIndexTemplateAction(settings, restController));
        registerHandler.accept(new RestDeleteIndexTemplateAction(settings, restController));

        registerHandler.accept(new RestPutMappingAction(settings, restController));
        registerHandler.accept(new RestGetMappingAction(settings, restController));
        registerHandler.accept(new RestGetFieldMappingAction(settings, restController));

        registerHandler.accept(new RestRefreshAction(settings, restController));
        registerHandler.accept(new RestFlushAction(settings, restController));
        registerHandler.accept(new RestSyncedFlushAction(settings, restController));
        registerHandler.accept(new RestForceMergeAction(settings, restController));
        registerHandler.accept(new RestUpgradeAction(settings, restController));
        registerHandler.accept(new RestClearIndicesCacheAction(settings, restController));

        registerHandler.accept(new RestIndexAction(settings, restController));
        registerHandler.accept(new RestGetAction(settings, restController));
        registerHandler.accept(new RestGetSourceAction(settings, restController));
        registerHandler.accept(new RestMultiGetAction(settings, restController));
        registerHandler.accept(new RestDeleteAction(settings, restController));
        registerHandler.accept(new org.elasticsearch.rest.action.document.RestCountAction(settings, restController));
        registerHandler.accept(new RestTermVectorsAction(settings, restController));
        registerHandler.accept(new RestMultiTermVectorsAction(settings, restController));
        registerHandler.accept(new RestBulkAction(settings, restController));
        registerHandler.accept(new RestUpdateAction(settings, restController));

        registerHandler.accept(new RestSearchAction(settings, restController));
        registerHandler.accept(new RestSearchScrollAction(settings, restController));
        registerHandler.accept(new RestClearScrollAction(settings, restController));
        registerHandler.accept(new RestMultiSearchAction(settings, restController));

        registerHandler.accept(new RestValidateQueryAction(settings, restController));

        registerHandler.accept(new RestExplainAction(settings, restController));

        registerHandler.accept(new RestRecoveryAction(settings, restController));

        // Scripts API
        registerHandler.accept(new RestGetStoredScriptAction(settings, restController));
        registerHandler.accept(new RestPutStoredScriptAction(settings, restController));
        registerHandler.accept(new RestDeleteStoredScriptAction(settings, restController));

        registerHandler.accept(new RestFieldCapabilitiesAction(settings, restController));

        // Tasks API
        registerHandler.accept(new RestListTasksAction(settings, restController, nodesInCluster));
        registerHandler.accept(new RestGetTaskAction(settings, restController));
        registerHandler.accept(new RestCancelTasksAction(settings, restController, nodesInCluster));

        // Ingest API
        registerHandler.accept(new RestPutPipelineAction(settings, restController));
        registerHandler.accept(new RestGetPipelineAction(settings, restController));
        registerHandler.accept(new RestDeletePipelineAction(settings, restController));
        registerHandler.accept(new RestSimulatePipelineAction(settings, restController));

        // CAT API
        registerHandler.accept(new RestAllocationAction(settings, restController));
        registerHandler.accept(new RestShardsAction(settings, restController));
        registerHandler.accept(new RestMasterAction(settings, restController));
        registerHandler.accept(new RestNodesAction(settings, restController));
        registerHandler.accept(new RestTasksAction(settings, restController, nodesInCluster));
        registerHandler.accept(new RestIndicesAction(settings, restController, indexNameExpressionResolver));
        registerHandler.accept(new RestSegmentsAction(settings, restController));
        // Fully qualified to prevent interference with rest.action.count.RestCountAction
        registerHandler.accept(new org.elasticsearch.rest.action.cat.RestCountAction(settings, restController));
        // Fully qualified to prevent interference with rest.action.indices.RestRecoveryAction
        registerHandler.accept(new org.elasticsearch.rest.action.cat.RestRecoveryAction(settings, restController));
        registerHandler.accept(new RestHealthAction(settings, restController));
        registerHandler.accept(new org.elasticsearch.rest.action.cat.RestPendingClusterTasksAction(settings, restController));
        registerHandler.accept(new RestAliasAction(settings, restController));
        registerHandler.accept(new RestThreadPoolAction(settings, restController));
        registerHandler.accept(new RestPluginsAction(settings, restController));
        registerHandler.accept(new RestFielddataAction(settings, restController));
        registerHandler.accept(new RestNodeAttrsAction(settings, restController));
        registerHandler.accept(new RestRepositoriesAction(settings, restController));
        registerHandler.accept(new RestSnapshotAction(settings, restController));
        registerHandler.accept(new RestTemplatesAction(settings, restController));
        for (ActionPlugin plugin : actionPlugins) {
            for (RestHandler handler : plugin.getRestHandlers(settings, restController, clusterSettings, indexScopedSettings,
                    settingsFilter, indexNameExpressionResolver, nodesInCluster)) {
                registerHandler.accept(handler);
            }
        }
        registerHandler.accept(new RestCatAction(settings, restController, catActions));
    }

    @Override
    protected void configure() {
        bind(ActionFilters.class).toInstance(actionFilters);
        bind(DestructiveOperations.class).toInstance(destructiveOperations);

        if (false == transportClient) {
            // Supporting classes only used when not a transport client
            bind(AutoCreateIndex.class).toInstance(autoCreateIndex);
            bind(TransportLivenessAction.class).asEagerSingleton();

            // register GenericAction -> transportAction Map used by NodeClient
            @SuppressWarnings("rawtypes")
            MapBinder<GenericAction, TransportAction> transportActionsBinder
                    = MapBinder.newMapBinder(binder(), GenericAction.class, TransportAction.class);
            for (ActionHandler<?, ?> action : actionRegistrar.getActions().values()) {
                // bind the action as eager singleton, so the map binder one will reuse it
                bind(action.getTransportAction()).asEagerSingleton();
                transportActionsBinder.addBinding(action.getAction()).to(action.getTransportAction()).asEagerSingleton();
                for (Class<?> supportAction : action.getSupportTransportActions()) {
                    bind(supportAction).asEagerSingleton();
                }
            }
        }
    }

    public ActionFilters getActionFilters() {
        return actionFilters;
    }

    public RestController getRestController() {
        return restController;
    }
}
