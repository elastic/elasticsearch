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

package org.elasticsearch.rest;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class RestModule extends AbstractModule {
    private final Map<Class<? extends BaseRestHandler>, Function<RestGlobalContext, ? extends BaseRestHandler>> actions = new HashMap<>();

    private final Settings settings;

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

    public RestModule(Settings settings, Version version) {
        this.settings = settings;

        add(RestMainAction.class, c -> new RestMainAction(c, version, clusterName, clusterService));
        add(RestAnalyzeAction.class, RestAnalyzeAction::new);
        add(RestFieldStatsAction.class, RestFieldStatsAction::new);
        add(RestSuggestAction.class, RestSuggestAction::new);
        add(RestTypesExistsAction.class, RestTypesExistsAction::new);

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

    public <T extends BaseRestHandler> void add(Class<T> type, Function<RestGlobalContext, T> builder) {
        requireNonNull(type, "Must define the handler type being registered");
        requireNonNull(builder, "Must define the builder to register");
        Object old = actions.put(type, builder);
        if (old != null) {
            throw new IllegalArgumentException("Action [" + type + "] is already defined.");
        }
    }

    @Override
    protected void configure(RestModule this) {
        /*
         * This is our shim to handle our dependencies not being non-guiced.
         * Remove this when the dependencies can be passed to the constructor.
         */
        binder().requestInjection(this);

        // Another temporary shim to work around half removing things from
        // NetworkModule:
        bind(RestController.class).asEagerSingleton();
    }

    @Inject
    public void configure(RestController controller, Client client, IndicesQueriesRegistry indicesQueriesRegistry,
            IndexNameExpressionResolver indexNameExpressionResolver, SettingsFilter settingsFilter, ClusterSettings clusterSettings,
            ClusterName clusterName, ClusterService clusterService) {
        // RestController controller = new RestController(settings);
        this.clusterSettings = clusterSettings;
        this.clusterName = clusterName;
        this.clusterService = clusterService;
        RestGlobalContext context = new RestGlobalContext(settings, controller, client, indicesQueriesRegistry, indexNameExpressionResolver,
                settingsFilter);
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
        }
        new RestCatAction(context, catActions);
    }

    private void registerAliasActions() {
        add(RestAliasesExistAction.class, RestAliasesExistAction::new);
        add(RestCreateIndexAction.class, RestCreateIndexAction::new);
        add(RestDeleteIndexAction.class, RestDeleteIndexAction::new);
        add(RestGetAliasesAction.class, RestGetAliasesAction::new);
        add(RestGetIndicesAliasesAction.class, RestGetIndicesAliasesAction::new);
        add(RestIndexDeleteAliasesAction.class, RestIndexDeleteAliasesAction::new);
        add(RestIndexPutAliasAction.class, RestIndexPutAliasAction::new);
        add(RestIndicesAliasesAction.class, RestIndicesAliasesAction::new);
    }

    private void registerCatActions() {
        add(RestAliasAction.class, RestAliasAction::new);
        add(RestAllocationAction.class, RestAllocationAction::new);
        add(RestCountCatAction.class, RestCountCatAction::new);
        add(RestFielddataAction.class, RestFielddataAction::new);
        add(RestHealthAction.class, RestHealthAction::new);
        add(RestIndicesAction.class, RestIndicesAction::new);
        add(RestMasterAction.class, RestMasterAction::new);
        add(RestNodeAttrsAction.class, RestNodeAttrsAction::new);
        add(RestNodesAction.class, RestNodesAction::new);
        add(RestPendingClusterTasksCatAction.class, RestPendingClusterTasksCatAction::new);
        add(RestPluginsAction.class, RestPluginsAction::new);
        add(RestRecoveryCatAction.class, RestRecoveryCatAction::new);
        add(RestRepositoriesAction.class, RestRepositoriesAction::new);
        add(RestSegmentsAction.class, RestSegmentsAction::new);
        add(RestShardsAction.class, RestShardsAction::new);
        add(RestSnapshotAction.class, RestSnapshotAction::new);
        add(RestThreadPoolAction.class, RestThreadPoolAction::new);
    }

    private void registerClusterActions() {
        add(RestClusterHealthAction.class, RestClusterHealthAction::new);
        add(RestClusterRerouteAction.class, RestClusterRerouteAction::new);
        add(RestClusterSearchShardsAction.class, RestClusterSearchShardsAction::new);
        add(RestClusterStateAction.class, RestClusterStateAction::new);
        add(RestClusterStatsAction.class, RestClusterStatsAction::new);
        add(RestClusterGetSettingsAction.class, c -> new RestClusterGetSettingsAction(c, clusterSettings));
        add(RestClusterUpdateSettingsAction.class, RestClusterUpdateSettingsAction::new);
        add(RestPendingClusterTasksAction.class, RestPendingClusterTasksAction::new);
    }

    private void registerDocumentActions() {
        add(RestBulkAction.class, RestBulkAction::new);
        add(RestCountAction.class, RestCountAction::new);
        add(RestDeleteAction.class, RestDeleteAction::new);
        add(RestGetAction.class, RestGetAction::new);
        add(RestGetSourceAction.class, RestGetSourceAction::new);
        add(RestHeadAction.class, RestHeadAction::new);
        add(RestIndexAction.class, RestIndexAction::new);
        add(RestMultiGetAction.class, RestMultiGetAction::new);
        add(RestMultiTermVectorsAction.class, RestMultiTermVectorsAction::new);
        add(RestTermVectorsAction.class, RestTermVectorsAction::new);
        add(RestUpdateAction.class, RestUpdateAction::new);
    }

    private void registerIndexActions() {
        add(RestCloseIndexAction.class, RestCloseIndexAction::new);
        add(RestClearIndicesCacheAction.class, RestClearIndicesCacheAction::new);
        add(RestFlushAction.class, RestFlushAction::new);
        add(RestForceMergeAction.class, RestForceMergeAction::new);
        add(RestGetIndicesAction.class, RestGetIndicesAction::new);
        add(RestIndicesExistsAction.class, RestIndicesExistsAction::new);
        add(RestIndicesSegmentsAction.class, RestIndicesSegmentsAction::new);
        add(RestIndicesShardStoresAction.class, RestIndicesShardStoresAction::new);
        add(RestIndicesStatsAction.class, RestIndicesStatsAction::new);
        add(RestOpenIndexAction.class, RestOpenIndexAction::new);
        add(RestRecoveryAction.class, RestRecoveryAction::new);
        add(RestRefreshAction.class, RestRefreshAction::new);
        add(RestSyncedFlushAction.class, RestSyncedFlushAction::new);
        add(RestUpgradeAction.class, RestUpgradeAction::new);
    }

    private void registerIndexTemplateActions() {
        add(RestDeleteIndexTemplateAction.class, RestDeleteIndexTemplateAction::new);
        add(RestGetIndexTemplateAction.class, RestGetIndexTemplateAction::new);
        add(RestHeadIndexTemplateAction.class, RestHeadIndexTemplateAction::new);
        add(RestPutIndexTemplateAction.class, RestPutIndexTemplateAction::new);
    }

    private void registerIndexedScriptsActions() {
        add(RestDeleteIndexedScriptAction.class, RestDeleteIndexedScriptAction::new);
        add(RestGetIndexedScriptAction.class, RestGetIndexedScriptAction::new);
        add(RestPutIndexedScriptAction.class, RestPutIndexedScriptAction::new);
    }

    private void registerMappingActions() {
        add(RestGetFieldMappingAction.class, RestGetFieldMappingAction::new);
        add(RestGetMappingAction.class, RestGetMappingAction::new);
        add(RestPutMappingAction.class, RestPutMappingAction::new);
    }

    private void registerNodeActions() {
        add(RestNodesHotThreadsAction.class, RestNodesHotThreadsAction::new);
        add(RestNodesInfoAction.class, RestNodesInfoAction::new);
        add(RestNodesStatsAction.class, RestNodesStatsAction::new);
    }

    private void registerPercolatorActions() {
        add(RestPercolateAction.class, RestPercolateAction::new);
        add(RestMultiPercolateAction.class, RestMultiPercolateAction::new);
    }

    private void registerSettingsActions() {
        add(RestGetSettingsAction.class, RestGetSettingsAction::new);
        add(RestUpdateSettingsAction.class, RestUpdateSettingsAction::new);
    }

    private void registerSearchActions() {
        add(RestClearScrollAction.class, RestClearScrollAction::new);
        add(RestExplainAction.class, RestExplainAction::new);
        add(RestSearchAction.class, RestSearchAction::new);
        add(RestSearchScrollAction.class, RestSearchScrollAction::new);
        add(RestMultiSearchAction.class, RestMultiSearchAction::new);
        add(RestValidateQueryAction.class, RestValidateQueryAction::new);
    }

    private void registerSearchTemplateActions() {
        add(RestGetSearchTemplateAction.class, RestGetSearchTemplateAction::new);
        add(RestDeleteSearchTemplateAction.class, RestDeleteSearchTemplateAction::new);
        add(RestRenderSearchTemplateAction.class, RestRenderSearchTemplateAction::new);
        add(RestPutSearchTemplateAction.class, RestPutSearchTemplateAction::new);
    }

    private void registerSnapshotActions() {
        add(RestGetSnapshotsAction.class, RestGetSnapshotsAction::new);
        add(RestCreateSnapshotAction.class, RestCreateSnapshotAction::new);
        add(RestDeleteRepositoryAction.class, RestDeleteRepositoryAction::new);
        add(RestDeleteSnapshotAction.class, RestDeleteSnapshotAction::new);
        add(RestGetRepositoriesAction.class, RestGetRepositoriesAction::new);
        add(RestPutRepositoryAction.class, RestPutRepositoryAction::new);
        add(RestRestoreSnapshotAction.class, RestRestoreSnapshotAction::new);
        add(RestSnapshotsStatusAction.class, RestSnapshotsStatusAction::new);
        add(RestVerifyRepositoryAction.class, RestVerifyRepositoryAction::new);
    }
}
