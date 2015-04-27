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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsAction;
import org.elasticsearch.action.admin.cluster.node.hotthreads.TransportNodesHotThreadsAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.info.TransportNodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.liveness.TransportLivenessAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.delete.TransportDeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesAction;
import org.elasticsearch.action.admin.cluster.repositories.get.TransportGetRepositoriesAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.verify.TransportVerifyRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsAction;
import org.elasticsearch.action.admin.cluster.shards.TransportClusterSearchShardsAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.TransportDeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.TransportGetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.TransportRestoreSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusAction;
import org.elasticsearch.action.admin.cluster.snapshots.status.TransportSnapshotsStatusAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.TransportClusterStateAction;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsAction;
import org.elasticsearch.action.admin.cluster.stats.TransportClusterStatsAction;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksAction;
import org.elasticsearch.action.admin.cluster.tasks.TransportPendingClusterTasksAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistAction;
import org.elasticsearch.action.admin.indices.alias.exists.TransportAliasesExistAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.TransportGetAliasesAction;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.analyze.TransportAnalyzeAction;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheAction;
import org.elasticsearch.action.admin.indices.cache.clear.TransportClearIndicesCacheAction;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.close.TransportCloseIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.indices.TransportIndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.types.TransportTypesExistsAction;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsAction;
import org.elasticsearch.action.admin.indices.flush.FlushAction;
import org.elasticsearch.action.admin.indices.flush.TransportFlushAction;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.TransportGetIndexAction;
import org.elasticsearch.action.admin.indices.mapping.get.*;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.admin.indices.open.TransportOpenIndexAction;
import org.elasticsearch.action.admin.indices.optimize.OptimizeAction;
import org.elasticsearch.action.admin.indices.optimize.TransportOptimizeAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.recovery.TransportRecoveryAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.TransportRefreshAction;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsAction;
import org.elasticsearch.action.admin.indices.segments.TransportIndicesSegmentsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.TransportGetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.TransportIndicesStatsAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.get.TransportGetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.action.admin.indices.validate.query.TransportValidateQueryAction;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.elasticsearch.action.admin.indices.warmer.delete.DeleteWarmerAction;
import org.elasticsearch.action.admin.indices.warmer.delete.TransportDeleteWarmerAction;
import org.elasticsearch.action.admin.indices.warmer.get.GetWarmersAction;
import org.elasticsearch.action.admin.indices.warmer.get.TransportGetWarmersAction;
import org.elasticsearch.action.admin.indices.warmer.put.PutWarmerAction;
import org.elasticsearch.action.admin.indices.warmer.put.TransportPutWarmerAction;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.count.CountAction;
import org.elasticsearch.action.count.TransportCountAction;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.TransportDeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.TransportIndexDeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.TransportShardDeleteByQueryAction;
import org.elasticsearch.action.exists.ExistsAction;
import org.elasticsearch.action.exists.TransportExistsAction;
import org.elasticsearch.action.explain.ExplainAction;
import org.elasticsearch.action.explain.TransportExplainAction;
import org.elasticsearch.action.fieldstats.FieldStatsAction;
import org.elasticsearch.action.fieldstats.TransportFieldStatsTransportAction;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptAction;
import org.elasticsearch.action.indexedscripts.delete.TransportDeleteIndexedScriptAction;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptAction;
import org.elasticsearch.action.indexedscripts.get.TransportGetIndexedScriptAction;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptAction;
import org.elasticsearch.action.indexedscripts.put.TransportPutIndexedScriptAction;
import org.elasticsearch.action.mlt.MoreLikeThisAction;
import org.elasticsearch.action.mlt.TransportMoreLikeThisAction;
import org.elasticsearch.action.percolate.*;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.search.type.*;
import org.elasticsearch.action.suggest.SuggestAction;
import org.elasticsearch.action.suggest.TransportSuggestAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.termvectors.*;
import org.elasticsearch.action.termvectors.dfs.TransportDfsOnlyAction;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.inject.multibindings.Multibinder;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class ActionModule extends AbstractModule {

    private final Map<String, ActionEntry> actions = Maps.newHashMap();
    private final List<Class<? extends ActionFilter>> actionFilters = Lists.newArrayList();

    static class ActionEntry<Request extends ActionRequest, Response extends ActionResponse> {
        public final GenericAction<Request, Response> action;
        public final Class<? extends TransportAction<Request, Response>> transportAction;
        public final Class[] supportTransportActions;

        ActionEntry(GenericAction<Request, Response> action, Class<? extends TransportAction<Request, Response>> transportAction, Class... supportTransportActions) {
            this.action = action;
            this.transportAction = transportAction;
            this.supportTransportActions = supportTransportActions;
        }


    }

    private final boolean proxy;

    public ActionModule(boolean proxy) {
        this.proxy = proxy;
    }

    /**
     * Registers an action.
     *
     * @param action                  The action type.
     * @param transportAction         The transport action implementing the actual action.
     * @param supportTransportActions Any support actions that are needed by the transport action.
     * @param <Request>               The request type.
     * @param <Response>              The response type.
     */
    public <Request extends ActionRequest, Response extends ActionResponse> void registerAction(GenericAction<Request, Response> action, Class<? extends TransportAction<Request, Response>> transportAction, Class... supportTransportActions) {
        actions.put(action.name(), new ActionEntry<>(action, transportAction, supportTransportActions));
    }

    public ActionModule registerFilter(Class<? extends ActionFilter> actionFilter) {
        actionFilters.add(actionFilter);
        return this;
    }

    @Override
    protected void configure() {

        Multibinder<ActionFilter> actionFilterMultibinder = Multibinder.newSetBinder(binder(), ActionFilter.class);
        for (Class<? extends ActionFilter> actionFilter : actionFilters) {
            actionFilterMultibinder.addBinding().to(actionFilter);
        }
        bind(ActionFilters.class).asEagerSingleton();
        registerAction(NodesInfoAction.INSTANCE, TransportNodesInfoAction.class);
        registerAction(NodesStatsAction.INSTANCE, TransportNodesStatsAction.class);
        registerAction(NodesHotThreadsAction.INSTANCE, TransportNodesHotThreadsAction.class);

        registerAction(ClusterStatsAction.INSTANCE, TransportClusterStatsAction.class);
        registerAction(ClusterStateAction.INSTANCE, TransportClusterStateAction.class);
        registerAction(ClusterHealthAction.INSTANCE, TransportClusterHealthAction.class);
        registerAction(ClusterUpdateSettingsAction.INSTANCE, TransportClusterUpdateSettingsAction.class);
        registerAction(ClusterRerouteAction.INSTANCE, TransportClusterRerouteAction.class);
        registerAction(ClusterSearchShardsAction.INSTANCE, TransportClusterSearchShardsAction.class);
        registerAction(PendingClusterTasksAction.INSTANCE, TransportPendingClusterTasksAction.class);
        registerAction(PutRepositoryAction.INSTANCE, TransportPutRepositoryAction.class);
        registerAction(GetRepositoriesAction.INSTANCE, TransportGetRepositoriesAction.class);
        registerAction(DeleteRepositoryAction.INSTANCE, TransportDeleteRepositoryAction.class);
        registerAction(VerifyRepositoryAction.INSTANCE, TransportVerifyRepositoryAction.class);
        registerAction(GetSnapshotsAction.INSTANCE, TransportGetSnapshotsAction.class);
        registerAction(DeleteSnapshotAction.INSTANCE, TransportDeleteSnapshotAction.class);
        registerAction(CreateSnapshotAction.INSTANCE, TransportCreateSnapshotAction.class);
        registerAction(RestoreSnapshotAction.INSTANCE, TransportRestoreSnapshotAction.class);
        registerAction(SnapshotsStatusAction.INSTANCE, TransportSnapshotsStatusAction.class);

        registerAction(IndicesStatsAction.INSTANCE, TransportIndicesStatsAction.class);
        registerAction(IndicesSegmentsAction.INSTANCE, TransportIndicesSegmentsAction.class);
        registerAction(CreateIndexAction.INSTANCE, TransportCreateIndexAction.class);
        registerAction(DeleteIndexAction.INSTANCE, TransportDeleteIndexAction.class);
        registerAction(GetIndexAction.INSTANCE, TransportGetIndexAction.class);
        registerAction(OpenIndexAction.INSTANCE, TransportOpenIndexAction.class);
        registerAction(CloseIndexAction.INSTANCE, TransportCloseIndexAction.class);
        registerAction(IndicesExistsAction.INSTANCE, TransportIndicesExistsAction.class);
        registerAction(TypesExistsAction.INSTANCE, TransportTypesExistsAction.class);
        registerAction(GetMappingsAction.INSTANCE, TransportGetMappingsAction.class);
        registerAction(GetFieldMappingsAction.INSTANCE, TransportGetFieldMappingsAction.class, TransportGetFieldMappingsIndexAction.class);
        registerAction(PutMappingAction.INSTANCE, TransportPutMappingAction.class);
        registerAction(IndicesAliasesAction.INSTANCE, TransportIndicesAliasesAction.class);
        registerAction(UpdateSettingsAction.INSTANCE, TransportUpdateSettingsAction.class);
        registerAction(AnalyzeAction.INSTANCE, TransportAnalyzeAction.class);
        registerAction(PutIndexTemplateAction.INSTANCE, TransportPutIndexTemplateAction.class);
        registerAction(GetIndexTemplatesAction.INSTANCE, TransportGetIndexTemplatesAction.class);
        registerAction(DeleteIndexTemplateAction.INSTANCE, TransportDeleteIndexTemplateAction.class);
        registerAction(ValidateQueryAction.INSTANCE, TransportValidateQueryAction.class);
        registerAction(RefreshAction.INSTANCE, TransportRefreshAction.class);
        registerAction(FlushAction.INSTANCE, TransportFlushAction.class);
        registerAction(OptimizeAction.INSTANCE, TransportOptimizeAction.class);
        registerAction(ClearIndicesCacheAction.INSTANCE, TransportClearIndicesCacheAction.class);
        registerAction(PutWarmerAction.INSTANCE, TransportPutWarmerAction.class);
        registerAction(DeleteWarmerAction.INSTANCE, TransportDeleteWarmerAction.class);
        registerAction(GetWarmersAction.INSTANCE, TransportGetWarmersAction.class);
        registerAction(GetAliasesAction.INSTANCE, TransportGetAliasesAction.class);
        registerAction(AliasesExistAction.INSTANCE, TransportAliasesExistAction.class);
        registerAction(GetSettingsAction.INSTANCE, TransportGetSettingsAction.class);

        registerAction(IndexAction.INSTANCE, TransportIndexAction.class);
        registerAction(GetAction.INSTANCE, TransportGetAction.class);
        registerAction(TermVectorsAction.INSTANCE, TransportTermVectorsAction.class,
                TransportDfsOnlyAction.class);
        registerAction(MultiTermVectorsAction.INSTANCE, TransportMultiTermVectorsAction.class,
                TransportShardMultiTermsVectorAction.class);
        registerAction(DeleteAction.INSTANCE, TransportDeleteAction.class);
        registerAction(CountAction.INSTANCE, TransportCountAction.class);
        registerAction(ExistsAction.INSTANCE, TransportExistsAction.class);
        registerAction(SuggestAction.INSTANCE, TransportSuggestAction.class);
        registerAction(UpdateAction.INSTANCE, TransportUpdateAction.class);
        registerAction(MultiGetAction.INSTANCE, TransportMultiGetAction.class,
                TransportShardMultiGetAction.class);
        registerAction(BulkAction.INSTANCE, TransportBulkAction.class,
                TransportShardBulkAction.class);
        registerAction(DeleteByQueryAction.INSTANCE, TransportDeleteByQueryAction.class,
                TransportIndexDeleteByQueryAction.class, TransportShardDeleteByQueryAction.class);
        registerAction(SearchAction.INSTANCE, TransportSearchAction.class,
                TransportSearchDfsQueryThenFetchAction.class,
                TransportSearchQueryThenFetchAction.class,
                TransportSearchDfsQueryAndFetchAction.class,
                TransportSearchQueryAndFetchAction.class,
                TransportSearchScanAction.class
        );
        registerAction(SearchScrollAction.INSTANCE, TransportSearchScrollAction.class,
                TransportSearchScrollScanAction.class,
                TransportSearchScrollQueryThenFetchAction.class,
                TransportSearchScrollQueryAndFetchAction.class
        );
        registerAction(MultiSearchAction.INSTANCE, TransportMultiSearchAction.class);
        registerAction(MoreLikeThisAction.INSTANCE, TransportMoreLikeThisAction.class);
        registerAction(PercolateAction.INSTANCE, TransportPercolateAction.class);
        registerAction(MultiPercolateAction.INSTANCE, TransportMultiPercolateAction.class, TransportShardMultiPercolateAction.class);
        registerAction(ExplainAction.INSTANCE, TransportExplainAction.class);
        registerAction(ClearScrollAction.INSTANCE, TransportClearScrollAction.class);
        registerAction(RecoveryAction.INSTANCE, TransportRecoveryAction.class);

        //Indexed scripts
        registerAction(PutIndexedScriptAction.INSTANCE, TransportPutIndexedScriptAction.class);
        registerAction(GetIndexedScriptAction.INSTANCE, TransportGetIndexedScriptAction.class);
        registerAction(DeleteIndexedScriptAction.INSTANCE, TransportDeleteIndexedScriptAction.class);

        registerAction(FieldStatsAction.INSTANCE, TransportFieldStatsTransportAction.class);

        // register Name -> GenericAction Map that can be injected to instances.
        MapBinder<String, GenericAction> actionsBinder
                = MapBinder.newMapBinder(binder(), String.class, GenericAction.class);

        for (Map.Entry<String, ActionEntry> entry : actions.entrySet()) {
            actionsBinder.addBinding(entry.getKey()).toInstance(entry.getValue().action);
        }
        // register GenericAction -> transportAction Map that can be injected to instances.
        // also register any supporting classes
        if (!proxy) {
            bind(TransportLivenessAction.class).asEagerSingleton();
            MapBinder<GenericAction, TransportAction> transportActionsBinder
                    = MapBinder.newMapBinder(binder(), GenericAction.class, TransportAction.class);
            for (Map.Entry<String, ActionEntry> entry : actions.entrySet()) {
                // bind the action as eager singleton, so the map binder one will reuse it
                bind(entry.getValue().transportAction).asEagerSingleton();
                transportActionsBinder.addBinding(entry.getValue().action).to(entry.getValue().transportAction).asEagerSingleton();
                for (Class supportAction : entry.getValue().supportTransportActions) {
                    bind(supportAction).asEagerSingleton();
                }
            }
        }
    }
}
