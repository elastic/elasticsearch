/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.Maps;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsAction;
import org.elasticsearch.action.admin.cluster.node.hotthreads.TransportNodesHotThreadsAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.info.TransportNodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.restart.NodesRestartAction;
import org.elasticsearch.action.admin.cluster.node.restart.TransportNodesRestartAction;
import org.elasticsearch.action.admin.cluster.node.shutdown.NodesShutdownAction;
import org.elasticsearch.action.admin.cluster.node.shutdown.TransportNodesShutdownAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsAction;
import org.elasticsearch.action.admin.cluster.shards.TransportClusterSearchShardsAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.TransportClusterStateAction;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksAction;
import org.elasticsearch.action.admin.cluster.tasks.TransportPendingClusterTasksAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistAction;
import org.elasticsearch.action.admin.indices.alias.exists.TransportAliasesExistAction;
import org.elasticsearch.action.admin.indices.alias.get.IndicesGetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.TransportIndicesGetAliasesAction;
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
import org.elasticsearch.action.admin.indices.gateway.snapshot.GatewaySnapshotAction;
import org.elasticsearch.action.admin.indices.gateway.snapshot.TransportGatewaySnapshotAction;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingAction;
import org.elasticsearch.action.admin.indices.mapping.delete.TransportDeleteMappingAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.TransportGetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.admin.indices.open.TransportOpenIndexAction;
import org.elasticsearch.action.admin.indices.optimize.OptimizeAction;
import org.elasticsearch.action.admin.indices.optimize.TransportOptimizeAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.TransportRefreshAction;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsAction;
import org.elasticsearch.action.admin.indices.segments.TransportIndicesSegmentsAction;
import org.elasticsearch.action.admin.indices.settings.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.TransportIndicesStatsAction;
import org.elasticsearch.action.admin.indices.status.IndicesStatusAction;
import org.elasticsearch.action.admin.indices.status.TransportIndicesStatusAction;
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
import org.elasticsearch.action.admin.indices.warmer.put.PutWarmerAction;
import org.elasticsearch.action.admin.indices.warmer.put.TransportPutWarmerAction;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.count.CountAction;
import org.elasticsearch.action.count.TransportCountAction;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.delete.index.TransportIndexDeleteAction;
import org.elasticsearch.action.delete.index.TransportShardDeleteAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.TransportDeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.TransportIndexDeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.TransportShardDeleteByQueryAction;
import org.elasticsearch.action.explain.ExplainAction;
import org.elasticsearch.action.explain.TransportExplainAction;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.mlt.MoreLikeThisAction;
import org.elasticsearch.action.mlt.TransportMoreLikeThisAction;
import org.elasticsearch.action.percolate.PercolateAction;
import org.elasticsearch.action.percolate.TransportPercolateAction;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.search.type.*;
import org.elasticsearch.action.suggest.SuggestAction;
import org.elasticsearch.action.suggest.TransportSuggestAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import java.util.Map;

/**
 *
 */
public class ActionModule extends AbstractModule {

    private final Map<String, ActionEntry> actions = Maps.newHashMap();

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
        actions.put(action.name(), new ActionEntry<Request, Response>(action, transportAction, supportTransportActions));
    }

    @Override
    protected void configure() {

        registerAction(NodesInfoAction.INSTANCE, TransportNodesInfoAction.class);
        registerAction(NodesStatsAction.INSTANCE, TransportNodesStatsAction.class);
        registerAction(NodesShutdownAction.INSTANCE, TransportNodesShutdownAction.class);
        registerAction(NodesRestartAction.INSTANCE, TransportNodesRestartAction.class);
        registerAction(NodesHotThreadsAction.INSTANCE, TransportNodesHotThreadsAction.class);

        registerAction(ClusterStateAction.INSTANCE, TransportClusterStateAction.class);
        registerAction(ClusterHealthAction.INSTANCE, TransportClusterHealthAction.class);
        registerAction(ClusterUpdateSettingsAction.INSTANCE, TransportClusterUpdateSettingsAction.class);
        registerAction(ClusterRerouteAction.INSTANCE, TransportClusterRerouteAction.class);
        registerAction(ClusterSearchShardsAction.INSTANCE, TransportClusterSearchShardsAction.class);
        registerAction(PendingClusterTasksAction.INSTANCE, TransportPendingClusterTasksAction.class);

        registerAction(IndicesStatsAction.INSTANCE, TransportIndicesStatsAction.class);
        registerAction(IndicesStatusAction.INSTANCE, TransportIndicesStatusAction.class);
        registerAction(IndicesSegmentsAction.INSTANCE, TransportIndicesSegmentsAction.class);
        registerAction(CreateIndexAction.INSTANCE, TransportCreateIndexAction.class);
        registerAction(DeleteIndexAction.INSTANCE, TransportDeleteIndexAction.class);
        registerAction(OpenIndexAction.INSTANCE, TransportOpenIndexAction.class);
        registerAction(CloseIndexAction.INSTANCE, TransportCloseIndexAction.class);
        registerAction(IndicesExistsAction.INSTANCE, TransportIndicesExistsAction.class);
        registerAction(TypesExistsAction.INSTANCE, TransportTypesExistsAction.class);
        registerAction(GetFieldMappingsAction.INSTANCE, TransportGetFieldMappingsAction.class);
        registerAction(PutMappingAction.INSTANCE, TransportPutMappingAction.class);
        registerAction(DeleteMappingAction.INSTANCE, TransportDeleteMappingAction.class);
        registerAction(IndicesAliasesAction.INSTANCE, TransportIndicesAliasesAction.class);
        registerAction(UpdateSettingsAction.INSTANCE, TransportUpdateSettingsAction.class);
        registerAction(AnalyzeAction.INSTANCE, TransportAnalyzeAction.class);
        registerAction(PutIndexTemplateAction.INSTANCE, TransportPutIndexTemplateAction.class);
        registerAction(GetIndexTemplatesAction.INSTANCE, TransportGetIndexTemplatesAction.class);
        registerAction(DeleteIndexTemplateAction.INSTANCE, TransportDeleteIndexTemplateAction.class);
        registerAction(ValidateQueryAction.INSTANCE, TransportValidateQueryAction.class);
        registerAction(GatewaySnapshotAction.INSTANCE, TransportGatewaySnapshotAction.class);
        registerAction(RefreshAction.INSTANCE, TransportRefreshAction.class);
        registerAction(FlushAction.INSTANCE, TransportFlushAction.class);
        registerAction(OptimizeAction.INSTANCE, TransportOptimizeAction.class);
        registerAction(ClearIndicesCacheAction.INSTANCE, TransportClearIndicesCacheAction.class);
        registerAction(PutWarmerAction.INSTANCE, TransportPutWarmerAction.class);
        registerAction(DeleteWarmerAction.INSTANCE, TransportDeleteWarmerAction.class);
        registerAction(IndicesGetAliasesAction.INSTANCE, TransportIndicesGetAliasesAction.class);
        registerAction(AliasesExistAction.INSTANCE, TransportAliasesExistAction.class);

        registerAction(IndexAction.INSTANCE, TransportIndexAction.class);
        registerAction(GetAction.INSTANCE, TransportGetAction.class);
        registerAction(DeleteAction.INSTANCE, TransportDeleteAction.class,
                TransportIndexDeleteAction.class, TransportShardDeleteAction.class);
        registerAction(CountAction.INSTANCE, TransportCountAction.class);
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
        registerAction(ExplainAction.INSTANCE, TransportExplainAction.class);
        registerAction(ClearScrollAction.INSTANCE, TransportClearScrollAction.class);

        // register Name -> GenericAction Map that can be injected to instances.
        MapBinder<String, GenericAction> actionsBinder
                = MapBinder.newMapBinder(binder(), String.class, GenericAction.class);

        for (Map.Entry<String, ActionEntry> entry : actions.entrySet()) {
            actionsBinder.addBinding(entry.getKey()).toInstance(entry.getValue().action);
        }

        // register GenericAction -> transportAction Map that can be injected to instances.
        // also register any supporting classes
        if (!proxy) {
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
