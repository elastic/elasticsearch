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

import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainAction;
import org.elasticsearch.action.admin.cluster.allocation.TransportClusterAllocationExplainAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsAction;
import org.elasticsearch.action.admin.cluster.node.hotthreads.TransportNodesHotThreadsAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.info.TransportNodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.liveness.TransportLivenessAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskAction;
import org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
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
import org.elasticsearch.action.admin.cluster.snapshots.status.TransportNodesSnapshotsStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.TransportSnapshotsStatusAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.TransportClusterStateAction;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsAction;
import org.elasticsearch.action.admin.cluster.stats.TransportClusterStatsAction;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptAction;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptAction;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptAction;
import org.elasticsearch.action.admin.cluster.storedscripts.TransportDeleteStoredScriptAction;
import org.elasticsearch.action.admin.cluster.storedscripts.TransportGetStoredScriptAction;
import org.elasticsearch.action.admin.cluster.storedscripts.TransportPutStoredScriptAction;
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
import org.elasticsearch.action.admin.indices.flush.SyncedFlushAction;
import org.elasticsearch.action.admin.indices.flush.TransportFlushAction;
import org.elasticsearch.action.admin.indices.flush.TransportShardFlushAction;
import org.elasticsearch.action.admin.indices.flush.TransportSyncedFlushAction;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.elasticsearch.action.admin.indices.forcemerge.TransportForceMergeAction;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.TransportGetIndexAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.TransportGetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.TransportGetFieldMappingsIndexAction;
import org.elasticsearch.action.admin.indices.mapping.get.TransportGetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.admin.indices.open.TransportOpenIndexAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.recovery.TransportRecoveryAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.TransportRefreshAction;
import org.elasticsearch.action.admin.indices.refresh.TransportShardRefreshAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.rollover.TransportRolloverAction;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsAction;
import org.elasticsearch.action.admin.indices.segments.TransportIndicesSegmentsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.TransportGetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresAction;
import org.elasticsearch.action.admin.indices.shards.TransportIndicesShardStoresAction;
import org.elasticsearch.action.admin.indices.shrink.ShrinkAction;
import org.elasticsearch.action.admin.indices.shrink.TransportShrinkAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.TransportIndicesStatsAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.get.TransportGetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.action.admin.indices.upgrade.get.TransportUpgradeStatusAction;
import org.elasticsearch.action.admin.indices.upgrade.get.UpgradeStatusAction;
import org.elasticsearch.action.admin.indices.upgrade.post.TransportUpgradeAction;
import org.elasticsearch.action.admin.indices.upgrade.post.TransportUpgradeSettingsAction;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeAction;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeSettingsAction;
import org.elasticsearch.action.admin.indices.validate.query.TransportValidateQueryAction;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.explain.ExplainAction;
import org.elasticsearch.action.explain.TransportExplainAction;
import org.elasticsearch.action.fieldstats.FieldStatsAction;
import org.elasticsearch.action.fieldstats.TransportFieldStatsAction;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.get.TransportMultiGetAction;
import org.elasticsearch.action.get.TransportShardMultiGetAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.ingest.DeletePipelineAction;
import org.elasticsearch.action.ingest.DeletePipelineTransportAction;
import org.elasticsearch.action.ingest.GetPipelineAction;
import org.elasticsearch.action.ingest.GetPipelineTransportAction;
import org.elasticsearch.action.ingest.IngestActionFilter;
import org.elasticsearch.action.ingest.IngestProxyActionFilter;
import org.elasticsearch.action.ingest.PutPipelineAction;
import org.elasticsearch.action.ingest.PutPipelineTransportAction;
import org.elasticsearch.action.ingest.SimulatePipelineAction;
import org.elasticsearch.action.ingest.SimulatePipelineTransportAction;
import org.elasticsearch.action.main.MainAction;
import org.elasticsearch.action.main.TransportMainAction;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.TransportClearScrollAction;
import org.elasticsearch.action.search.TransportMultiSearchAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.search.TransportSearchScrollAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.termvectors.MultiTermVectorsAction;
import org.elasticsearch.action.termvectors.TermVectorsAction;
import org.elasticsearch.action.termvectors.TransportMultiTermVectorsAction;
import org.elasticsearch.action.termvectors.TransportShardMultiTermsVectorAction;
import org.elasticsearch.action.termvectors.TransportTermVectorsAction;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.NamedRegistry;
import org.elasticsearch.common.ReflectiveInstantiator;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

/**
 * Builds and binds the generic action map, all {@link TransportAction}s, and {@link ActionFilters}.
 */
public class ActionModule extends AbstractModule {

    private final NodeClient nodeClient;
    private final Settings settings;
    private final Map<String, ActionHandler<?, ?>> actions;
    private final List<Class<? extends ActionFilter>> actionFilters;
    private final AutoCreateIndex autoCreateIndex;
    private final DestructiveOperations destructiveOperations;

    public ActionModule(NodeClient nodeClient, boolean ingestEnabled, Settings settings, IndexNameExpressionResolver resolver,
            ClusterSettings clusterSettings, List<ActionPlugin> actionPlugins) {
        this.nodeClient = nodeClient;
        this.settings = settings;
        actions = setupActions(actionPlugins);
        actionFilters = setupActionFilters(actionPlugins, ingestEnabled);
        autoCreateIndex = nodeClient == null ? null : new AutoCreateIndex(settings, resolver);
        destructiveOperations = new DestructiveOperations(settings, clusterSettings);
    }

    private Map<String, ActionHandler<?, ?>> setupActions(List<ActionPlugin> actionPlugins) {
        // Subclass NamedRegistry for easy registration
        class ActionRegistry extends NamedRegistry<ActionHandler<?, ?>> {
            public ActionRegistry() {
                super("action");
            }

            public void register(ActionHandler<?, ?> handler) {
                register(handler.getAction().name(), handler);
            }

            public <Request extends ActionRequest<Request>, Response extends ActionResponse> void register(
                    GenericAction<Request, Response> action, Class<? extends TransportAction<Request, Response>> transportAction,
                    Class<?>... supportTransportActions) {
                register(new ActionHandler<>(action, transportAction, supportTransportActions));
            }
        }
        ActionRegistry actions = new ActionRegistry();

        actions.register(MainAction.INSTANCE, TransportMainAction.class);
        actions.register(NodesInfoAction.INSTANCE, TransportNodesInfoAction.class);
        actions.register(NodesStatsAction.INSTANCE, TransportNodesStatsAction.class);
        actions.register(NodesHotThreadsAction.INSTANCE, TransportNodesHotThreadsAction.class);
        actions.register(ListTasksAction.INSTANCE, TransportListTasksAction.class);
        actions.register(GetTaskAction.INSTANCE, TransportGetTaskAction.class);
        actions.register(CancelTasksAction.INSTANCE, TransportCancelTasksAction.class);

        actions.register(ClusterAllocationExplainAction.INSTANCE, TransportClusterAllocationExplainAction.class,
                TransportIndicesShardStoresAction.class);
        actions.register(ClusterStatsAction.INSTANCE, TransportClusterStatsAction.class);
        actions.register(ClusterStateAction.INSTANCE, TransportClusterStateAction.class);
        actions.register(ClusterHealthAction.INSTANCE, TransportClusterHealthAction.class);
        actions.register(ClusterUpdateSettingsAction.INSTANCE, TransportClusterUpdateSettingsAction.class);
        actions.register(ClusterRerouteAction.INSTANCE, TransportClusterRerouteAction.class);
        actions.register(ClusterSearchShardsAction.INSTANCE, TransportClusterSearchShardsAction.class);
        actions.register(PendingClusterTasksAction.INSTANCE, TransportPendingClusterTasksAction.class);
        actions.register(PutRepositoryAction.INSTANCE, TransportPutRepositoryAction.class);
        actions.register(GetRepositoriesAction.INSTANCE, TransportGetRepositoriesAction.class);
        actions.register(DeleteRepositoryAction.INSTANCE, TransportDeleteRepositoryAction.class);
        actions.register(VerifyRepositoryAction.INSTANCE, TransportVerifyRepositoryAction.class);
        actions.register(GetSnapshotsAction.INSTANCE, TransportGetSnapshotsAction.class);
        actions.register(DeleteSnapshotAction.INSTANCE, TransportDeleteSnapshotAction.class);
        actions.register(CreateSnapshotAction.INSTANCE, TransportCreateSnapshotAction.class);
        actions.register(RestoreSnapshotAction.INSTANCE, TransportRestoreSnapshotAction.class);
        actions.register(SnapshotsStatusAction.INSTANCE, TransportSnapshotsStatusAction.class, TransportNodesSnapshotsStatus.class);

        actions.register(IndicesStatsAction.INSTANCE, TransportIndicesStatsAction.class);
        actions.register(IndicesSegmentsAction.INSTANCE, TransportIndicesSegmentsAction.class);
        actions.register(IndicesShardStoresAction.INSTANCE, TransportIndicesShardStoresAction.class);
        actions.register(CreateIndexAction.INSTANCE, TransportCreateIndexAction.class);
        actions.register(ShrinkAction.INSTANCE, TransportShrinkAction.class);
        actions.register(RolloverAction.INSTANCE, TransportRolloverAction.class);
        actions.register(DeleteIndexAction.INSTANCE, TransportDeleteIndexAction.class);
        actions.register(GetIndexAction.INSTANCE, TransportGetIndexAction.class);
        actions.register(OpenIndexAction.INSTANCE, TransportOpenIndexAction.class);
        actions.register(CloseIndexAction.INSTANCE, TransportCloseIndexAction.class);
        actions.register(IndicesExistsAction.INSTANCE, TransportIndicesExistsAction.class);
        actions.register(TypesExistsAction.INSTANCE, TransportTypesExistsAction.class);
        actions.register(GetMappingsAction.INSTANCE, TransportGetMappingsAction.class);
        actions.register(GetFieldMappingsAction.INSTANCE, TransportGetFieldMappingsAction.class,
                TransportGetFieldMappingsIndexAction.class);
        actions.register(PutMappingAction.INSTANCE, TransportPutMappingAction.class);
        actions.register(IndicesAliasesAction.INSTANCE, TransportIndicesAliasesAction.class);
        actions.register(UpdateSettingsAction.INSTANCE, TransportUpdateSettingsAction.class);
        actions.register(AnalyzeAction.INSTANCE, TransportAnalyzeAction.class);
        actions.register(PutIndexTemplateAction.INSTANCE, TransportPutIndexTemplateAction.class);
        actions.register(GetIndexTemplatesAction.INSTANCE, TransportGetIndexTemplatesAction.class);
        actions.register(DeleteIndexTemplateAction.INSTANCE, TransportDeleteIndexTemplateAction.class);
        actions.register(ValidateQueryAction.INSTANCE, TransportValidateQueryAction.class);
        actions.register(RefreshAction.INSTANCE, TransportRefreshAction.class, TransportShardRefreshAction.class);
        actions.register(FlushAction.INSTANCE, TransportFlushAction.class, TransportShardFlushAction.class);
        actions.register(SyncedFlushAction.INSTANCE, TransportSyncedFlushAction.class);
        actions.register(ForceMergeAction.INSTANCE, TransportForceMergeAction.class);
        actions.register(UpgradeAction.INSTANCE, TransportUpgradeAction.class, TransportUpgradeSettingsAction.class);
        actions.register(UpgradeStatusAction.INSTANCE, TransportUpgradeStatusAction.class);
        actions.register(UpgradeSettingsAction.INSTANCE, TransportUpgradeSettingsAction.class);
        actions.register(ClearIndicesCacheAction.INSTANCE, TransportClearIndicesCacheAction.class);
        actions.register(GetAliasesAction.INSTANCE, TransportGetAliasesAction.class);
        actions.register(AliasesExistAction.INSTANCE, TransportAliasesExistAction.class);
        actions.register(GetSettingsAction.INSTANCE, TransportGetSettingsAction.class);

        actions.register(IndexAction.INSTANCE, TransportIndexAction.class, TransportCreateIndexAction.class);
        actions.register(GetAction.INSTANCE, TransportGetAction.class);
        actions.register(TermVectorsAction.INSTANCE, TransportTermVectorsAction.class);
        actions.register(MultiTermVectorsAction.INSTANCE, TransportMultiTermVectorsAction.class,
                TransportShardMultiTermsVectorAction.class);
        actions.register(DeleteAction.INSTANCE, TransportDeleteAction.class);
        actions.register(UpdateAction.INSTANCE, TransportUpdateAction.class, TransportIndexAction.class,
                TransportDeleteAction.class, TransportCreateIndexAction.class);
        actions.register(MultiGetAction.INSTANCE, TransportMultiGetAction.class,
                TransportShardMultiGetAction.class);
        actions.register(BulkAction.INSTANCE, TransportBulkAction.class, TransportShardBulkAction.class, TransportCreateIndexAction.class);
        actions.register(SearchAction.INSTANCE, TransportSearchAction.class);
        actions.register(SearchScrollAction.INSTANCE, TransportSearchScrollAction.class);
        actions.register(MultiSearchAction.INSTANCE, TransportMultiSearchAction.class, TransportSearchAction.class);
        actions.register(ExplainAction.INSTANCE, TransportExplainAction.class);
        actions.register(ClearScrollAction.INSTANCE, TransportClearScrollAction.class);
        actions.register(RecoveryAction.INSTANCE, TransportRecoveryAction.class);

        //Indexed scripts
        actions.register(PutStoredScriptAction.INSTANCE, TransportPutStoredScriptAction.class);
        actions.register(GetStoredScriptAction.INSTANCE, TransportGetStoredScriptAction.class);
        actions.register(DeleteStoredScriptAction.INSTANCE, TransportDeleteStoredScriptAction.class);

        actions.register(FieldStatsAction.INSTANCE, TransportFieldStatsAction.class);

        actions.register(PutPipelineAction.INSTANCE, PutPipelineTransportAction.class, TransportNodesInfoAction.class);
        actions.register(GetPipelineAction.INSTANCE, GetPipelineTransportAction.class);
        actions.register(DeletePipelineAction.INSTANCE, DeletePipelineTransportAction.class);
        actions.register(SimulatePipelineAction.INSTANCE, SimulatePipelineTransportAction.class);

        actionPlugins.stream().flatMap(p -> p.getActions().stream()).forEach(actions::register);

        return unmodifiableMap(actions.getRegistry());
    }

    private List<Class<? extends ActionFilter>> setupActionFilters(List<ActionPlugin> actionPlugins,
            boolean ingestEnabled) {
        List<Class<? extends ActionFilter>> filters = new ArrayList<>();
        if (nodeClient != null) {
            if (ingestEnabled) {
                filters.add(IngestActionFilter.class);
            } else {
                filters.add(IngestProxyActionFilter.class);
            }
        }

        for (ActionPlugin plugin : actionPlugins) {
            filters.addAll(plugin.getActionFilters());
        }
        return unmodifiableList(filters);
    }

    public void buildActions(ReflectiveInstantiator instantiator) {
        instantiator.addCtorArg(Client.class, nodeClient);
        instantiator.addCtorArg(autoCreateIndex);
        instantiator.addCtorArg(destructiveOperations);
        instantiator.addCtorArg(settings);

        // ShardStateAction is used by so many things we just give everything access to it
        instantiator.addCtorArg(instantiator.instantiate(ShardStateAction.class, emptySet()));
        @SuppressWarnings("rawtypes")
        Map<GenericAction, TransportAction> transportActions = new HashMap<>();
        Set<Type> reuseOk = new HashSet<>();
        for (ActionHandler<?, ?> handler : actions.values()) {
            ReflectiveInstantiator handlerInstantiator = instantiator;
            if (handler.getSupportTransportActions().length > 0) {
                // Build the support actions from the end of the array to the start
                for (int s = handler.getSupportTransportActions().length - 1; s >= 0; s--) {
                    Class<?> supportAction = handler.getSupportTransportActions()[s];
                    instantiator.instantiate(supportAction, reuseOk);
                    reuseOk.add(supportAction);
                }
            }
            transportActions.put(handler.getAction(), handlerInstantiator.instantiate(handler.getTransportAction(), reuseOk));
            reuseOk.clear();
        }
        nodeClient.initialize(transportActions);

        instantiator.instantiate(TransportLivenessAction.class, emptySet());
    }

    @Override
    protected void configure() {
        Multibinder<ActionFilter> actionFilterMultibinder = Multibinder.newSetBinder(binder(), ActionFilter.class);
        for (Class<? extends ActionFilter> actionFilter : actionFilters) {
            actionFilterMultibinder.addBinding().to(actionFilter);
        }
        bind(ActionFilters.class).asEagerSingleton();

        // register Name -> GenericAction Map that can be injected to instances.
        @SuppressWarnings("rawtypes")
        MapBinder<String, GenericAction> actionsBinder
                = MapBinder.newMapBinder(binder(), String.class, GenericAction.class);

        for (Map.Entry<String, ActionHandler<?, ?>> entry : actions.entrySet()) {
            actionsBinder.addBinding(entry.getKey()).toInstance(entry.getValue().getAction());
        }
        if (nodeClient != null) {
            bind(AutoCreateIndex.class).toInstance(autoCreateIndex);
        }
    }
}
