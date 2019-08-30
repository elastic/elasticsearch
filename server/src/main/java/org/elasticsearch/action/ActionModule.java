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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainAction;
import org.elasticsearch.action.admin.cluster.allocation.TransportClusterAllocationExplainAction;
import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.TransportAddVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.TransportClearVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsAction;
import org.elasticsearch.action.admin.cluster.node.hotthreads.TransportNodesHotThreadsAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.info.TransportNodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.reload.NodesReloadSecureSettingsAction;
import org.elasticsearch.action.admin.cluster.node.reload.TransportNodesReloadSecureSettingsAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskAction;
import org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.admin.cluster.node.usage.NodesUsageAction;
import org.elasticsearch.action.admin.cluster.node.usage.TransportNodesUsageAction;
import org.elasticsearch.action.admin.cluster.remote.RemoteInfoAction;
import org.elasticsearch.action.admin.cluster.remote.TransportRemoteInfoAction;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.TransportCleanupRepositoryAction;
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
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.TransportGetAliasesAction;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.analyze.TransportAnalyzeAction;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheAction;
import org.elasticsearch.action.admin.indices.cache.clear.TransportClearIndicesCacheAction;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.close.TransportCloseIndexAction;
import org.elasticsearch.action.admin.indices.close.TransportVerifyShardBeforeCloseAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
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
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
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
import org.elasticsearch.action.admin.indices.shrink.ResizeAction;
import org.elasticsearch.action.admin.indices.shrink.TransportResizeAction;
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
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesAction;
import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesIndexAction;
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
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.NamedRegistry;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.gateway.TransportNodesListGatewayMetaState;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards;
import org.elasticsearch.index.seqno.GlobalCheckpointSyncAction;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.index.seqno.RetentionLeaseBackgroundSyncAction;
import org.elasticsearch.index.seqno.RetentionLeaseSyncAction;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetaData;
import org.elasticsearch.persistent.CompletionPersistentTaskAction;
import org.elasticsearch.persistent.RemovePersistentTaskAction;
import org.elasticsearch.persistent.StartPersistentTaskAction;
import org.elasticsearch.persistent.UpdatePersistentTaskStatusAction;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.action.RestFieldCapabilitiesAction;
import org.elasticsearch.rest.action.RestMainAction;
import org.elasticsearch.rest.action.admin.cluster.RestAddVotingConfigExclusionAction;
import org.elasticsearch.rest.action.admin.cluster.RestCancelTasksAction;
import org.elasticsearch.rest.action.admin.cluster.RestCleanupRepositoryAction;
import org.elasticsearch.rest.action.admin.cluster.RestClearVotingConfigExclusionsAction;
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
import org.elasticsearch.rest.action.admin.cluster.RestReloadSecureSettingsAction;
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
import org.elasticsearch.rest.action.admin.indices.RestResizeHandler;
import org.elasticsearch.rest.action.admin.indices.RestRolloverIndexAction;
import org.elasticsearch.rest.action.admin.indices.RestSyncedFlushAction;
import org.elasticsearch.rest.action.admin.indices.RestUpdateSettingsAction;
import org.elasticsearch.rest.action.admin.indices.RestUpgradeAction;
import org.elasticsearch.rest.action.admin.indices.RestUpgradeStatusAction;
import org.elasticsearch.rest.action.admin.indices.RestValidateQueryAction;
import org.elasticsearch.rest.action.cat.AbstractCatAction;
import org.elasticsearch.rest.action.cat.RestAliasAction;
import org.elasticsearch.rest.action.cat.RestAllocationAction;
import org.elasticsearch.rest.action.cat.RestCatAction;
import org.elasticsearch.rest.action.cat.RestCatRecoveryAction;
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
import org.elasticsearch.rest.action.search.RestCountAction;
import org.elasticsearch.rest.action.search.RestExplainAction;
import org.elasticsearch.rest.action.search.RestMultiSearchAction;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.rest.action.search.RestSearchScrollAction;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.usage.UsageService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableMap;

/**
 * Builds and binds the generic action map, all {@link TransportAction}s, and {@link ActionFilters}.
 */
public class ActionModule extends AbstractModule {

    private static final Logger logger = LogManager.getLogger(ActionModule.class);

    private final Settings settings;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final IndexScopedSettings indexScopedSettings;
    private final ClusterSettings clusterSettings;
    private final SettingsFilter settingsFilter;
    private final List<ActionPlugin> actionPlugins;
    private final Map<String, ActionHandler<?, ?>> actions;
    private final ActionFilters actionFilters;
    private final AutoCreateIndex autoCreateIndex;
    private final DestructiveOperations destructiveOperations;
    private final RestController restController;
    private final RequestValidators<PutMappingRequest> mappingRequestValidators;
    private final RequestValidators<IndicesAliasesRequest> indicesAliasesRequestRequestValidators;

    public ActionModule(Settings settings, IndexNameExpressionResolver indexNameExpressionResolver,
                        IndexScopedSettings indexScopedSettings, ClusterSettings clusterSettings, SettingsFilter settingsFilter,
                        ThreadPool threadPool, List<ActionPlugin> actionPlugins, NodeClient nodeClient,
                        CircuitBreakerService circuitBreakerService, UsageService usageService) {
        this.settings = settings;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.indexScopedSettings = indexScopedSettings;
        this.clusterSettings = clusterSettings;
        this.settingsFilter = settingsFilter;
        this.actionPlugins = actionPlugins;
        actions = setupActions(actionPlugins);
        actionFilters = setupActionFilters(actionPlugins);
        autoCreateIndex = new AutoCreateIndex(settings, clusterSettings, indexNameExpressionResolver);
        destructiveOperations = new DestructiveOperations(settings, clusterSettings);
        Set<String> headers = Stream.concat(
            actionPlugins.stream().flatMap(p -> p.getRestHeaders().stream()),
            Stream.of(Task.X_OPAQUE_ID)
        ).collect(Collectors.toSet());
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
        mappingRequestValidators = new RequestValidators<>(
            actionPlugins.stream().flatMap(p -> p.mappingRequestValidators().stream()).collect(Collectors.toList()));
        indicesAliasesRequestRequestValidators = new RequestValidators<>(
                actionPlugins.stream().flatMap(p -> p.indicesAliasesRequestValidators().stream()).collect(Collectors.toList()));

        restController = new RestController(headers, restWrapper, nodeClient, circuitBreakerService, usageService);
    }


    public Map<String, ActionHandler<?, ?>> getActions() {
        return actions;
    }

    static Map<String, ActionHandler<?, ?>> setupActions(List<ActionPlugin> actionPlugins) {
        // Subclass NamedRegistry for easy registration
        class ActionRegistry extends NamedRegistry<ActionHandler<?, ?>> {
            ActionRegistry() {
                super("action");
            }

            public void register(ActionHandler<?, ?> handler) {
                register(handler.getAction().name(), handler);
            }

            public <Request extends ActionRequest, Response extends ActionResponse> void register(
                ActionType<Response> action, Class<? extends TransportAction<Request, Response>> transportAction) {
                register(new ActionHandler<>(action, transportAction));
            }
        }
        ActionRegistry actions = new ActionRegistry();

        actions.register(MainAction.INSTANCE, TransportMainAction.class);
        actions.register(NodesInfoAction.INSTANCE, TransportNodesInfoAction.class);
        actions.register(RemoteInfoAction.INSTANCE, TransportRemoteInfoAction.class);
        actions.register(NodesStatsAction.INSTANCE, TransportNodesStatsAction.class);
        actions.register(NodesUsageAction.INSTANCE, TransportNodesUsageAction.class);
        actions.register(NodesHotThreadsAction.INSTANCE, TransportNodesHotThreadsAction.class);
        actions.register(ListTasksAction.INSTANCE, TransportListTasksAction.class);
        actions.register(GetTaskAction.INSTANCE, TransportGetTaskAction.class);
        actions.register(CancelTasksAction.INSTANCE, TransportCancelTasksAction.class);

        actions.register(AddVotingConfigExclusionsAction.INSTANCE, TransportAddVotingConfigExclusionsAction.class);
        actions.register(ClearVotingConfigExclusionsAction.INSTANCE, TransportClearVotingConfigExclusionsAction.class);
        actions.register(ClusterAllocationExplainAction.INSTANCE, TransportClusterAllocationExplainAction.class);
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
        actions.register(CleanupRepositoryAction.INSTANCE, TransportCleanupRepositoryAction.class);
        actions.register(GetSnapshotsAction.INSTANCE, TransportGetSnapshotsAction.class);
        actions.register(DeleteSnapshotAction.INSTANCE, TransportDeleteSnapshotAction.class);
        actions.register(CreateSnapshotAction.INSTANCE, TransportCreateSnapshotAction.class);
        actions.register(RestoreSnapshotAction.INSTANCE, TransportRestoreSnapshotAction.class);
        actions.register(SnapshotsStatusAction.INSTANCE, TransportSnapshotsStatusAction.class);

        actions.register(IndicesStatsAction.INSTANCE, TransportIndicesStatsAction.class);
        actions.register(IndicesSegmentsAction.INSTANCE, TransportIndicesSegmentsAction.class);
        actions.register(IndicesShardStoresAction.INSTANCE, TransportIndicesShardStoresAction.class);
        actions.register(CreateIndexAction.INSTANCE, TransportCreateIndexAction.class);
        actions.register(ResizeAction.INSTANCE, TransportResizeAction.class);
        actions.register(RolloverAction.INSTANCE, TransportRolloverAction.class);
        actions.register(DeleteIndexAction.INSTANCE, TransportDeleteIndexAction.class);
        actions.register(GetIndexAction.INSTANCE, TransportGetIndexAction.class);
        actions.register(OpenIndexAction.INSTANCE, TransportOpenIndexAction.class);
        actions.register(CloseIndexAction.INSTANCE, TransportCloseIndexAction.class);
        actions.register(GetMappingsAction.INSTANCE, TransportGetMappingsAction.class);
        actions.register(GetFieldMappingsAction.INSTANCE, TransportGetFieldMappingsAction.class);
        actions.register(TransportGetFieldMappingsIndexAction.TYPE, TransportGetFieldMappingsIndexAction.class);
        actions.register(PutMappingAction.INSTANCE, TransportPutMappingAction.class);
        actions.register(IndicesAliasesAction.INSTANCE, TransportIndicesAliasesAction.class);
        actions.register(UpdateSettingsAction.INSTANCE, TransportUpdateSettingsAction.class);
        actions.register(AnalyzeAction.INSTANCE, TransportAnalyzeAction.class);
        actions.register(PutIndexTemplateAction.INSTANCE, TransportPutIndexTemplateAction.class);
        actions.register(GetIndexTemplatesAction.INSTANCE, TransportGetIndexTemplatesAction.class);
        actions.register(DeleteIndexTemplateAction.INSTANCE, TransportDeleteIndexTemplateAction.class);
        actions.register(ValidateQueryAction.INSTANCE, TransportValidateQueryAction.class);
        actions.register(RefreshAction.INSTANCE, TransportRefreshAction.class);
        actions.register(FlushAction.INSTANCE, TransportFlushAction.class);
        actions.register(SyncedFlushAction.INSTANCE, TransportSyncedFlushAction.class);
        actions.register(ForceMergeAction.INSTANCE, TransportForceMergeAction.class);
        actions.register(UpgradeAction.INSTANCE, TransportUpgradeAction.class);
        actions.register(UpgradeStatusAction.INSTANCE, TransportUpgradeStatusAction.class);
        actions.register(UpgradeSettingsAction.INSTANCE, TransportUpgradeSettingsAction.class);
        actions.register(ClearIndicesCacheAction.INSTANCE, TransportClearIndicesCacheAction.class);
        actions.register(GetAliasesAction.INSTANCE, TransportGetAliasesAction.class);
        actions.register(GetSettingsAction.INSTANCE, TransportGetSettingsAction.class);

        actions.register(IndexAction.INSTANCE, TransportIndexAction.class);
        actions.register(GetAction.INSTANCE, TransportGetAction.class);
        actions.register(TermVectorsAction.INSTANCE, TransportTermVectorsAction.class);
        actions.register(MultiTermVectorsAction.INSTANCE, TransportMultiTermVectorsAction.class);
        actions.register(TransportShardMultiTermsVectorAction.TYPE, TransportShardMultiTermsVectorAction.class);
        actions.register(DeleteAction.INSTANCE, TransportDeleteAction.class);
        actions.register(UpdateAction.INSTANCE, TransportUpdateAction.class);
        actions.register(MultiGetAction.INSTANCE, TransportMultiGetAction.class);
        actions.register(TransportShardMultiGetAction.TYPE, TransportShardMultiGetAction.class);
        actions.register(BulkAction.INSTANCE, TransportBulkAction.class);
        actions.register(TransportShardBulkAction.TYPE, TransportShardBulkAction.class);
        actions.register(SearchAction.INSTANCE, TransportSearchAction.class);
        actions.register(SearchScrollAction.INSTANCE, TransportSearchScrollAction.class);
        actions.register(MultiSearchAction.INSTANCE, TransportMultiSearchAction.class);
        actions.register(ExplainAction.INSTANCE, TransportExplainAction.class);
        actions.register(ClearScrollAction.INSTANCE, TransportClearScrollAction.class);
        actions.register(RecoveryAction.INSTANCE, TransportRecoveryAction.class);
        actions.register(NodesReloadSecureSettingsAction.INSTANCE, TransportNodesReloadSecureSettingsAction.class);

        //Indexed scripts
        actions.register(PutStoredScriptAction.INSTANCE, TransportPutStoredScriptAction.class);
        actions.register(GetStoredScriptAction.INSTANCE, TransportGetStoredScriptAction.class);
        actions.register(DeleteStoredScriptAction.INSTANCE, TransportDeleteStoredScriptAction.class);

        actions.register(FieldCapabilitiesAction.INSTANCE, TransportFieldCapabilitiesAction.class);
        actions.register(TransportFieldCapabilitiesIndexAction.TYPE, TransportFieldCapabilitiesIndexAction.class);

        actions.register(PutPipelineAction.INSTANCE, PutPipelineTransportAction.class);
        actions.register(GetPipelineAction.INSTANCE, GetPipelineTransportAction.class);
        actions.register(DeletePipelineAction.INSTANCE, DeletePipelineTransportAction.class);
        actions.register(SimulatePipelineAction.INSTANCE, SimulatePipelineTransportAction.class);

        actionPlugins.stream().flatMap(p -> p.getActions().stream()).forEach(actions::register);

        // Persistent tasks:
        actions.register(StartPersistentTaskAction.INSTANCE, StartPersistentTaskAction.TransportAction.class);
        actions.register(UpdatePersistentTaskStatusAction.INSTANCE, UpdatePersistentTaskStatusAction.TransportAction.class);
        actions.register(CompletionPersistentTaskAction.INSTANCE, CompletionPersistentTaskAction.TransportAction.class);
        actions.register(RemovePersistentTaskAction.INSTANCE, RemovePersistentTaskAction.TransportAction.class);

        // retention leases
        actions.register(RetentionLeaseActions.Add.INSTANCE, RetentionLeaseActions.Add.TransportAction.class);
        actions.register(RetentionLeaseActions.Renew.INSTANCE, RetentionLeaseActions.Renew.TransportAction.class);
        actions.register(RetentionLeaseActions.Remove.INSTANCE, RetentionLeaseActions.Remove.TransportAction.class);

        // internal actions
        actions.register(GlobalCheckpointSyncAction.TYPE, GlobalCheckpointSyncAction.class);
        actions.register(RetentionLeaseBackgroundSyncAction.TYPE, RetentionLeaseBackgroundSyncAction.class);
        actions.register(RetentionLeaseSyncAction.TYPE, RetentionLeaseSyncAction.class);
        actions.register(TransportNodesSnapshotsStatus.TYPE, TransportNodesSnapshotsStatus.class);
        actions.register(TransportNodesListGatewayMetaState.TYPE, TransportNodesListGatewayMetaState.class);
        actions.register(TransportVerifyShardBeforeCloseAction.TYPE, TransportVerifyShardBeforeCloseAction.class);
        actions.register(TransportNodesListGatewayStartedShards.TYPE, TransportNodesListGatewayStartedShards.class);
        actions.register(TransportNodesListShardStoreMetaData.TYPE, TransportNodesListShardStoreMetaData.class);
        actions.register(TransportShardFlushAction.TYPE, TransportShardFlushAction.class);
        actions.register(TransportShardRefreshAction.TYPE, TransportShardRefreshAction.class);

        return unmodifiableMap(actions.getRegistry());
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
        registerHandler.accept(new RestAddVotingConfigExclusionAction(restController));
        registerHandler.accept(new RestClearVotingConfigExclusionsAction(restController));
        registerHandler.accept(new RestMainAction(restController));
        registerHandler.accept(new RestNodesInfoAction(restController, settingsFilter));
        registerHandler.accept(new RestRemoteClusterInfoAction(restController));
        registerHandler.accept(new RestNodesStatsAction(restController));
        registerHandler.accept(new RestNodesUsageAction(restController));
        registerHandler.accept(new RestNodesHotThreadsAction(restController));
        registerHandler.accept(new RestClusterAllocationExplainAction(restController));
        registerHandler.accept(new RestClusterStatsAction(restController));
        registerHandler.accept(new RestClusterStateAction(restController, settingsFilter));
        registerHandler.accept(new RestClusterHealthAction(restController));
        registerHandler.accept(new RestClusterUpdateSettingsAction(restController));
        registerHandler.accept(new RestClusterGetSettingsAction(settings, restController, clusterSettings, settingsFilter));
        registerHandler.accept(new RestClusterRerouteAction(restController, settingsFilter));
        registerHandler.accept(new RestClusterSearchShardsAction(restController));
        registerHandler.accept(new RestPendingClusterTasksAction(restController));
        registerHandler.accept(new RestPutRepositoryAction(restController));
        registerHandler.accept(new RestGetRepositoriesAction(restController, settingsFilter));
        registerHandler.accept(new RestDeleteRepositoryAction(restController));
        registerHandler.accept(new RestVerifyRepositoryAction(restController));
        registerHandler.accept(new RestCleanupRepositoryAction(restController));
        registerHandler.accept(new RestGetSnapshotsAction(restController));
        registerHandler.accept(new RestCreateSnapshotAction(restController));
        registerHandler.accept(new RestRestoreSnapshotAction(restController));
        registerHandler.accept(new RestDeleteSnapshotAction(restController));
        registerHandler.accept(new RestSnapshotsStatusAction(restController));
        registerHandler.accept(new RestGetIndicesAction(restController));
        registerHandler.accept(new RestIndicesStatsAction(restController));
        registerHandler.accept(new RestIndicesSegmentsAction(restController));
        registerHandler.accept(new RestIndicesShardStoresAction(restController));
        registerHandler.accept(new RestGetAliasesAction(restController));
        registerHandler.accept(new RestIndexDeleteAliasesAction(restController));
        registerHandler.accept(new RestIndexPutAliasAction(restController));
        registerHandler.accept(new RestIndicesAliasesAction(restController));
        registerHandler.accept(new RestCreateIndexAction(restController));
        registerHandler.accept(new RestResizeHandler.RestShrinkIndexAction(restController));
        registerHandler.accept(new RestResizeHandler.RestSplitIndexAction(restController));
        registerHandler.accept(new RestResizeHandler.RestCloneIndexAction(restController));
        registerHandler.accept(new RestRolloverIndexAction(restController));
        registerHandler.accept(new RestDeleteIndexAction(restController));
        registerHandler.accept(new RestCloseIndexAction(restController));
        registerHandler.accept(new RestOpenIndexAction(restController));

        registerHandler.accept(new RestUpdateSettingsAction(restController));
        registerHandler.accept(new RestGetSettingsAction(restController));

        registerHandler.accept(new RestAnalyzeAction(restController));
        registerHandler.accept(new RestGetIndexTemplateAction(restController));
        registerHandler.accept(new RestPutIndexTemplateAction(restController));
        registerHandler.accept(new RestDeleteIndexTemplateAction(restController));

        registerHandler.accept(new RestPutMappingAction(restController));
        registerHandler.accept(new RestGetMappingAction(restController));
        registerHandler.accept(new RestGetFieldMappingAction(restController));

        registerHandler.accept(new RestRefreshAction(restController));
        registerHandler.accept(new RestFlushAction(restController));
        registerHandler.accept(new RestSyncedFlushAction(restController));
        registerHandler.accept(new RestForceMergeAction(restController));
        registerHandler.accept(new RestUpgradeAction(restController));
        registerHandler.accept(new RestUpgradeStatusAction(restController));
        registerHandler.accept(new RestClearIndicesCacheAction(restController));

        registerHandler.accept(new RestIndexAction(restController));
        registerHandler.accept(new RestGetAction(restController));
        registerHandler.accept(new RestGetSourceAction(restController));
        registerHandler.accept(new RestMultiGetAction(settings, restController));
        registerHandler.accept(new RestDeleteAction(restController));
        registerHandler.accept(new RestCountAction(restController));
        registerHandler.accept(new RestTermVectorsAction(restController));
        registerHandler.accept(new RestMultiTermVectorsAction(restController));
        registerHandler.accept(new RestBulkAction(settings, restController));
        registerHandler.accept(new RestUpdateAction(restController));

        registerHandler.accept(new RestSearchAction(restController));
        registerHandler.accept(new RestSearchScrollAction(restController));
        registerHandler.accept(new RestClearScrollAction(restController));
        registerHandler.accept(new RestMultiSearchAction(settings, restController));

        registerHandler.accept(new RestValidateQueryAction(restController));

        registerHandler.accept(new RestExplainAction(restController));

        registerHandler.accept(new RestRecoveryAction(restController));

        registerHandler.accept(new RestReloadSecureSettingsAction(restController));

        // Scripts API
        registerHandler.accept(new RestGetStoredScriptAction(restController));
        registerHandler.accept(new RestPutStoredScriptAction(restController));
        registerHandler.accept(new RestDeleteStoredScriptAction(restController));

        registerHandler.accept(new RestFieldCapabilitiesAction(restController));

        // Tasks API
        registerHandler.accept(new RestListTasksAction(restController, nodesInCluster));
        registerHandler.accept(new RestGetTaskAction(restController));
        registerHandler.accept(new RestCancelTasksAction(restController, nodesInCluster));

        // Ingest API
        registerHandler.accept(new RestPutPipelineAction(restController));
        registerHandler.accept(new RestGetPipelineAction(restController));
        registerHandler.accept(new RestDeletePipelineAction(restController));
        registerHandler.accept(new RestSimulatePipelineAction(restController));

        // CAT API
        registerHandler.accept(new RestAllocationAction(restController));
        registerHandler.accept(new RestShardsAction(restController));
        registerHandler.accept(new RestMasterAction(restController));
        registerHandler.accept(new RestNodesAction(restController));
        registerHandler.accept(new RestTasksAction(restController, nodesInCluster));
        registerHandler.accept(new RestIndicesAction(restController));
        registerHandler.accept(new RestSegmentsAction(restController));
        // Fully qualified to prevent interference with rest.action.count.RestCountAction
        registerHandler.accept(new org.elasticsearch.rest.action.cat.RestCountAction(restController));
        // Fully qualified to prevent interference with rest.action.indices.RestRecoveryAction
        registerHandler.accept(new RestCatRecoveryAction(restController));
        registerHandler.accept(new RestHealthAction(restController));
        registerHandler.accept(new org.elasticsearch.rest.action.cat.RestPendingClusterTasksAction(restController));
        registerHandler.accept(new RestAliasAction(restController));
        registerHandler.accept(new RestThreadPoolAction(restController));
        registerHandler.accept(new RestPluginsAction(restController));
        registerHandler.accept(new RestFielddataAction(restController));
        registerHandler.accept(new RestNodeAttrsAction(restController));
        registerHandler.accept(new RestRepositoriesAction(restController));
        registerHandler.accept(new RestSnapshotAction(restController));
        registerHandler.accept(new RestTemplatesAction(restController));
        for (ActionPlugin plugin : actionPlugins) {
            for (RestHandler handler : plugin.getRestHandlers(settings, restController, clusterSettings, indexScopedSettings,
                    settingsFilter, indexNameExpressionResolver, nodesInCluster)) {
                registerHandler.accept(handler);
            }
        }
        registerHandler.accept(new RestCatAction(restController, catActions));
    }

    @Override
    protected void configure() {
        bind(ActionFilters.class).toInstance(actionFilters);
        bind(DestructiveOperations.class).toInstance(destructiveOperations);
        bind(new TypeLiteral<RequestValidators<PutMappingRequest>>() {}).toInstance(mappingRequestValidators);
        bind(new TypeLiteral<RequestValidators<IndicesAliasesRequest>>() {}).toInstance(indicesAliasesRequestRequestValidators);
        bind(AutoCreateIndex.class).toInstance(autoCreateIndex);

        // register ActionType -> transportAction Map used by NodeClient
        @SuppressWarnings("rawtypes")
        MapBinder<ActionType, TransportAction> transportActionsBinder
                = MapBinder.newMapBinder(binder(), ActionType.class, TransportAction.class);
        for (ActionHandler<?, ?> action : actions.values()) {
            // bind the action as eager singleton, so the map binder one will reuse it
            bind(action.getTransportAction()).asEagerSingleton();
            transportActionsBinder.addBinding(action.getAction()).to(action.getTransportAction()).asEagerSingleton();
        }

    }

    public ActionFilters getActionFilters() {
        return actionFilters;
    }

    public RestController getRestController() {
        return restController;
    }
}
