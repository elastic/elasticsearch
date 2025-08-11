/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusAction;
import org.elasticsearch.action.admin.cluster.migration.PostFeatureUpgradeAction;
import org.elasticsearch.action.admin.cluster.migration.TransportGetFeatureUpgradeStatusAction;
import org.elasticsearch.action.admin.cluster.migration.TransportPostFeatureUpgradeAction;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsAction;
import org.elasticsearch.action.admin.cluster.node.hotthreads.TransportNodesHotThreadsAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.info.TransportNodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.liveness.TransportLivenessAction;
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
import org.elasticsearch.action.admin.cluster.snapshots.clone.CloneSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.clone.TransportCloneSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.TransportDeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateAction;
import org.elasticsearch.action.admin.cluster.snapshots.features.SnapshottableFeaturesAction;
import org.elasticsearch.action.admin.cluster.snapshots.features.TransportResetFeatureStateAction;
import org.elasticsearch.action.admin.cluster.snapshots.features.TransportSnapshottableFeaturesAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.TransportGetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.shard.GetShardSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.shard.TransportGetShardSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.TransportRestoreSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusAction;
import org.elasticsearch.action.admin.cluster.snapshots.status.TransportSnapshotsStatusAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.TransportClusterStateAction;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsAction;
import org.elasticsearch.action.admin.cluster.stats.TransportClusterStatsAction;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptAction;
import org.elasticsearch.action.admin.cluster.storedscripts.GetScriptContextAction;
import org.elasticsearch.action.admin.cluster.storedscripts.GetScriptLanguageAction;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptAction;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptAction;
import org.elasticsearch.action.admin.cluster.storedscripts.TransportDeleteStoredScriptAction;
import org.elasticsearch.action.admin.cluster.storedscripts.TransportGetScriptContextAction;
import org.elasticsearch.action.admin.cluster.storedscripts.TransportGetScriptLanguageAction;
import org.elasticsearch.action.admin.cluster.storedscripts.TransportGetStoredScriptAction;
import org.elasticsearch.action.admin.cluster.storedscripts.TransportPutStoredScriptAction;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksAction;
import org.elasticsearch.action.admin.cluster.tasks.TransportPendingClusterTasksAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
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
import org.elasticsearch.action.admin.indices.create.AutoCreateAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.dangling.delete.DeleteDanglingIndexAction;
import org.elasticsearch.action.admin.indices.dangling.delete.TransportDeleteDanglingIndexAction;
import org.elasticsearch.action.admin.indices.dangling.find.FindDanglingIndexAction;
import org.elasticsearch.action.admin.indices.dangling.find.TransportFindDanglingIndexAction;
import org.elasticsearch.action.admin.indices.dangling.import_index.ImportDanglingIndexAction;
import org.elasticsearch.action.admin.indices.dangling.import_index.TransportImportDanglingIndexAction;
import org.elasticsearch.action.admin.indices.dangling.list.ListDanglingIndicesAction;
import org.elasticsearch.action.admin.indices.dangling.list.TransportListDanglingIndicesAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.diskusage.AnalyzeIndexDiskUsageAction;
import org.elasticsearch.action.admin.indices.diskusage.TransportAnalyzeIndexDiskUsageAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.indices.TransportIndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.types.TransportTypesExistsAction;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsAction;
import org.elasticsearch.action.admin.indices.flush.FlushAction;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushAction;
import org.elasticsearch.action.admin.indices.flush.TransportFlushAction;
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
import org.elasticsearch.action.admin.indices.mapping.put.AutoPutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.TransportAutoPutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.admin.indices.open.TransportOpenIndexAction;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockAction;
import org.elasticsearch.action.admin.indices.readonly.TransportAddIndexBlockAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.recovery.TransportRecoveryAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.TransportRefreshAction;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
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
import org.elasticsearch.action.admin.indices.stats.FieldUsageStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.TransportFieldUsageAction;
import org.elasticsearch.action.admin.indices.stats.TransportIndicesStatsAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.get.TransportGetComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.TransportGetComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.TransportGetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.post.SimulateIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.post.SimulateTemplateAction;
import org.elasticsearch.action.admin.indices.template.post.TransportSimulateIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.post.TransportSimulateTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
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
import org.elasticsearch.action.datastreams.ModifyDataStreamsAction;
import org.elasticsearch.action.datastreams.ModifyDataStreamsTransportAction;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.explain.ExplainAction;
import org.elasticsearch.action.explain.TransportExplainAction;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesAction;
import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction;
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
import org.elasticsearch.action.search.ClosePointInTimeAction;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.OpenPointInTimeAction;
import org.elasticsearch.action.search.RestClosePointInTimeAction;
import org.elasticsearch.action.search.RestOpenPointInTimeAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.TransportClearScrollAction;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportMultiSearchAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
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
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpPreRequest;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.persistent.CompletionPersistentTaskAction;
import org.elasticsearch.persistent.RemovePersistentTaskAction;
import org.elasticsearch.persistent.StartPersistentTaskAction;
import org.elasticsearch.persistent.UpdatePersistentTaskStatusAction;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestHeaderDefinition;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.action.RestFieldCapabilitiesAction;
import org.elasticsearch.rest.action.RestMainAction;
import org.elasticsearch.rest.action.admin.cluster.RestAddVotingConfigExclusionAction;
import org.elasticsearch.rest.action.admin.cluster.RestCancelTasksAction;
import org.elasticsearch.rest.action.admin.cluster.RestCleanupRepositoryAction;
import org.elasticsearch.rest.action.admin.cluster.RestClearVotingConfigExclusionsAction;
import org.elasticsearch.rest.action.admin.cluster.RestCloneSnapshotAction;
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
import org.elasticsearch.rest.action.admin.cluster.RestGetFeatureUpgradeStatusAction;
import org.elasticsearch.rest.action.admin.cluster.RestGetRepositoriesAction;
import org.elasticsearch.rest.action.admin.cluster.RestGetScriptContextAction;
import org.elasticsearch.rest.action.admin.cluster.RestGetScriptLanguageAction;
import org.elasticsearch.rest.action.admin.cluster.RestGetSnapshotsAction;
import org.elasticsearch.rest.action.admin.cluster.RestGetStoredScriptAction;
import org.elasticsearch.rest.action.admin.cluster.RestGetTaskAction;
import org.elasticsearch.rest.action.admin.cluster.RestListTasksAction;
import org.elasticsearch.rest.action.admin.cluster.RestNodesHotThreadsAction;
import org.elasticsearch.rest.action.admin.cluster.RestNodesInfoAction;
import org.elasticsearch.rest.action.admin.cluster.RestNodesStatsAction;
import org.elasticsearch.rest.action.admin.cluster.RestNodesUsageAction;
import org.elasticsearch.rest.action.admin.cluster.RestPendingClusterTasksAction;
import org.elasticsearch.rest.action.admin.cluster.RestPostFeatureUpgradeAction;
import org.elasticsearch.rest.action.admin.cluster.RestPutRepositoryAction;
import org.elasticsearch.rest.action.admin.cluster.RestPutStoredScriptAction;
import org.elasticsearch.rest.action.admin.cluster.RestReloadSecureSettingsAction;
import org.elasticsearch.rest.action.admin.cluster.RestRemoteClusterInfoAction;
import org.elasticsearch.rest.action.admin.cluster.RestResetFeatureStateAction;
import org.elasticsearch.rest.action.admin.cluster.RestRestoreSnapshotAction;
import org.elasticsearch.rest.action.admin.cluster.RestSnapshotsStatusAction;
import org.elasticsearch.rest.action.admin.cluster.RestSnapshottableFeaturesAction;
import org.elasticsearch.rest.action.admin.cluster.RestVerifyRepositoryAction;
import org.elasticsearch.rest.action.admin.cluster.dangling.RestDeleteDanglingIndexAction;
import org.elasticsearch.rest.action.admin.cluster.dangling.RestImportDanglingIndexAction;
import org.elasticsearch.rest.action.admin.cluster.dangling.RestListDanglingIndicesAction;
import org.elasticsearch.rest.action.admin.indices.RestAddIndexBlockAction;
import org.elasticsearch.rest.action.admin.indices.RestAnalyzeAction;
import org.elasticsearch.rest.action.admin.indices.RestAnalyzeIndexDiskUsageAction;
import org.elasticsearch.rest.action.admin.indices.RestClearIndicesCacheAction;
import org.elasticsearch.rest.action.admin.indices.RestCloseIndexAction;
import org.elasticsearch.rest.action.admin.indices.RestCreateIndexAction;
import org.elasticsearch.rest.action.admin.indices.RestDeleteComponentTemplateAction;
import org.elasticsearch.rest.action.admin.indices.RestDeleteComposableIndexTemplateAction;
import org.elasticsearch.rest.action.admin.indices.RestDeleteIndexAction;
import org.elasticsearch.rest.action.admin.indices.RestDeleteIndexTemplateAction;
import org.elasticsearch.rest.action.admin.indices.RestFieldUsageStatsAction;
import org.elasticsearch.rest.action.admin.indices.RestFlushAction;
import org.elasticsearch.rest.action.admin.indices.RestForceMergeAction;
import org.elasticsearch.rest.action.admin.indices.RestGetAliasesAction;
import org.elasticsearch.rest.action.admin.indices.RestGetComponentTemplateAction;
import org.elasticsearch.rest.action.admin.indices.RestGetComposableIndexTemplateAction;
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
import org.elasticsearch.rest.action.admin.indices.RestPutComponentTemplateAction;
import org.elasticsearch.rest.action.admin.indices.RestPutComposableIndexTemplateAction;
import org.elasticsearch.rest.action.admin.indices.RestPutIndexTemplateAction;
import org.elasticsearch.rest.action.admin.indices.RestPutMappingAction;
import org.elasticsearch.rest.action.admin.indices.RestRecoveryAction;
import org.elasticsearch.rest.action.admin.indices.RestRefreshAction;
import org.elasticsearch.rest.action.admin.indices.RestResizeHandler;
import org.elasticsearch.rest.action.admin.indices.RestResolveIndexAction;
import org.elasticsearch.rest.action.admin.indices.RestRolloverIndexAction;
import org.elasticsearch.rest.action.admin.indices.RestSimulateIndexTemplateAction;
import org.elasticsearch.rest.action.admin.indices.RestSimulateTemplateAction;
import org.elasticsearch.rest.action.admin.indices.RestSyncedFlushAction;
import org.elasticsearch.rest.action.admin.indices.RestUpdateSettingsAction;
import org.elasticsearch.rest.action.admin.indices.RestUpgradeActionDeprecated;
import org.elasticsearch.rest.action.admin.indices.RestUpgradeStatusActionDeprecated;
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
import org.elasticsearch.rest.action.datastreams.RestModifyDataStreamsAction;
import org.elasticsearch.rest.action.document.RestBulkAction;
import org.elasticsearch.rest.action.document.RestDeleteAction;
import org.elasticsearch.rest.action.document.RestGetAction;
import org.elasticsearch.rest.action.document.RestGetSourceAction;
import org.elasticsearch.rest.action.document.RestIndexAction;
import org.elasticsearch.rest.action.document.RestIndexAction.AutoIdHandler;
import org.elasticsearch.rest.action.document.RestIndexAction.CreateHandler;
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
import java.util.Optional;
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
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(logger.getName());

    private final boolean transportClient;
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
    /** Rest headers that are copied to internal requests made during a rest request. */
    private final Set<RestHeaderDefinition> headersToCopy;
    private final RequestValidators<PutMappingRequest> mappingRequestValidators;
    private final RequestValidators<IndicesAliasesRequest> indicesAliasesRequestRequestValidators;
    private final ThreadPool threadPool;

    public ActionModule(
        boolean transportClient,
        Settings settings,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndexScopedSettings indexScopedSettings,
        ClusterSettings clusterSettings,
        SettingsFilter settingsFilter,
        ThreadPool threadPool,
        List<ActionPlugin> actionPlugins,
        NodeClient nodeClient,
        CircuitBreakerService circuitBreakerService,
        UsageService usageService,
        SystemIndices systemIndices
    ) {
        this.transportClient = transportClient;
        this.settings = settings;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.indexScopedSettings = indexScopedSettings;
        this.clusterSettings = clusterSettings;
        this.settingsFilter = settingsFilter;
        this.actionPlugins = actionPlugins;
        this.threadPool = threadPool;
        actions = setupActions(actionPlugins);
        actionFilters = setupActionFilters(actionPlugins);
        autoCreateIndex = transportClient
            ? null
            : new AutoCreateIndex(settings, clusterSettings, indexNameExpressionResolver, systemIndices);
        destructiveOperations = new DestructiveOperations(settings, clusterSettings);
        Set<RestHeaderDefinition> headers = Stream.concat(
            actionPlugins.stream().flatMap(p -> p.getRestHeaders().stream()),
            Stream.of(
                new RestHeaderDefinition(Task.X_OPAQUE_ID_HTTP_HEADER, false),
                new RestHeaderDefinition(Task.TRACE_PARENT_HTTP_HEADER, false),
                new RestHeaderDefinition(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER, false)
            )
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
                if ("org.elasticsearch.xpack.security.Security".equals(plugin.getClass().getCanonicalName()) == false) {
                    deprecationLogger.critical(
                        DeprecationCategory.PLUGINS,
                        "3rd_party_rest_deprecation",
                        "The "
                            + plugin.getClass().getName()
                            + " plugin installs a custom REST wrapper. This functionality is deprecated and will not be possible in "
                            + "Elasticsearch 8.0. If this plugin is intended to provide security features for Elasticsearch "
                            + "then you should switch to using the built-in Elasticsearch features instead."
                    );
                }
            }
        }
        mappingRequestValidators = new RequestValidators<>(
            actionPlugins.stream().flatMap(p -> p.mappingRequestValidators().stream()).collect(Collectors.toList())
        );
        indicesAliasesRequestRequestValidators = new RequestValidators<>(
            actionPlugins.stream().flatMap(p -> p.indicesAliasesRequestValidators().stream()).collect(Collectors.toList())
        );
        headersToCopy = headers;
        if (transportClient) {
            restController = null;
        } else {
            restController = new RestController(restWrapper, nodeClient, circuitBreakerService, usageService);
        }
    }

    /**
     * Certain request header values need to be copied in the thread context under which request handlers are to be dispatched.
     * Careful that this method modifies the thread context. The thread context must be reinstated after the request handler
     * finishes and returns.
     */
    public void copyRequestHeadersToThreadContext(HttpPreRequest request, ThreadContext threadContext) {
        for (final RestHeaderDefinition restHeader : headersToCopy) {
            final String name = restHeader.getName();
            final List<String> headerValues = request.getHeaders().get(name);
            if (headerValues != null && headerValues.isEmpty() == false) {
                final List<String> distinctHeaderValues = headerValues.stream().distinct().collect(Collectors.toList());
                if (restHeader.isMultiValueAllowed() == false && distinctHeaderValues.size() > 1) {
                    throw new IllegalArgumentException("multiple values for single-valued header [" + name + "].");
                } else if (name.equals(Task.TRACE_PARENT_HTTP_HEADER)) {
                    String traceparent = distinctHeaderValues.get(0);
                    Optional<String> traceId = RestUtils.extractTraceId(traceparent);
                    if (traceId.isPresent()) {
                        threadContext.putHeader(Task.TRACE_ID, traceId.get());
                    }
                } else {
                    threadContext.putHeader(name, String.join(",", distinctHeaderValues));
                }
            }
        }
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
                ActionType<Response> action,
                Class<? extends TransportAction<Request, Response>> transportAction,
                Class<?>... supportTransportActions
            ) {
                register(new ActionHandler<>(action, transportAction, supportTransportActions));
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
        actions.register(CloneSnapshotAction.INSTANCE, TransportCloneSnapshotAction.class);
        actions.register(RestoreSnapshotAction.INSTANCE, TransportRestoreSnapshotAction.class);
        actions.register(SnapshotsStatusAction.INSTANCE, TransportSnapshotsStatusAction.class);
        actions.register(SnapshottableFeaturesAction.INSTANCE, TransportSnapshottableFeaturesAction.class);
        actions.register(ResetFeatureStateAction.INSTANCE, TransportResetFeatureStateAction.class);
        actions.register(GetFeatureUpgradeStatusAction.INSTANCE, TransportGetFeatureUpgradeStatusAction.class);
        actions.register(PostFeatureUpgradeAction.INSTANCE, TransportPostFeatureUpgradeAction.class);
        actions.register(GetShardSnapshotAction.INSTANCE, TransportGetShardSnapshotAction.class);

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
        actions.register(IndicesExistsAction.INSTANCE, TransportIndicesExistsAction.class);
        actions.register(TypesExistsAction.INSTANCE, TransportTypesExistsAction.class);
        actions.register(AddIndexBlockAction.INSTANCE, TransportAddIndexBlockAction.class);
        actions.register(GetMappingsAction.INSTANCE, TransportGetMappingsAction.class);
        actions.register(
            GetFieldMappingsAction.INSTANCE,
            TransportGetFieldMappingsAction.class,
            TransportGetFieldMappingsIndexAction.class
        );
        actions.register(PutMappingAction.INSTANCE, TransportPutMappingAction.class);
        actions.register(AutoPutMappingAction.INSTANCE, TransportAutoPutMappingAction.class);
        actions.register(IndicesAliasesAction.INSTANCE, TransportIndicesAliasesAction.class);
        actions.register(UpdateSettingsAction.INSTANCE, TransportUpdateSettingsAction.class);
        actions.register(AnalyzeAction.INSTANCE, TransportAnalyzeAction.class);
        actions.register(PutIndexTemplateAction.INSTANCE, TransportPutIndexTemplateAction.class);
        actions.register(GetIndexTemplatesAction.INSTANCE, TransportGetIndexTemplatesAction.class);
        actions.register(DeleteIndexTemplateAction.INSTANCE, TransportDeleteIndexTemplateAction.class);
        actions.register(PutComponentTemplateAction.INSTANCE, TransportPutComponentTemplateAction.class);
        actions.register(GetComponentTemplateAction.INSTANCE, TransportGetComponentTemplateAction.class);
        actions.register(DeleteComponentTemplateAction.INSTANCE, TransportDeleteComponentTemplateAction.class);
        actions.register(PutComposableIndexTemplateAction.INSTANCE, TransportPutComposableIndexTemplateAction.class);
        actions.register(GetComposableIndexTemplateAction.INSTANCE, TransportGetComposableIndexTemplateAction.class);
        actions.register(DeleteComposableIndexTemplateAction.INSTANCE, TransportDeleteComposableIndexTemplateAction.class);
        actions.register(SimulateIndexTemplateAction.INSTANCE, TransportSimulateIndexTemplateAction.class);
        actions.register(SimulateTemplateAction.INSTANCE, TransportSimulateTemplateAction.class);
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
        actions.register(AliasesExistAction.INSTANCE, TransportAliasesExistAction.class);
        actions.register(GetSettingsAction.INSTANCE, TransportGetSettingsAction.class);

        actions.register(IndexAction.INSTANCE, TransportIndexAction.class);
        actions.register(GetAction.INSTANCE, TransportGetAction.class);
        actions.register(TermVectorsAction.INSTANCE, TransportTermVectorsAction.class);
        actions.register(
            MultiTermVectorsAction.INSTANCE,
            TransportMultiTermVectorsAction.class,
            TransportShardMultiTermsVectorAction.class
        );
        actions.register(DeleteAction.INSTANCE, TransportDeleteAction.class);
        actions.register(UpdateAction.INSTANCE, TransportUpdateAction.class);
        actions.register(MultiGetAction.INSTANCE, TransportMultiGetAction.class, TransportShardMultiGetAction.class);
        actions.register(BulkAction.INSTANCE, TransportBulkAction.class, TransportShardBulkAction.class);
        actions.register(SearchAction.INSTANCE, TransportSearchAction.class);
        actions.register(SearchScrollAction.INSTANCE, TransportSearchScrollAction.class);
        actions.register(OpenPointInTimeAction.INSTANCE, TransportOpenPointInTimeAction.class);
        actions.register(ClosePointInTimeAction.INSTANCE, TransportClosePointInTimeAction.class);
        actions.register(MultiSearchAction.INSTANCE, TransportMultiSearchAction.class);
        actions.register(ExplainAction.INSTANCE, TransportExplainAction.class);
        actions.register(ClearScrollAction.INSTANCE, TransportClearScrollAction.class);
        actions.register(RecoveryAction.INSTANCE, TransportRecoveryAction.class);
        actions.register(NodesReloadSecureSettingsAction.INSTANCE, TransportNodesReloadSecureSettingsAction.class);
        actions.register(AutoCreateAction.INSTANCE, AutoCreateAction.TransportAction.class);
        actions.register(ResolveIndexAction.INSTANCE, ResolveIndexAction.TransportAction.class);
        actions.register(AnalyzeIndexDiskUsageAction.INSTANCE, TransportAnalyzeIndexDiskUsageAction.class);
        actions.register(FieldUsageStatsAction.INSTANCE, TransportFieldUsageAction.class);

        // Data streams
        actions.register(ModifyDataStreamsAction.INSTANCE, ModifyDataStreamsTransportAction.class);

        // Indexed scripts
        actions.register(PutStoredScriptAction.INSTANCE, TransportPutStoredScriptAction.class);
        actions.register(GetStoredScriptAction.INSTANCE, TransportGetStoredScriptAction.class);
        actions.register(DeleteStoredScriptAction.INSTANCE, TransportDeleteStoredScriptAction.class);
        actions.register(GetScriptContextAction.INSTANCE, TransportGetScriptContextAction.class);
        actions.register(GetScriptLanguageAction.INSTANCE, TransportGetScriptLanguageAction.class);

        actions.register(FieldCapabilitiesAction.INSTANCE, TransportFieldCapabilitiesAction.class);

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

        // Dangling indices
        actions.register(ListDanglingIndicesAction.INSTANCE, TransportListDanglingIndicesAction.class);
        actions.register(ImportDanglingIndexAction.INSTANCE, TransportImportDanglingIndexAction.class);
        actions.register(DeleteDanglingIndexAction.INSTANCE, TransportDeleteDanglingIndexAction.class);
        actions.register(FindDanglingIndexAction.INSTANCE, TransportFindDanglingIndexAction.class);

        return unmodifiableMap(actions.getRegistry());
    }

    private ActionFilters setupActionFilters(List<ActionPlugin> actionPlugins) {
        return new ActionFilters(
            Collections.unmodifiableSet(actionPlugins.stream().flatMap(p -> p.getActionFilters().stream()).collect(Collectors.toSet()))
        );
    }

    public void initRestHandlers(Supplier<DiscoveryNodes> nodesInCluster) {
        List<AbstractCatAction> catActions = new ArrayList<>();
        Consumer<RestHandler> registerHandler = handler -> {
            if (handler instanceof AbstractCatAction) {
                catActions.add((AbstractCatAction) handler);
            }
            restController.registerHandler(handler);
        };
        registerHandler.accept(new RestAddVotingConfigExclusionAction());
        registerHandler.accept(new RestClearVotingConfigExclusionsAction());
        registerHandler.accept(new RestMainAction());
        registerHandler.accept(new RestNodesInfoAction(settingsFilter));
        registerHandler.accept(new RestRemoteClusterInfoAction());
        registerHandler.accept(new RestNodesStatsAction());
        registerHandler.accept(new RestNodesUsageAction());
        registerHandler.accept(new RestNodesHotThreadsAction());
        registerHandler.accept(new RestClusterAllocationExplainAction());
        registerHandler.accept(new RestClusterStatsAction());
        registerHandler.accept(new RestClusterStateAction(settingsFilter, threadPool));
        registerHandler.accept(new RestClusterHealthAction());
        registerHandler.accept(new RestClusterUpdateSettingsAction());
        registerHandler.accept(new RestClusterGetSettingsAction(settings, clusterSettings, settingsFilter));
        registerHandler.accept(new RestClusterRerouteAction(settingsFilter));
        registerHandler.accept(new RestClusterSearchShardsAction());
        registerHandler.accept(new RestPendingClusterTasksAction());
        registerHandler.accept(new RestPutRepositoryAction());
        registerHandler.accept(new RestGetRepositoriesAction(settingsFilter));
        registerHandler.accept(new RestDeleteRepositoryAction());
        registerHandler.accept(new RestVerifyRepositoryAction());
        registerHandler.accept(new RestCleanupRepositoryAction());
        registerHandler.accept(new RestGetSnapshotsAction(threadPool));
        registerHandler.accept(new RestCreateSnapshotAction());
        registerHandler.accept(new RestCloneSnapshotAction());
        registerHandler.accept(new RestRestoreSnapshotAction());
        registerHandler.accept(new RestDeleteSnapshotAction());
        registerHandler.accept(new RestSnapshotsStatusAction());
        registerHandler.accept(new RestSnapshottableFeaturesAction());
        registerHandler.accept(new RestResetFeatureStateAction());
        registerHandler.accept(new RestGetFeatureUpgradeStatusAction());
        registerHandler.accept(new RestPostFeatureUpgradeAction());
        registerHandler.accept(new RestGetIndicesAction(threadPool));
        registerHandler.accept(new RestIndicesStatsAction());
        registerHandler.accept(new RestIndicesSegmentsAction(threadPool));
        registerHandler.accept(new RestIndicesShardStoresAction());
        registerHandler.accept(new RestGetAliasesAction());
        registerHandler.accept(new RestIndexDeleteAliasesAction());
        registerHandler.accept(new RestIndexPutAliasAction());
        registerHandler.accept(new RestIndicesAliasesAction());
        registerHandler.accept(new RestCreateIndexAction());
        registerHandler.accept(new RestResizeHandler.RestShrinkIndexAction());
        registerHandler.accept(new RestResizeHandler.RestSplitIndexAction());
        registerHandler.accept(new RestResizeHandler.RestCloneIndexAction());
        registerHandler.accept(new RestRolloverIndexAction());
        registerHandler.accept(new RestDeleteIndexAction());
        registerHandler.accept(new RestCloseIndexAction());
        registerHandler.accept(new RestOpenIndexAction());
        registerHandler.accept(new RestAddIndexBlockAction());

        registerHandler.accept(new RestUpdateSettingsAction());
        registerHandler.accept(new RestGetSettingsAction());

        registerHandler.accept(new RestAnalyzeAction());
        registerHandler.accept(new RestGetIndexTemplateAction());
        registerHandler.accept(new RestPutIndexTemplateAction());
        registerHandler.accept(new RestDeleteIndexTemplateAction());
        registerHandler.accept(new RestPutComponentTemplateAction());
        registerHandler.accept(new RestGetComponentTemplateAction());
        registerHandler.accept(new RestDeleteComponentTemplateAction());
        registerHandler.accept(new RestPutComposableIndexTemplateAction());
        registerHandler.accept(new RestGetComposableIndexTemplateAction());
        registerHandler.accept(new RestDeleteComposableIndexTemplateAction());
        registerHandler.accept(new RestSimulateIndexTemplateAction());
        registerHandler.accept(new RestSimulateTemplateAction());

        registerHandler.accept(new RestPutMappingAction());
        registerHandler.accept(new RestGetMappingAction(threadPool));
        registerHandler.accept(new RestGetFieldMappingAction());

        registerHandler.accept(new RestRefreshAction());
        registerHandler.accept(new RestFlushAction());
        registerHandler.accept(new RestSyncedFlushAction());
        registerHandler.accept(new RestForceMergeAction());
        registerHandler.accept(new RestUpgradeActionDeprecated());
        registerHandler.accept(new RestUpgradeStatusActionDeprecated());
        registerHandler.accept(new RestClearIndicesCacheAction());
        registerHandler.accept(new RestResolveIndexAction());

        registerHandler.accept(new RestIndexAction());
        registerHandler.accept(new CreateHandler());
        registerHandler.accept(new AutoIdHandler(nodesInCluster));
        registerHandler.accept(new RestGetAction());
        registerHandler.accept(new RestGetSourceAction());
        registerHandler.accept(new RestMultiGetAction(settings));
        registerHandler.accept(new RestDeleteAction());
        registerHandler.accept(new RestCountAction());
        registerHandler.accept(new RestTermVectorsAction());
        registerHandler.accept(new RestMultiTermVectorsAction());
        registerHandler.accept(new RestBulkAction(settings));
        registerHandler.accept(new RestUpdateAction());

        registerHandler.accept(new RestSearchAction());
        registerHandler.accept(new RestSearchScrollAction());
        registerHandler.accept(new RestClearScrollAction());
        registerHandler.accept(new RestOpenPointInTimeAction());
        registerHandler.accept(new RestClosePointInTimeAction());
        registerHandler.accept(new RestMultiSearchAction(settings));

        registerHandler.accept(new RestValidateQueryAction());

        registerHandler.accept(new RestExplainAction());

        registerHandler.accept(new RestRecoveryAction());

        registerHandler.accept(new RestReloadSecureSettingsAction());

        // Data streams
        registerHandler.accept(new RestModifyDataStreamsAction());

        // Scripts API
        registerHandler.accept(new RestGetStoredScriptAction());
        registerHandler.accept(new RestPutStoredScriptAction());
        registerHandler.accept(new RestDeleteStoredScriptAction());
        registerHandler.accept(new RestGetScriptContextAction());
        registerHandler.accept(new RestGetScriptLanguageAction());

        registerHandler.accept(new RestFieldCapabilitiesAction());

        // Tasks API
        registerHandler.accept(new RestListTasksAction(nodesInCluster));
        registerHandler.accept(new RestGetTaskAction());
        registerHandler.accept(new RestCancelTasksAction(nodesInCluster));

        // Ingest API
        registerHandler.accept(new RestPutPipelineAction());
        registerHandler.accept(new RestGetPipelineAction());
        registerHandler.accept(new RestDeletePipelineAction());
        registerHandler.accept(new RestSimulatePipelineAction());

        // Dangling indices API
        registerHandler.accept(new RestListDanglingIndicesAction());
        registerHandler.accept(new RestImportDanglingIndexAction());
        registerHandler.accept(new RestDeleteDanglingIndexAction());

        // CAT API
        registerHandler.accept(new RestAllocationAction());
        registerHandler.accept(new RestShardsAction());
        registerHandler.accept(new RestMasterAction());
        registerHandler.accept(new RestNodesAction());
        registerHandler.accept(new RestTasksAction(nodesInCluster));
        registerHandler.accept(new RestIndicesAction());
        registerHandler.accept(new RestSegmentsAction());
        // Fully qualified to prevent interference with rest.action.count.RestCountAction
        registerHandler.accept(new org.elasticsearch.rest.action.cat.RestCountAction());
        // Fully qualified to prevent interference with rest.action.indices.RestRecoveryAction
        registerHandler.accept(new RestCatRecoveryAction());
        registerHandler.accept(new RestHealthAction());
        registerHandler.accept(new org.elasticsearch.rest.action.cat.RestPendingClusterTasksAction());
        registerHandler.accept(new RestAliasAction());
        registerHandler.accept(new RestThreadPoolAction());
        registerHandler.accept(new RestPluginsAction());
        registerHandler.accept(new RestFielddataAction());
        registerHandler.accept(new RestNodeAttrsAction());
        registerHandler.accept(new RestRepositoriesAction());
        registerHandler.accept(new RestSnapshotAction());
        registerHandler.accept(new RestTemplatesAction());
        registerHandler.accept(new RestAnalyzeIndexDiskUsageAction());
        registerHandler.accept(new RestFieldUsageStatsAction());

        for (ActionPlugin plugin : actionPlugins) {
            for (RestHandler handler : plugin.getRestHandlers(
                settings,
                restController,
                clusterSettings,
                indexScopedSettings,
                settingsFilter,
                indexNameExpressionResolver,
                nodesInCluster
            )) {
                registerHandler.accept(handler);
            }
        }
        registerHandler.accept(new RestCatAction(catActions));
    }

    @Override
    protected void configure() {
        bind(ActionFilters.class).toInstance(actionFilters);
        bind(DestructiveOperations.class).toInstance(destructiveOperations);
        bind(new TypeLiteral<RequestValidators<PutMappingRequest>>() {
        }).toInstance(mappingRequestValidators);
        bind(new TypeLiteral<RequestValidators<IndicesAliasesRequest>>() {
        }).toInstance(indicesAliasesRequestRequestValidators);

        if (false == transportClient) {
            // Supporting classes only used when not a transport client
            bind(AutoCreateIndex.class).toInstance(autoCreateIndex);
            bind(TransportLivenessAction.class).asEagerSingleton();

            // register ActionType -> transportAction Map used by NodeClient
            @SuppressWarnings("rawtypes")
            MapBinder<ActionType, TransportAction> transportActionsBinder = MapBinder.newMapBinder(
                binder(),
                ActionType.class,
                TransportAction.class
            );
            for (ActionHandler<?, ?> action : actions.values()) {
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
