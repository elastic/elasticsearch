/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.allocation.TransportClusterAllocationExplainAction;
import org.elasticsearch.action.admin.cluster.allocation.TransportDeleteDesiredBalanceAction;
import org.elasticsearch.action.admin.cluster.allocation.TransportGetAllocationStatsAction;
import org.elasticsearch.action.admin.cluster.allocation.TransportGetDesiredBalanceAction;
import org.elasticsearch.action.admin.cluster.configuration.TransportAddVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.TransportClearVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.coordination.ClusterFormationInfoAction;
import org.elasticsearch.action.admin.cluster.coordination.CoordinationDiagnosticsAction;
import org.elasticsearch.action.admin.cluster.coordination.MasterHistoryAction;
import org.elasticsearch.action.admin.cluster.desirednodes.GetDesiredNodesAction;
import org.elasticsearch.action.admin.cluster.desirednodes.TransportDeleteDesiredNodesAction;
import org.elasticsearch.action.admin.cluster.desirednodes.TransportGetDesiredNodesAction;
import org.elasticsearch.action.admin.cluster.desirednodes.TransportUpdateDesiredNodesAction;
import org.elasticsearch.action.admin.cluster.desirednodes.UpdateDesiredNodesAction;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.admin.cluster.node.capabilities.TransportNodesCapabilitiesAction;
import org.elasticsearch.action.admin.cluster.node.features.TransportNodesFeaturesAction;
import org.elasticsearch.action.admin.cluster.node.hotthreads.TransportNodesHotThreadsAction;
import org.elasticsearch.action.admin.cluster.node.info.TransportNodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.reload.TransportNodesReloadSecureSettingsAction;
import org.elasticsearch.action.admin.cluster.node.shutdown.PrevalidateNodeRemovalAction;
import org.elasticsearch.action.admin.cluster.node.shutdown.TransportPrevalidateNodeRemovalAction;
import org.elasticsearch.action.admin.cluster.node.shutdown.TransportPrevalidateShardPathAction;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.admin.cluster.node.usage.TransportNodesUsageAction;
import org.elasticsearch.action.admin.cluster.remote.RemoteClusterNodesAction;
import org.elasticsearch.action.admin.cluster.remote.TransportRemoteInfoAction;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.TransportCleanupRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.delete.TransportDeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesAction;
import org.elasticsearch.action.admin.cluster.repositories.get.TransportGetRepositoriesAction;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.verify.TransportVerifyRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryAction;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterGetSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.shards.TransportClusterSearchShardsAction;
import org.elasticsearch.action.admin.cluster.snapshots.clone.TransportCloneSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.TransportDeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateAction;
import org.elasticsearch.action.admin.cluster.snapshots.features.SnapshottableFeaturesAction;
import org.elasticsearch.action.admin.cluster.snapshots.features.TransportResetFeatureStateAction;
import org.elasticsearch.action.admin.cluster.snapshots.features.TransportSnapshottableFeaturesAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.TransportGetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.shard.TransportGetShardSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.TransportRestoreSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.status.TransportNodesSnapshotsStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.TransportSnapshotsStatusAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.TransportClusterStateAction;
import org.elasticsearch.action.admin.cluster.stats.TransportClusterStatsAction;
import org.elasticsearch.action.admin.cluster.storedscripts.GetScriptContextAction;
import org.elasticsearch.action.admin.cluster.storedscripts.GetScriptLanguageAction;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptAction;
import org.elasticsearch.action.admin.cluster.storedscripts.TransportDeleteStoredScriptAction;
import org.elasticsearch.action.admin.cluster.storedscripts.TransportGetScriptContextAction;
import org.elasticsearch.action.admin.cluster.storedscripts.TransportGetScriptLanguageAction;
import org.elasticsearch.action.admin.cluster.storedscripts.TransportGetStoredScriptAction;
import org.elasticsearch.action.admin.cluster.storedscripts.TransportPutStoredScriptAction;
import org.elasticsearch.action.admin.cluster.tasks.TransportPendingClusterTasksAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.TransportGetAliasesAction;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.analyze.TransportAnalyzeAction;
import org.elasticsearch.action.admin.indices.analyze.TransportReloadAnalyzersAction;
import org.elasticsearch.action.admin.indices.cache.clear.TransportClearIndicesCacheAction;
import org.elasticsearch.action.admin.indices.close.TransportCloseIndexAction;
import org.elasticsearch.action.admin.indices.close.TransportVerifyShardBeforeCloseAction;
import org.elasticsearch.action.admin.indices.create.AutoCreateAction;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.dangling.delete.TransportDeleteDanglingIndexAction;
import org.elasticsearch.action.admin.indices.dangling.find.TransportFindDanglingIndexAction;
import org.elasticsearch.action.admin.indices.dangling.import_index.TransportImportDanglingIndexAction;
import org.elasticsearch.action.admin.indices.dangling.list.TransportListDanglingIndicesAction;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.diskusage.TransportAnalyzeIndexDiskUsageAction;
import org.elasticsearch.action.admin.indices.flush.FlushAction;
import org.elasticsearch.action.admin.indices.flush.TransportFlushAction;
import org.elasticsearch.action.admin.indices.flush.TransportShardFlushAction;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.elasticsearch.action.admin.indices.forcemerge.TransportForceMergeAction;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.TransportGetIndexAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.TransportGetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.TransportGetFieldMappingsIndexAction;
import org.elasticsearch.action.admin.indices.mapping.get.TransportGetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.TransportAutoPutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.admin.indices.open.TransportOpenIndexAction;
import org.elasticsearch.action.admin.indices.readonly.TransportAddIndexBlockAction;
import org.elasticsearch.action.admin.indices.readonly.TransportVerifyShardIndexBlockAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.recovery.TransportRecoveryAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.TransportRefreshAction;
import org.elasticsearch.action.admin.indices.refresh.TransportShardRefreshAction;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
import org.elasticsearch.action.admin.indices.resolve.TransportResolveClusterAction;
import org.elasticsearch.action.admin.indices.rollover.LazyRolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.rollover.TransportRolloverAction;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsAction;
import org.elasticsearch.action.admin.indices.segments.TransportIndicesSegmentsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.TransportGetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.shards.TransportIndicesShardStoresAction;
import org.elasticsearch.action.admin.indices.shrink.ResizeAction;
import org.elasticsearch.action.admin.indices.shrink.TransportResizeAction;
import org.elasticsearch.action.admin.indices.stats.FieldUsageStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.TransportFieldUsageAction;
import org.elasticsearch.action.admin.indices.stats.TransportIndicesStatsAction;
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
import org.elasticsearch.action.admin.indices.template.put.TransportPutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.action.admin.indices.validate.query.TransportValidateQueryAction;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.elasticsearch.action.bulk.IncrementalBulkService;
import org.elasticsearch.action.bulk.SimulateBulkAction;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.bulk.TransportSimulateBulkAction;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.explain.TransportExplainAction;
import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.get.TransportMultiGetAction;
import org.elasticsearch.action.get.TransportShardMultiGetAction;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.ingest.DeletePipelineTransportAction;
import org.elasticsearch.action.ingest.GetPipelineAction;
import org.elasticsearch.action.ingest.GetPipelineTransportAction;
import org.elasticsearch.action.ingest.PutPipelineTransportAction;
import org.elasticsearch.action.ingest.SimulatePipelineAction;
import org.elasticsearch.action.ingest.SimulatePipelineTransportAction;
import org.elasticsearch.action.search.RestClosePointInTimeAction;
import org.elasticsearch.action.search.RestOpenPointInTimeAction;
import org.elasticsearch.action.search.TransportClearScrollAction;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportMultiSearchAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.search.TransportSearchScrollAction;
import org.elasticsearch.action.search.TransportSearchShardsAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.action.support.MappedActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.synonyms.DeleteSynonymRuleAction;
import org.elasticsearch.action.synonyms.DeleteSynonymsAction;
import org.elasticsearch.action.synonyms.GetSynonymRuleAction;
import org.elasticsearch.action.synonyms.GetSynonymsAction;
import org.elasticsearch.action.synonyms.GetSynonymsSetsAction;
import org.elasticsearch.action.synonyms.PutSynonymRuleAction;
import org.elasticsearch.action.synonyms.PutSynonymsAction;
import org.elasticsearch.action.synonyms.TransportDeleteSynonymRuleAction;
import org.elasticsearch.action.synonyms.TransportDeleteSynonymsAction;
import org.elasticsearch.action.synonyms.TransportGetSynonymRuleAction;
import org.elasticsearch.action.synonyms.TransportGetSynonymsAction;
import org.elasticsearch.action.synonyms.TransportGetSynonymsSetsAction;
import org.elasticsearch.action.synonyms.TransportPutSynonymRuleAction;
import org.elasticsearch.action.synonyms.TransportPutSynonymsAction;
import org.elasticsearch.action.termvectors.MultiTermVectorsAction;
import org.elasticsearch.action.termvectors.TermVectorsAction;
import org.elasticsearch.action.termvectors.TransportMultiTermVectorsAction;
import org.elasticsearch.action.termvectors.TransportShardMultiTermsVectorAction;
import org.elasticsearch.action.termvectors.TransportTermVectorsAction;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.ProjectIdResolver;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.NamedRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards;
import org.elasticsearch.health.GetHealthAction;
import org.elasticsearch.health.RestGetHealthAction;
import org.elasticsearch.health.node.FetchHealthInfoCacheAction;
import org.elasticsearch.health.node.UpdateHealthInfoCacheAction;
import org.elasticsearch.health.stats.HealthApiStatsAction;
import org.elasticsearch.health.stats.HealthApiStatsTransportAction;
import org.elasticsearch.http.HttpPreRequest;
import org.elasticsearch.index.seqno.GlobalCheckpointSyncAction;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetadata;
import org.elasticsearch.injection.guice.AbstractModule;
import org.elasticsearch.injection.guice.TypeLiteral;
import org.elasticsearch.injection.guice.multibindings.MapBinder;
import org.elasticsearch.monitor.metrics.IndexModeStatsActionType;
import org.elasticsearch.persistent.CompletionPersistentTaskAction;
import org.elasticsearch.persistent.RemovePersistentTaskAction;
import org.elasticsearch.persistent.StartPersistentTaskAction;
import org.elasticsearch.persistent.UpdatePersistentTaskStatusAction;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;
import org.elasticsearch.plugins.interceptor.RestServerActionPlugin;
import org.elasticsearch.plugins.internal.RestExtension;
import org.elasticsearch.repositories.VerifyNodeRepositoryAction;
import org.elasticsearch.repositories.VerifyNodeRepositoryCoordinationAction;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.service.ReservedClusterStateService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestHeaderDefinition;
import org.elasticsearch.rest.RestInterceptor;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.action.RestFieldCapabilitiesAction;
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
import org.elasticsearch.rest.action.admin.cluster.RestDeleteDesiredBalanceAction;
import org.elasticsearch.rest.action.admin.cluster.RestDeleteDesiredNodesAction;
import org.elasticsearch.rest.action.admin.cluster.RestDeleteRepositoryAction;
import org.elasticsearch.rest.action.admin.cluster.RestDeleteSnapshotAction;
import org.elasticsearch.rest.action.admin.cluster.RestDeleteStoredScriptAction;
import org.elasticsearch.rest.action.admin.cluster.RestGetDesiredBalanceAction;
import org.elasticsearch.rest.action.admin.cluster.RestGetDesiredNodesAction;
import org.elasticsearch.rest.action.admin.cluster.RestGetRepositoriesAction;
import org.elasticsearch.rest.action.admin.cluster.RestGetScriptContextAction;
import org.elasticsearch.rest.action.admin.cluster.RestGetScriptLanguageAction;
import org.elasticsearch.rest.action.admin.cluster.RestGetSnapshotsAction;
import org.elasticsearch.rest.action.admin.cluster.RestGetStoredScriptAction;
import org.elasticsearch.rest.action.admin.cluster.RestGetTaskAction;
import org.elasticsearch.rest.action.admin.cluster.RestListTasksAction;
import org.elasticsearch.rest.action.admin.cluster.RestNodesCapabilitiesAction;
import org.elasticsearch.rest.action.admin.cluster.RestNodesHotThreadsAction;
import org.elasticsearch.rest.action.admin.cluster.RestNodesInfoAction;
import org.elasticsearch.rest.action.admin.cluster.RestNodesStatsAction;
import org.elasticsearch.rest.action.admin.cluster.RestNodesUsageAction;
import org.elasticsearch.rest.action.admin.cluster.RestPendingClusterTasksAction;
import org.elasticsearch.rest.action.admin.cluster.RestPrevalidateNodeRemovalAction;
import org.elasticsearch.rest.action.admin.cluster.RestPutRepositoryAction;
import org.elasticsearch.rest.action.admin.cluster.RestPutStoredScriptAction;
import org.elasticsearch.rest.action.admin.cluster.RestReloadSecureSettingsAction;
import org.elasticsearch.rest.action.admin.cluster.RestRemoteClusterInfoAction;
import org.elasticsearch.rest.action.admin.cluster.RestResetFeatureStateAction;
import org.elasticsearch.rest.action.admin.cluster.RestRestoreSnapshotAction;
import org.elasticsearch.rest.action.admin.cluster.RestSnapshotsStatusAction;
import org.elasticsearch.rest.action.admin.cluster.RestSnapshottableFeaturesAction;
import org.elasticsearch.rest.action.admin.cluster.RestUpdateDesiredNodesAction;
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
import org.elasticsearch.rest.action.admin.indices.RestReloadAnalyzersAction;
import org.elasticsearch.rest.action.admin.indices.RestResizeHandler;
import org.elasticsearch.rest.action.admin.indices.RestResolveClusterAction;
import org.elasticsearch.rest.action.admin.indices.RestResolveIndexAction;
import org.elasticsearch.rest.action.admin.indices.RestRolloverIndexAction;
import org.elasticsearch.rest.action.admin.indices.RestSimulateIndexTemplateAction;
import org.elasticsearch.rest.action.admin.indices.RestSimulateTemplateAction;
import org.elasticsearch.rest.action.admin.indices.RestUpdateSettingsAction;
import org.elasticsearch.rest.action.admin.indices.RestValidateQueryAction;
import org.elasticsearch.rest.action.cat.AbstractCatAction;
import org.elasticsearch.rest.action.cat.RestAliasAction;
import org.elasticsearch.rest.action.cat.RestAllocationAction;
import org.elasticsearch.rest.action.cat.RestCatAction;
import org.elasticsearch.rest.action.cat.RestCatComponentTemplateAction;
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
import org.elasticsearch.rest.action.document.RestIndexAction.AutoIdHandler;
import org.elasticsearch.rest.action.document.RestIndexAction.CreateHandler;
import org.elasticsearch.rest.action.document.RestMultiGetAction;
import org.elasticsearch.rest.action.document.RestMultiTermVectorsAction;
import org.elasticsearch.rest.action.document.RestTermVectorsAction;
import org.elasticsearch.rest.action.document.RestUpdateAction;
import org.elasticsearch.rest.action.info.RestClusterInfoAction;
import org.elasticsearch.rest.action.ingest.RestDeletePipelineAction;
import org.elasticsearch.rest.action.ingest.RestGetPipelineAction;
import org.elasticsearch.rest.action.ingest.RestPutPipelineAction;
import org.elasticsearch.rest.action.ingest.RestSimulateIngestAction;
import org.elasticsearch.rest.action.ingest.RestSimulatePipelineAction;
import org.elasticsearch.rest.action.search.RestClearScrollAction;
import org.elasticsearch.rest.action.search.RestCountAction;
import org.elasticsearch.rest.action.search.RestExplainAction;
import org.elasticsearch.rest.action.search.RestKnnSearchAction;
import org.elasticsearch.rest.action.search.RestMultiSearchAction;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.rest.action.search.RestSearchScrollAction;
import org.elasticsearch.rest.action.synonyms.RestDeleteSynonymRuleAction;
import org.elasticsearch.rest.action.synonyms.RestDeleteSynonymsAction;
import org.elasticsearch.rest.action.synonyms.RestGetSynonymRuleAction;
import org.elasticsearch.rest.action.synonyms.RestGetSynonymsAction;
import org.elasticsearch.rest.action.synonyms.RestGetSynonymsSetsAction;
import org.elasticsearch.rest.action.synonyms.RestPutSynonymRuleAction;
import org.elasticsearch.rest.action.synonyms.RestPutSynonymsAction;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.usage.UsageService;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableMap;

/**
 * Builds and binds the generic action map, all {@link TransportAction}s, and {@link ActionFilters}.
 */
public class ActionModule extends AbstractModule {

    private static final Logger logger = LogManager.getLogger(ActionModule.class);
    /**
     *  This RestHandler is used as a placeholder for any routes that are unreachable (i.e. have no ServerlessScope annotation) when
     *  running in serverless mode. It does nothing, and its handleRequest method is never called. It just provides a way to register the
     *  routes so that we know they do exist.
     */
    private static final RestHandler placeholderRestHandler = (request, channel, client) -> {};

    private final Settings settings;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final IndexScopedSettings indexScopedSettings;
    private final ClusterSettings clusterSettings;
    private final SettingsFilter settingsFilter;
    private final List<ActionPlugin> actionPlugins;
    private final Map<String, ActionHandler> actions;
    private final ActionFilters actionFilters;
    private final IncrementalBulkService bulkService;
    private final ProjectIdResolver projectIdResolver;
    private final AutoCreateIndex autoCreateIndex;
    private final DestructiveOperations destructiveOperations;
    private final RestController restController;
    /** Rest headers that are copied to internal requests made during a rest request. */
    private final Set<RestHeaderDefinition> headersToCopy;
    private final RequestValidators<PutMappingRequest> mappingRequestValidators;
    private final RequestValidators<IndicesAliasesRequest> indicesAliasesRequestRequestValidators;
    private final ThreadPool threadPool;
    private final ReservedClusterStateService reservedClusterStateService;
    private final RestExtension restExtension;

    public ActionModule(
        Settings settings,
        IndexNameExpressionResolver indexNameExpressionResolver,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexScopedSettings indexScopedSettings,
        ClusterSettings clusterSettings,
        SettingsFilter settingsFilter,
        ThreadPool threadPool,
        List<ActionPlugin> actionPlugins,
        NodeClient nodeClient,
        CircuitBreakerService circuitBreakerService,
        UsageService usageService,
        SystemIndices systemIndices,
        TelemetryProvider telemetryProvider,
        ClusterService clusterService,
        RerouteService rerouteService,
        List<ReservedClusterStateHandler<ClusterState, ?>> reservedClusterStateHandlers,
        List<ReservedClusterStateHandler<ProjectMetadata, ?>> reservedProjectStateHandlers,
        RestExtension restExtension,
        IncrementalBulkService bulkService,
        ProjectIdResolver projectIdResolver
    ) {
        this.settings = settings;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.indexScopedSettings = indexScopedSettings;
        this.clusterSettings = clusterSettings;
        this.settingsFilter = settingsFilter;
        this.actionPlugins = actionPlugins;
        this.threadPool = threadPool;
        actions = setupActions(actionPlugins);
        actionFilters = setupActionFilters(actionPlugins);
        this.bulkService = bulkService;
        this.projectIdResolver = projectIdResolver;
        autoCreateIndex = new AutoCreateIndex(settings, clusterSettings, indexNameExpressionResolver, systemIndices);
        destructiveOperations = new DestructiveOperations(settings, clusterSettings);
        Set<RestHeaderDefinition> headers = Stream.concat(
            actionPlugins.stream().flatMap(p -> p.getRestHeaders().stream()),
            Stream.of(
                new RestHeaderDefinition(Task.X_OPAQUE_ID_HTTP_HEADER, false),
                new RestHeaderDefinition(Task.TRACE_STATE, false),
                new RestHeaderDefinition(Task.TRACE_PARENT_HTTP_HEADER, false),
                new RestHeaderDefinition(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER, false)
            )
        ).collect(Collectors.toSet());
        final RestInterceptor restInterceptor = getRestServerComponent(
            "REST interceptor",
            actionPlugins,
            restPlugin -> restPlugin.getRestHandlerInterceptor(threadPool.getThreadContext())
        );
        mappingRequestValidators = new RequestValidators<>(
            actionPlugins.stream().flatMap(p -> p.mappingRequestValidators().stream()).toList()
        );
        indicesAliasesRequestRequestValidators = new RequestValidators<>(
            actionPlugins.stream().flatMap(p -> p.indicesAliasesRequestValidators().stream()).toList()
        );
        headersToCopy = headers;

        var customController = getRestServerComponent(
            "REST controller",
            actionPlugins,
            restPlugin -> restPlugin.getRestController(restInterceptor, nodeClient, circuitBreakerService, usageService, telemetryProvider)
        );
        if (customController != null) {
            restController = customController;
        } else {
            restController = new RestController(restInterceptor, nodeClient, circuitBreakerService, usageService, telemetryProvider);
        }
        reservedClusterStateService = new ReservedClusterStateService(
            clusterService,
            rerouteService,
            reservedClusterStateHandlers,
            reservedProjectStateHandlers
        );
        this.restExtension = restExtension;
    }

    private static <T> T getRestServerComponent(
        String type,
        List<ActionPlugin> actionPlugins,
        Function<RestServerActionPlugin, T> function
    ) {
        T result = null;
        for (ActionPlugin plugin : actionPlugins) {
            if (plugin instanceof RestServerActionPlugin restPlugin) {
                var newInstance = function.apply(restPlugin);
                if (newInstance != null) {
                    logger.debug("Using custom {} from plugin {}", type, plugin.getClass().getName());
                    if (isInternalPlugin(plugin) == false) {
                        throw new IllegalArgumentException(
                            "The "
                                + plugin.getClass().getName()
                                + " plugin tried to install a custom "
                                + type
                                + ". This functionality is not available to external plugins."
                        );
                    }
                    if (result != null) {
                        throw new IllegalArgumentException("Cannot have more than one plugin implementing a " + type);
                    }
                    result = newInstance;
                }
            }
        }
        return result;
    }

    private static boolean isInternalPlugin(ActionPlugin plugin) {
        final String canonicalName = plugin.getClass().getCanonicalName();
        if (canonicalName == null) {
            return false;
        }
        return canonicalName.startsWith("org.elasticsearch.xpack.") || canonicalName.startsWith("co.elastic.elasticsearch.");
    }

    /**
     * Certain request header values need to be copied in the thread context under which request handlers are to be dispatched.
     * Careful that this method modifies the thread context. The thread context must be reinstated after the request handler
     * finishes and returns.
     */
    public void copyRequestHeadersToThreadContext(HttpPreRequest request, ThreadContext threadContext) {
        // the request's thread-context must always be populated (by calling this method) before undergoing any request related processing
        // we use this opportunity to first record the request processing start time
        threadContext.putTransient(Task.TRACE_START_TIME, Instant.ofEpochMilli(System.currentTimeMillis()));
        for (final RestHeaderDefinition restHeader : headersToCopy) {
            final String name = restHeader.getName();
            final List<String> headerValues = request.getHeaders().get(name);
            if (headerValues != null && headerValues.isEmpty() == false) {
                final List<String> distinctHeaderValues = headerValues.stream().distinct().toList();
                if (restHeader.isMultiValueAllowed() == false && distinctHeaderValues.size() > 1) {
                    throw new IllegalArgumentException("multiple values for single-valued header [" + name + "].");
                } else if (name.equals(Task.TRACE_PARENT_HTTP_HEADER)) {
                    String traceparent = distinctHeaderValues.get(0);
                    Optional<String> traceId = RestUtils.extractTraceId(traceparent);
                    if (traceId.isPresent()) {
                        threadContext.putHeader(Task.TRACE_ID, traceId.get());
                        threadContext.putTransient("parent_" + Task.TRACE_PARENT_HTTP_HEADER, traceparent);
                    }
                } else if (name.equals(Task.TRACE_STATE)) {
                    threadContext.putTransient("parent_" + Task.TRACE_STATE, distinctHeaderValues.get(0));
                } else {
                    threadContext.putHeader(name, String.join(",", distinctHeaderValues));
                }
            }
        }
    }

    public Map<String, ActionHandler> getActions() {
        return actions;
    }

    static Map<String, ActionHandler> setupActions(List<ActionPlugin> actionPlugins) {
        // Subclass NamedRegistry for easy registration
        class ActionRegistry extends NamedRegistry<ActionHandler> {
            ActionRegistry() {
                super("action");
            }

            public void register(ActionHandler handler) {
                register(handler.getAction().name(), handler);
            }

            public <Request extends ActionRequest, Response extends ActionResponse> void register(
                ActionType<Response> action,
                Class<? extends TransportAction<Request, Response>> transportAction
            ) {
                register(new ActionHandler(action, transportAction));
            }
        }
        ActionRegistry actions = new ActionRegistry();

        actions.register(TransportNodesInfoAction.TYPE, TransportNodesInfoAction.class);
        actions.register(TransportRemoteInfoAction.TYPE, TransportRemoteInfoAction.class);
        actions.register(TransportNodesCapabilitiesAction.TYPE, TransportNodesCapabilitiesAction.class);
        actions.register(TransportNodesFeaturesAction.TYPE, TransportNodesFeaturesAction.class);
        actions.register(RemoteClusterNodesAction.TYPE, RemoteClusterNodesAction.TransportAction.class);
        actions.register(TransportNodesStatsAction.TYPE, TransportNodesStatsAction.class);
        actions.register(IndexModeStatsActionType.TYPE, IndexModeStatsActionType.TransportAction.class);
        actions.register(TransportNodesUsageAction.TYPE, TransportNodesUsageAction.class);
        actions.register(TransportNodesHotThreadsAction.TYPE, TransportNodesHotThreadsAction.class);
        actions.register(TransportListTasksAction.TYPE, TransportListTasksAction.class);
        actions.register(TransportGetTaskAction.TYPE, TransportGetTaskAction.class);
        actions.register(TransportCancelTasksAction.TYPE, TransportCancelTasksAction.class);
        actions.register(GetHealthAction.INSTANCE, GetHealthAction.LocalAction.class);
        actions.register(PrevalidateNodeRemovalAction.INSTANCE, TransportPrevalidateNodeRemovalAction.class);
        actions.register(HealthApiStatsAction.INSTANCE, HealthApiStatsTransportAction.class);

        actions.register(TransportAddVotingConfigExclusionsAction.TYPE, TransportAddVotingConfigExclusionsAction.class);
        actions.register(TransportClearVotingConfigExclusionsAction.TYPE, TransportClearVotingConfigExclusionsAction.class);
        actions.register(TransportClusterAllocationExplainAction.TYPE, TransportClusterAllocationExplainAction.class);
        actions.register(TransportGetAllocationStatsAction.TYPE, TransportGetAllocationStatsAction.class);
        actions.register(TransportGetDesiredBalanceAction.TYPE, TransportGetDesiredBalanceAction.class);
        actions.register(TransportDeleteDesiredBalanceAction.TYPE, TransportDeleteDesiredBalanceAction.class);
        actions.register(TransportClusterStatsAction.TYPE, TransportClusterStatsAction.class);
        actions.register(ClusterStateAction.INSTANCE, TransportClusterStateAction.class);
        actions.register(TransportClusterHealthAction.TYPE, TransportClusterHealthAction.class);
        actions.register(ClusterUpdateSettingsAction.INSTANCE, TransportClusterUpdateSettingsAction.class);
        actions.register(ClusterGetSettingsAction.INSTANCE, TransportClusterGetSettingsAction.class);
        actions.register(TransportClusterRerouteAction.TYPE, TransportClusterRerouteAction.class);
        actions.register(TransportClusterSearchShardsAction.TYPE, TransportClusterSearchShardsAction.class);
        actions.register(ClusterFormationInfoAction.INSTANCE, ClusterFormationInfoAction.TransportAction.class);
        actions.register(TransportPendingClusterTasksAction.TYPE, TransportPendingClusterTasksAction.class);
        actions.register(TransportPutRepositoryAction.TYPE, TransportPutRepositoryAction.class);
        actions.register(GetRepositoriesAction.INSTANCE, TransportGetRepositoriesAction.class);
        actions.register(TransportDeleteRepositoryAction.TYPE, TransportDeleteRepositoryAction.class);
        actions.register(VerifyRepositoryAction.INSTANCE, TransportVerifyRepositoryAction.class);
        actions.register(VerifyNodeRepositoryCoordinationAction.TYPE, VerifyNodeRepositoryCoordinationAction.LocalAction.class);
        actions.register(VerifyNodeRepositoryAction.TYPE, VerifyNodeRepositoryAction.TransportAction.class);
        actions.register(TransportCleanupRepositoryAction.TYPE, TransportCleanupRepositoryAction.class);
        actions.register(TransportGetSnapshotsAction.TYPE, TransportGetSnapshotsAction.class);
        actions.register(TransportDeleteSnapshotAction.TYPE, TransportDeleteSnapshotAction.class);
        actions.register(TransportCreateSnapshotAction.TYPE, TransportCreateSnapshotAction.class);
        actions.register(TransportCloneSnapshotAction.TYPE, TransportCloneSnapshotAction.class);
        actions.register(TransportRestoreSnapshotAction.TYPE, TransportRestoreSnapshotAction.class);
        actions.register(TransportSnapshotsStatusAction.TYPE, TransportSnapshotsStatusAction.class);
        actions.register(SnapshottableFeaturesAction.INSTANCE, TransportSnapshottableFeaturesAction.class);
        actions.register(ResetFeatureStateAction.INSTANCE, TransportResetFeatureStateAction.class);
        actions.register(TransportGetShardSnapshotAction.TYPE, TransportGetShardSnapshotAction.class);
        actions.register(IndicesStatsAction.INSTANCE, TransportIndicesStatsAction.class);
        actions.register(IndicesSegmentsAction.INSTANCE, TransportIndicesSegmentsAction.class);
        actions.register(TransportIndicesShardStoresAction.TYPE, TransportIndicesShardStoresAction.class);
        actions.register(TransportCreateIndexAction.TYPE, TransportCreateIndexAction.class);
        actions.register(ResizeAction.INSTANCE, TransportResizeAction.class);
        actions.register(RolloverAction.INSTANCE, TransportRolloverAction.class);
        actions.register(LazyRolloverAction.INSTANCE, LazyRolloverAction.TransportLazyRolloverAction.class);
        actions.register(TransportDeleteIndexAction.TYPE, TransportDeleteIndexAction.class);
        actions.register(GetIndexAction.INSTANCE, TransportGetIndexAction.class);
        actions.register(OpenIndexAction.INSTANCE, TransportOpenIndexAction.class);
        actions.register(TransportCloseIndexAction.TYPE, TransportCloseIndexAction.class);
        actions.register(TransportAddIndexBlockAction.TYPE, TransportAddIndexBlockAction.class);
        actions.register(GetMappingsAction.INSTANCE, TransportGetMappingsAction.class);
        actions.register(GetFieldMappingsAction.INSTANCE, TransportGetFieldMappingsAction.class);
        actions.register(TransportGetFieldMappingsIndexAction.TYPE, TransportGetFieldMappingsIndexAction.class);
        actions.register(TransportPutMappingAction.TYPE, TransportPutMappingAction.class);
        actions.register(TransportAutoPutMappingAction.TYPE, TransportAutoPutMappingAction.class);
        actions.register(TransportIndicesAliasesAction.TYPE, TransportIndicesAliasesAction.class);
        actions.register(TransportUpdateSettingsAction.TYPE, TransportUpdateSettingsAction.class);
        actions.register(AnalyzeAction.INSTANCE, TransportAnalyzeAction.class);
        actions.register(TransportReloadAnalyzersAction.TYPE, TransportReloadAnalyzersAction.class);
        actions.register(TransportPutIndexTemplateAction.TYPE, TransportPutIndexTemplateAction.class);
        actions.register(GetIndexTemplatesAction.INSTANCE, TransportGetIndexTemplatesAction.class);
        actions.register(TransportDeleteIndexTemplateAction.TYPE, TransportDeleteIndexTemplateAction.class);
        actions.register(PutComponentTemplateAction.INSTANCE, TransportPutComponentTemplateAction.class);
        actions.register(GetComponentTemplateAction.INSTANCE, TransportGetComponentTemplateAction.class);
        actions.register(TransportDeleteComponentTemplateAction.TYPE, TransportDeleteComponentTemplateAction.class);
        actions.register(TransportPutComposableIndexTemplateAction.TYPE, TransportPutComposableIndexTemplateAction.class);
        actions.register(GetComposableIndexTemplateAction.INSTANCE, TransportGetComposableIndexTemplateAction.class);
        actions.register(TransportDeleteComposableIndexTemplateAction.TYPE, TransportDeleteComposableIndexTemplateAction.class);
        actions.register(SimulateIndexTemplateAction.INSTANCE, TransportSimulateIndexTemplateAction.class);
        actions.register(SimulateTemplateAction.INSTANCE, TransportSimulateTemplateAction.class);
        actions.register(ValidateQueryAction.INSTANCE, TransportValidateQueryAction.class);
        actions.register(RefreshAction.INSTANCE, TransportRefreshAction.class);
        actions.register(FlushAction.INSTANCE, TransportFlushAction.class);
        actions.register(ForceMergeAction.INSTANCE, TransportForceMergeAction.class);
        actions.register(TransportClearIndicesCacheAction.TYPE, TransportClearIndicesCacheAction.class);
        actions.register(GetAliasesAction.INSTANCE, TransportGetAliasesAction.class);
        actions.register(GetSettingsAction.INSTANCE, TransportGetSettingsAction.class);

        actions.register(TransportIndexAction.TYPE, TransportIndexAction.class);
        actions.register(TransportGetAction.TYPE, TransportGetAction.class);
        actions.register(TermVectorsAction.INSTANCE, TransportTermVectorsAction.class);
        actions.register(MultiTermVectorsAction.INSTANCE, TransportMultiTermVectorsAction.class);
        actions.register(TransportShardMultiTermsVectorAction.TYPE, TransportShardMultiTermsVectorAction.class);
        actions.register(TransportDeleteAction.TYPE, TransportDeleteAction.class);
        actions.register(TransportUpdateAction.TYPE, TransportUpdateAction.class);
        actions.register(TransportMultiGetAction.TYPE, TransportMultiGetAction.class);
        actions.register(TransportShardMultiGetAction.TYPE, TransportShardMultiGetAction.class);
        actions.register(TransportBulkAction.TYPE, TransportBulkAction.class);
        actions.register(SimulateBulkAction.INSTANCE, TransportSimulateBulkAction.class);
        actions.register(TransportShardBulkAction.TYPE, TransportShardBulkAction.class);
        actions.register(TransportSearchAction.TYPE, TransportSearchAction.class);
        actions.register(TransportSearchScrollAction.TYPE, TransportSearchScrollAction.class);
        actions.register(TransportOpenPointInTimeAction.TYPE, TransportOpenPointInTimeAction.class);
        actions.register(TransportClosePointInTimeAction.TYPE, TransportClosePointInTimeAction.class);
        actions.register(TransportSearchShardsAction.TYPE, TransportSearchShardsAction.class);
        actions.register(TransportMultiSearchAction.TYPE, TransportMultiSearchAction.class);
        actions.register(TransportExplainAction.TYPE, TransportExplainAction.class);
        actions.register(TransportClearScrollAction.TYPE, TransportClearScrollAction.class);
        actions.register(RecoveryAction.INSTANCE, TransportRecoveryAction.class);
        actions.register(TransportNodesReloadSecureSettingsAction.TYPE, TransportNodesReloadSecureSettingsAction.class);
        actions.register(AutoCreateAction.INSTANCE, AutoCreateAction.TransportAction.class);
        actions.register(ResolveIndexAction.INSTANCE, ResolveIndexAction.TransportAction.class);
        actions.register(TransportResolveClusterAction.TYPE, TransportResolveClusterAction.class);
        actions.register(TransportAnalyzeIndexDiskUsageAction.TYPE, TransportAnalyzeIndexDiskUsageAction.class);
        actions.register(FieldUsageStatsAction.INSTANCE, TransportFieldUsageAction.class);
        actions.register(MasterHistoryAction.INSTANCE, MasterHistoryAction.TransportAction.class);
        actions.register(CoordinationDiagnosticsAction.INSTANCE, CoordinationDiagnosticsAction.TransportAction.class);

        // Indexed scripts
        actions.register(TransportPutStoredScriptAction.TYPE, TransportPutStoredScriptAction.class);
        actions.register(GetStoredScriptAction.INSTANCE, TransportGetStoredScriptAction.class);
        actions.register(TransportDeleteStoredScriptAction.TYPE, TransportDeleteStoredScriptAction.class);
        actions.register(GetScriptContextAction.INSTANCE, TransportGetScriptContextAction.class);
        actions.register(GetScriptLanguageAction.INSTANCE, TransportGetScriptLanguageAction.class);

        actions.register(TransportFieldCapabilitiesAction.TYPE, TransportFieldCapabilitiesAction.class);

        actions.register(PutPipelineTransportAction.TYPE, PutPipelineTransportAction.class);
        actions.register(GetPipelineAction.INSTANCE, GetPipelineTransportAction.class);
        actions.register(DeletePipelineTransportAction.TYPE, DeletePipelineTransportAction.class);
        actions.register(SimulatePipelineAction.INSTANCE, SimulatePipelineTransportAction.class);

        actionPlugins.stream().flatMap(p -> p.getActions().stream()).forEach(actions::register);

        // Persistent tasks:
        actions.register(StartPersistentTaskAction.INSTANCE, StartPersistentTaskAction.TransportAction.class);
        actions.register(UpdatePersistentTaskStatusAction.INSTANCE, UpdatePersistentTaskStatusAction.TransportAction.class);
        actions.register(CompletionPersistentTaskAction.INSTANCE, CompletionPersistentTaskAction.TransportAction.class);
        actions.register(RemovePersistentTaskAction.INSTANCE, RemovePersistentTaskAction.TransportAction.class);

        // retention leases
        actions.register(RetentionLeaseActions.ADD, RetentionLeaseActions.TransportAddAction.class);
        actions.register(RetentionLeaseActions.RENEW, RetentionLeaseActions.TransportRenewAction.class);
        actions.register(RetentionLeaseActions.REMOVE, RetentionLeaseActions.TransportRemoveAction.class);

        // Dangling indices
        actions.register(TransportListDanglingIndicesAction.TYPE, TransportListDanglingIndicesAction.class);
        actions.register(TransportImportDanglingIndexAction.TYPE, TransportImportDanglingIndexAction.class);
        actions.register(TransportDeleteDanglingIndexAction.TYPE, TransportDeleteDanglingIndexAction.class);
        actions.register(TransportFindDanglingIndexAction.TYPE, TransportFindDanglingIndexAction.class);

        // internal actions
        actions.register(GlobalCheckpointSyncAction.TYPE, GlobalCheckpointSyncAction.class);
        actions.register(TransportNodesSnapshotsStatus.TYPE, TransportNodesSnapshotsStatus.class);
        actions.register(TransportVerifyShardBeforeCloseAction.TYPE, TransportVerifyShardBeforeCloseAction.class);
        actions.register(TransportVerifyShardIndexBlockAction.TYPE, TransportVerifyShardIndexBlockAction.class);
        actions.register(TransportNodesListGatewayStartedShards.TYPE, TransportNodesListGatewayStartedShards.class);
        actions.register(TransportNodesListShardStoreMetadata.TYPE, TransportNodesListShardStoreMetadata.class);
        actions.register(TransportShardFlushAction.TYPE, TransportShardFlushAction.class);
        actions.register(TransportShardRefreshAction.TYPE, TransportShardRefreshAction.class);
        actions.register(TransportPrevalidateShardPathAction.TYPE, TransportPrevalidateShardPathAction.class);

        // desired nodes
        actions.register(GetDesiredNodesAction.INSTANCE, TransportGetDesiredNodesAction.class);
        actions.register(UpdateDesiredNodesAction.INSTANCE, TransportUpdateDesiredNodesAction.class);
        actions.register(TransportDeleteDesiredNodesAction.TYPE, TransportDeleteDesiredNodesAction.class);

        actions.register(UpdateHealthInfoCacheAction.INSTANCE, UpdateHealthInfoCacheAction.TransportAction.class);
        actions.register(FetchHealthInfoCacheAction.INSTANCE, FetchHealthInfoCacheAction.TransportAction.class);

        // Synonyms
        actions.register(PutSynonymsAction.INSTANCE, TransportPutSynonymsAction.class);
        actions.register(GetSynonymsAction.INSTANCE, TransportGetSynonymsAction.class);
        actions.register(DeleteSynonymsAction.INSTANCE, TransportDeleteSynonymsAction.class);
        actions.register(GetSynonymsSetsAction.INSTANCE, TransportGetSynonymsSetsAction.class);
        actions.register(PutSynonymRuleAction.INSTANCE, TransportPutSynonymRuleAction.class);
        actions.register(GetSynonymRuleAction.INSTANCE, TransportGetSynonymRuleAction.class);
        actions.register(DeleteSynonymRuleAction.INSTANCE, TransportDeleteSynonymRuleAction.class);

        return unmodifiableMap(actions.getRegistry());
    }

    private static ActionFilters setupActionFilters(List<ActionPlugin> actionPlugins) {
        List<ActionFilter> finalFilters = new ArrayList<>();
        List<MappedActionFilter> mappedFilters = new ArrayList<>();
        for (var plugin : actionPlugins) {
            finalFilters.addAll(plugin.getActionFilters());
            mappedFilters.addAll(plugin.getMappedActionFilters());
        }
        if (mappedFilters.isEmpty() == false) {
            finalFilters.add(new MappedActionFilters(mappedFilters));
        }
        return new ActionFilters(Set.copyOf(finalFilters));
    }

    public void initRestHandlers(Supplier<DiscoveryNodes> nodesInCluster, Predicate<NodeFeature> clusterSupportsFeature) {
        List<AbstractCatAction> catActions = new ArrayList<>();
        Predicate<AbstractCatAction> catActionsFilter = restExtension.getCatActionsFilter();
        Predicate<RestHandler> restFilter = restExtension.getActionsFilter();
        Consumer<RestHandler> registerHandler = handler -> {
            if (restFilter.test(handler)) {
                if (handler instanceof AbstractCatAction catAction && catActionsFilter.test(catAction)) {
                    catActions.add(catAction);
                }
                restController.registerHandler(handler);
            } else {
                /*
                 * There's no way this handler can be reached, so we just register a placeholder so that requests for it are routed to
                 * RestController for proper error messages.
                 */
                handler.routes().forEach(route -> restController.registerHandler(route, placeholderRestHandler));
            }
        };
        registerHandler.accept(new RestAddVotingConfigExclusionAction());
        registerHandler.accept(new RestClearVotingConfigExclusionsAction());
        registerHandler.accept(new RestNodesInfoAction(settingsFilter));
        registerHandler.accept(new RestRemoteClusterInfoAction());
        registerHandler.accept(new RestNodesCapabilitiesAction());
        registerHandler.accept(new RestNodesStatsAction(projectIdResolver));
        registerHandler.accept(new RestNodesUsageAction());
        registerHandler.accept(new RestNodesHotThreadsAction());
        registerHandler.accept(new RestClusterAllocationExplainAction());
        registerHandler.accept(new RestGetDesiredBalanceAction());
        registerHandler.accept(new RestDeleteDesiredBalanceAction());
        registerHandler.accept(new RestClusterStatsAction());
        registerHandler.accept(new RestClusterStateAction(settingsFilter, threadPool));
        registerHandler.accept(new RestClusterHealthAction());
        registerHandler.accept(new RestClusterUpdateSettingsAction());
        registerHandler.accept(new RestClusterGetSettingsAction(settings, clusterSettings, settingsFilter));
        registerHandler.accept(new RestClusterRerouteAction(settingsFilter, projectIdResolver));
        registerHandler.accept(new RestClusterSearchShardsAction());
        registerHandler.accept(new RestPendingClusterTasksAction());
        registerHandler.accept(new RestPutRepositoryAction());
        registerHandler.accept(new RestGetRepositoriesAction(settingsFilter));
        registerHandler.accept(new RestDeleteRepositoryAction());
        registerHandler.accept(new RestVerifyRepositoryAction());
        registerHandler.accept(new RestCleanupRepositoryAction());
        registerHandler.accept(new RestGetSnapshotsAction());
        registerHandler.accept(new RestCreateSnapshotAction());
        registerHandler.accept(new RestCloneSnapshotAction());
        registerHandler.accept(new RestRestoreSnapshotAction());
        registerHandler.accept(new RestDeleteSnapshotAction());
        registerHandler.accept(new RestSnapshotsStatusAction());
        registerHandler.accept(new RestSnapshottableFeaturesAction());
        registerHandler.accept(new RestResetFeatureStateAction());
        registerHandler.accept(new RestGetIndicesAction());
        registerHandler.accept(new RestIndicesStatsAction());
        registerHandler.accept(new RestIndicesSegmentsAction());
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
        registerHandler.accept(new RestGetHealthAction());
        registerHandler.accept(new RestPrevalidateNodeRemovalAction());

        registerHandler.accept(new RestUpdateSettingsAction());
        registerHandler.accept(new RestGetSettingsAction());

        registerHandler.accept(new RestAnalyzeAction());
        registerHandler.accept(new RestReloadAnalyzersAction());
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
        registerHandler.accept(new RestSimulateIngestAction());
        registerHandler.accept(new RestSimulateTemplateAction());

        registerHandler.accept(new RestPutMappingAction());
        registerHandler.accept(new RestGetMappingAction());
        registerHandler.accept(new RestGetFieldMappingAction());

        registerHandler.accept(new RestRefreshAction());
        registerHandler.accept(new RestFlushAction());
        registerHandler.accept(new RestForceMergeAction());
        registerHandler.accept(new RestClearIndicesCacheAction());
        registerHandler.accept(new RestResolveClusterAction());
        registerHandler.accept(new RestResolveIndexAction());

        registerHandler.accept(new RestIndexAction());
        registerHandler.accept(new CreateHandler());
        registerHandler.accept(new AutoIdHandler());
        registerHandler.accept(new RestGetAction());
        registerHandler.accept(new RestGetSourceAction());
        registerHandler.accept(new RestMultiGetAction(settings));
        registerHandler.accept(new RestDeleteAction());
        registerHandler.accept(new RestCountAction());
        registerHandler.accept(new RestTermVectorsAction());
        registerHandler.accept(new RestMultiTermVectorsAction());
        registerHandler.accept(new RestBulkAction(settings, bulkService));
        registerHandler.accept(new RestUpdateAction());

        registerHandler.accept(new RestSearchAction(restController.getSearchUsageHolder(), clusterSupportsFeature));
        registerHandler.accept(new RestSearchScrollAction());
        registerHandler.accept(new RestClearScrollAction());
        registerHandler.accept(new RestOpenPointInTimeAction());
        registerHandler.accept(new RestClosePointInTimeAction());
        registerHandler.accept(new RestMultiSearchAction(settings, restController.getSearchUsageHolder(), clusterSupportsFeature));
        registerHandler.accept(new RestKnnSearchAction());

        registerHandler.accept(new RestValidateQueryAction());

        registerHandler.accept(new RestExplainAction());

        registerHandler.accept(new RestRecoveryAction());

        registerHandler.accept(new RestReloadSecureSettingsAction());

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
        registerHandler.accept(new RestClusterInfoAction(projectIdResolver));
        registerHandler.accept(new RestTasksAction(nodesInCluster));
        registerHandler.accept(new RestIndicesAction(projectIdResolver));
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
        registerHandler.accept(new RestCatComponentTemplateAction(projectIdResolver));
        registerHandler.accept(new RestAnalyzeIndexDiskUsageAction());
        registerHandler.accept(new RestFieldUsageStatsAction());

        // Desired nodes
        registerHandler.accept(new RestGetDesiredNodesAction());
        registerHandler.accept(new RestUpdateDesiredNodesAction());
        registerHandler.accept(new RestDeleteDesiredNodesAction());

        for (ActionPlugin plugin : actionPlugins) {
            for (RestHandler handler : plugin.getRestHandlers(
                settings,
                namedWriteableRegistry,
                restController,
                clusterSettings,
                indexScopedSettings,
                settingsFilter,
                indexNameExpressionResolver,
                nodesInCluster,
                clusterSupportsFeature
            )) {
                registerHandler.accept(handler);
            }
        }
        registerHandler.accept(new RestCatAction(catActions));

        // Synonyms
        registerHandler.accept(new RestPutSynonymsAction());
        registerHandler.accept(new RestGetSynonymsAction());
        registerHandler.accept(new RestDeleteSynonymsAction());
        registerHandler.accept(new RestGetSynonymsSetsAction());
        registerHandler.accept(new RestPutSynonymRuleAction());
        registerHandler.accept(new RestGetSynonymRuleAction());
        registerHandler.accept(new RestDeleteSynonymRuleAction());
    }

    @Override
    protected void configure() {
        bind(RestController.class).toInstance(restController);
        bind(ActionFilters.class).toInstance(actionFilters);
        bind(DestructiveOperations.class).toInstance(destructiveOperations);
        bind(new TypeLiteral<RequestValidators<PutMappingRequest>>() {
        }).toInstance(mappingRequestValidators);
        bind(new TypeLiteral<RequestValidators<IndicesAliasesRequest>>() {
        }).toInstance(indicesAliasesRequestRequestValidators);
        bind(AutoCreateIndex.class).toInstance(autoCreateIndex);

        // register ActionType -> transportAction Map used by NodeClient
        @SuppressWarnings("rawtypes")
        MapBinder<ActionType, TransportAction> transportActionsBinder = MapBinder.newMapBinder(
            binder(),
            ActionType.class,
            TransportAction.class
        );
        for (ActionHandler action : actions.values()) {
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

    public ReservedClusterStateService getReservedClusterStateService() {
        return reservedClusterStateService;
    }
}
