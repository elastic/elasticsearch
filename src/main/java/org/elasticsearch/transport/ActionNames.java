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

package org.elasticsearch.transport;

import com.google.common.collect.ImmutableBiMap;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.restart.NodesRestartAction;
import org.elasticsearch.action.admin.cluster.node.shutdown.NodesShutdownAction;
import org.elasticsearch.action.admin.cluster.node.shutdown.TransportNodesShutdownAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusAction;
import org.elasticsearch.action.admin.cluster.snapshots.status.TransportNodesSnapshotsStatus;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsAction;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheAction;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsAction;
import org.elasticsearch.action.admin.indices.flush.FlushAction;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.admin.indices.optimize.OptimizeAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.status.IndicesStatusAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.elasticsearch.action.admin.indices.warmer.delete.DeleteWarmerAction;
import org.elasticsearch.action.admin.indices.warmer.get.GetWarmersAction;
import org.elasticsearch.action.admin.indices.warmer.put.PutWarmerAction;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.count.CountAction;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryAction;
import org.elasticsearch.action.explain.ExplainAction;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptAction;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptAction;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptAction;
import org.elasticsearch.action.mlt.MoreLikeThisAction;
import org.elasticsearch.action.percolate.MultiPercolateAction;
import org.elasticsearch.action.percolate.PercolateAction;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.suggest.SuggestAction;
import org.elasticsearch.action.termvector.MultiTermVectorsAction;
import org.elasticsearch.action.termvector.TermVectorAction;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.index.NodeIndexDeletedAction;
import org.elasticsearch.cluster.action.index.NodeMappingRefreshAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.discovery.zen.fd.MasterFaultDetection;
import org.elasticsearch.discovery.zen.fd.NodesFaultDetection;
import org.elasticsearch.discovery.zen.membership.MembershipAction;
import org.elasticsearch.discovery.zen.ping.multicast.MulticastZenPing;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastZenPing;
import org.elasticsearch.discovery.zen.publish.PublishClusterStateAction;
import org.elasticsearch.gateway.local.state.meta.LocalAllocateDangledIndices;
import org.elasticsearch.gateway.local.state.meta.TransportNodesListGatewayMetaState;
import org.elasticsearch.gateway.local.state.shards.TransportNodesListGatewayStartedShards;
import org.elasticsearch.indices.recovery.RecoverySource;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetaData;
import org.elasticsearch.river.cluster.PublishRiverClusterStateAction;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SnapshotsService;

final class ActionNames {

    static final ImmutableBiMap<String, String> ACTION_NAMES = createActionNamesMap();

    private ActionNames() {

    }

    static String incomingAction(String action, Version version) {
        if (version.before(Version.V_1_4_0_Beta1)) {
            String post_1_4_action = post_1_4_action(action);
            //custom action e.g. registered through plugin are not mapped, fallback to the original one
            if (post_1_4_action != null) {
                return post_1_4_action;
            }
        }
        return action;
    }

    static String outgoingAction(String action, Version version) {
        if (version.before(Version.V_1_4_0_Beta1)) {
            String pre_1_4_Action = pre_1_4_Action(action);
            //custom actions e.g. registered through plugins are not mapped, fallback to the original one
            if (pre_1_4_Action != null) {
                return pre_1_4_Action;
            }
        }
        return action;
    }

    static String post_1_4_action(String action) {
        return ACTION_NAMES.inverse().get(action);
    }

    static String pre_1_4_Action(String action) {
        return ACTION_NAMES.get(action);
    }

    private static ImmutableBiMap<String, String> createActionNamesMap() {
        ImmutableBiMap.Builder<String, String> builder = ImmutableBiMap.builder();

        addNodeAction(NodesRestartAction.NAME, "cluster/nodes/restart", builder);
        builder.put(NodesShutdownAction.NAME, "cluster/nodes/shutdown");
        builder.put(TransportNodesShutdownAction.SHUTDOWN_NODE_ACTION_NAME, "/cluster/nodes/shutdown/node");

        builder.put(DeleteRepositoryAction.NAME, "cluster/repository/delete");
        builder.put(GetRepositoriesAction.NAME, "cluster/repository/get");
        builder.put(PutRepositoryAction.NAME, "cluster/repository/put");

        builder.put(ClusterRerouteAction.NAME, "cluster/reroute");

        builder.put(ClusterUpdateSettingsAction.NAME, "cluster/settings/update");

        builder.put(CreateSnapshotAction.NAME, "cluster/snapshot/create");
        builder.put(DeleteSnapshotAction.NAME, "cluster/snapshot/delete");
        builder.put(GetSnapshotsAction.NAME, "cluster/snapshot/get");
        builder.put(RestoreSnapshotAction.NAME, "cluster/snapshot/restore");

        builder.put(ClusterHealthAction.NAME, "cluster/health");
        addNodeAction(NodesHotThreadsAction.NAME, "cluster/nodes/hot_threads", builder);
        addNodeAction(NodesInfoAction.NAME, "cluster/nodes/info", builder);
        addNodeAction(NodesStatsAction.NAME, "cluster/nodes/stats", builder);

        builder.put(SnapshotsStatusAction.NAME, "cluster/snapshot/status");
        addNodeAction(TransportNodesSnapshotsStatus.ACTION_NAME, "cluster/snapshot/status/nodes", builder);

        builder.put(ClusterStateAction.NAME, "cluster/state");
        addNodeAction(ClusterStatsAction.NAME, "cluster/stats", builder);

        builder.put(PendingClusterTasksAction.NAME, "cluster/task");

        builder.put(IndicesAliasesAction.NAME, "indices/aliases");
        builder.put(AliasesExistAction.NAME, "indices/exists/aliases");
        builder.put(GetAliasesAction.NAME, "indices/get/aliases");

        addShardAction(AnalyzeAction.NAME, "indices/analyze", builder);
        addShardAction(ClearIndicesCacheAction.NAME, "indices/cache/clear", builder);

        builder.put(CloseIndexAction.NAME, "indices/close");
        builder.put(CreateIndexAction.NAME, "indices/create");
        builder.put(DeleteIndexAction.NAME, "indices/delete");
        builder.put(IndicesExistsAction.NAME, "indices/exists");
        addShardAction(FlushAction.NAME, "indices/flush", builder);

        builder.put(DeleteMappingAction.NAME, "indices/mapping/delete");
        builder.put(PutMappingAction.NAME, "indices/mapping/put");
        builder.put(GetFieldMappingsAction.NAME, "mappings/fields/get");
        builder.put(GetFieldMappingsAction.NAME + "[index][s]", "mappings/fields/get/index/s");
        builder.put(GetMappingsAction.NAME, "mappings/get");

        builder.put(OpenIndexAction.NAME, "indices/open");

        addShardAction(OptimizeAction.NAME, "indices/optimize", builder);
        addShardAction(RefreshAction.NAME, "indices/refresh", builder);
        builder.put(UpdateSettingsAction.NAME, "indices/settings/update");
        builder.put(ClusterSearchShardsAction.NAME, "cluster/shards/search_shards");

        builder.put(DeleteIndexTemplateAction.NAME, "indices/template/delete");
        builder.put(GetIndexTemplatesAction.NAME, "indices/template/get");
        builder.put(PutIndexTemplateAction.NAME, "indices/template/put");

        builder.put(TypesExistsAction.NAME, "indices/types/exists");
        addShardAction(ValidateQueryAction.NAME, "indices/validate/query", builder);

        builder.put(DeleteWarmerAction.NAME, "indices/warmer/delete");
        builder.put(GetWarmersAction.NAME, "warmers/get");
        builder.put(PutWarmerAction.NAME, "indices/warmer/put");

        addShardAction(CountAction.NAME, "count", builder);
        addShardAction(ExplainAction.NAME, "explain", builder);
        addShardAction(GetAction.NAME, "get", builder);
        builder.put(MultiGetAction.NAME, "mget");
        builder.put(MultiGetAction.NAME + "[shard][s]", "mget/shard/s");

        builder.put(GetIndexedScriptAction.NAME, "getIndexedScript");
        builder.put(PutIndexedScriptAction.NAME, "putIndexedScript");
        builder.put(DeleteIndexedScriptAction.NAME, "deleteIndexedScript");

        builder.put(MoreLikeThisAction.NAME, "mlt");

        builder.put(MultiPercolateAction.NAME, "mpercolate");
        builder.put(MultiPercolateAction.NAME + "[shard][s]", "mpercolate/shard/s");

        builder.put(MultiSearchAction.NAME, "msearch");

        builder.put(MultiTermVectorsAction.NAME, "mtv");
        builder.put(MultiTermVectorsAction.NAME + "[shard][s]", "mtv/shard/s");

        addShardAction(PercolateAction.NAME, "percolate", builder);

        builder.put(SearchScrollAction.NAME, "search/scroll");
        builder.put(ClearScrollAction.NAME, "clear_sc");

        builder.put(SearchAction.NAME, "search");
        builder.put(SearchServiceTransportAction.FREE_CONTEXT_ACTION_NAME, "search/freeContext");
        builder.put(SearchServiceTransportAction.CLEAR_SCROLL_CONTEXTS_ACTION_NAME, "search/clearScrollContexts");
        builder.put(SearchServiceTransportAction.DFS_ACTION_NAME, "search/phase/dfs");
        builder.put(SearchServiceTransportAction.QUERY_ACTION_NAME, "search/phase/query");
        builder.put(SearchServiceTransportAction.QUERY_ID_ACTION_NAME, "search/phase/query/id");
        builder.put(SearchServiceTransportAction.QUERY_SCROLL_ACTION_NAME, "search/phase/query/scroll");
        builder.put(SearchServiceTransportAction.QUERY_FETCH_ACTION_NAME, "search/phase/query+fetch");
        builder.put(SearchServiceTransportAction.QUERY_QUERY_FETCH_ACTION_NAME, "search/phase/query/query+fetch");
        builder.put(SearchServiceTransportAction.QUERY_FETCH_SCROLL_ACTION_NAME, "search/phase/query+fetch/scroll");
        builder.put(SearchServiceTransportAction.FETCH_ID_ACTION_NAME, "search/phase/fetch/id");
        builder.put(SearchServiceTransportAction.SCAN_ACTION_NAME, "search/phase/scan");
        builder.put(SearchServiceTransportAction.SCAN_SCROLL_ACTION_NAME, "search/phase/scan/scroll");

        addShardAction(SuggestAction.NAME, "suggest", builder);
        addShardAction(TermVectorAction.NAME, "tv", builder);

        builder.put(BulkAction.NAME, "bulk");
        builder.put(BulkAction.NAME + "[s]", "bulk/shard");
        builder.put(BulkAction.NAME + "[s][r]", "bulk/shard/replica");

        builder.put(DeleteAction.NAME, "delete");
        builder.put(DeleteAction.NAME + "[r]", "delete/replica");
        builder.put(DeleteAction.NAME + "[s]", "indices/index/b_shard/delete");
        builder.put(DeleteAction.NAME + "[s][r]", "indices/index/b_shard/delete/replica");

        builder.put(DeleteByQueryAction.NAME, "deleteByQuery");
        builder.put(DeleteByQueryAction.NAME + "[s]", "deleteByQuery/shard");
        builder.put(DeleteByQueryAction.NAME + "[s][r]", "deleteByQuery/shard/replica");

        builder.put(IndexAction.NAME, "index");
        builder.put(IndexAction.NAME + "[r]", "index/replica");

        builder.put(UpdateAction.NAME, "update");

        addShardAction(RecoveryAction.NAME, "indices/recovery", builder);
        addShardAction(IndicesSegmentsAction.NAME, "indices/segments", builder);
        addShardAction(IndicesStatsAction.NAME, "indices/stats", builder);
        addShardAction(IndicesStatusAction.NAME, "indices/status", builder);

        builder.put(GetSettingsAction.NAME, "indices/settings/get");

        builder.put(MappingUpdatedAction.ACTION_NAME, "cluster/mappingUpdated");
        builder.put(NodeIndexDeletedAction.INDEX_DELETED_ACTION_NAME, "cluster/nodeIndexDeleted");
        builder.put(NodeIndexDeletedAction.INDEX_STORE_DELETED_ACTION_NAME, "cluster/nodeIndexStoreDeleted");
        builder.put(NodeMappingRefreshAction.ACTION_NAME, "cluster/nodeMappingRefresh");
        addNodeAction(TransportNodesListShardStoreMetaData.ACTION_NAME, "/cluster/nodes/indices/shard/store", builder);
        builder.put(ShardStateAction.SHARD_STARTED_ACTION_NAME, "cluster/shardStarted");
        builder.put(ShardStateAction.SHARD_FAILED_ACTION_NAME, "cluster/shardFailure");
        builder.put(RestoreService.UPDATE_RESTORE_ACTION_NAME, "cluster/snapshot/update_restore");
        builder.put(SnapshotsService.UPDATE_SNAPSHOT_ACTION_NAME, "cluster/snapshot/update_snapshot");
        builder.put(MasterFaultDetection.MASTER_PING_ACTION_NAME, "discovery/zen/fd/masterPing");
        builder.put(NodesFaultDetection.PING_ACTION_NAME, "discovery/zen/fd/ping");
        builder.put(MembershipAction.DISCOVERY_JOIN_ACTION_NAME, "discovery/zen/join");
        builder.put(ZenDiscovery.DISCOVERY_REJOIN_ACTION_NAME, "discovery/zen/rejoin");
        builder.put(MembershipAction.DISCOVERY_JOIN_VALIDATE_ACTION_NAME, "discovery/zen/join/validate");
        builder.put(MembershipAction.DISCOVERY_LEAVE_ACTION_NAME, "discovery/zen/leave");
        builder.put(MulticastZenPing.ACTION_NAME, "discovery/zen/multicast");
        builder.put(PublishClusterStateAction.ACTION_NAME, "discovery/zen/publish");
        builder.put(UnicastZenPing.ACTION_NAME, "discovery/zen/unicast");
        builder.put(LocalAllocateDangledIndices.ACTION_NAME, "/gateway/local/allocate_dangled");
        addNodeAction(TransportNodesListGatewayMetaState.ACTION_NAME, "/gateway/local/meta-state", builder);
        addNodeAction(TransportNodesListGatewayStartedShards.ACTION_NAME, "/gateway/local/started-shards", builder);

        builder.put(RecoveryTarget.Actions.CLEAN_FILES, "index/shard/recovery/cleanFiles");
        builder.put(RecoveryTarget.Actions.FILE_CHUNK, "index/shard/recovery/fileChunk");
        builder.put(RecoveryTarget.Actions.FILES_INFO, "index/shard/recovery/filesInfo");
        builder.put(RecoveryTarget.Actions.FINALIZE, "index/shard/recovery/finalize");
        builder.put(RecoveryTarget.Actions.PREPARE_TRANSLOG, "index/shard/recovery/prepareTranslog");
        builder.put(RecoveryTarget.Actions.TRANSLOG_OPS, "index/shard/recovery/translogOps");
        builder.put(RecoverySource.Actions.START_RECOVERY, "index/shard/recovery/startRecovery");

        builder.put(IndicesStore.ACTION_SHARD_EXISTS, "index/shard/exists");

        builder.put(PublishRiverClusterStateAction.ACTION_NAME, "river/state/publish");

        return builder.build();
    }

    private static void addNodeAction(String name, String pre_14_name, ImmutableBiMap.Builder<String, String> builder) {
        builder.put(name, pre_14_name);
        builder.put(name + "[n]", pre_14_name + "/n");
    }

    private static void addShardAction(String name, String pre_14_name, ImmutableBiMap.Builder<String, String> builder) {
        builder.put(name, pre_14_name);
        builder.put(name + "[s]", pre_14_name + "/s");
    }
}
