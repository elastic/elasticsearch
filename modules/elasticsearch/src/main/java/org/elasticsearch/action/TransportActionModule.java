/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.admin.cluster.node.info.TransportNodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.restart.TransportNodesRestartAction;
import org.elasticsearch.action.admin.cluster.node.shutdown.TransportNodesShutdownAction;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.elasticsearch.action.admin.cluster.ping.broadcast.TransportBroadcastPingAction;
import org.elasticsearch.action.admin.cluster.ping.replication.TransportIndexReplicationPingAction;
import org.elasticsearch.action.admin.cluster.ping.replication.TransportReplicationPingAction;
import org.elasticsearch.action.admin.cluster.ping.replication.TransportShardReplicationPingAction;
import org.elasticsearch.action.admin.cluster.ping.single.TransportSinglePingAction;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.state.TransportClusterStateAction;
import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.action.admin.indices.analyze.TransportAnalyzeAction;
import org.elasticsearch.action.admin.indices.cache.clear.TransportClearIndicesCacheAction;
import org.elasticsearch.action.admin.indices.close.TransportCloseIndexAction;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.exists.TransportIndicesExistsAction;
import org.elasticsearch.action.admin.indices.flush.TransportFlushAction;
import org.elasticsearch.action.admin.indices.gateway.snapshot.TransportGatewaySnapshotAction;
import org.elasticsearch.action.admin.indices.mapping.delete.TransportDeleteMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.admin.indices.open.TransportOpenIndexAction;
import org.elasticsearch.action.admin.indices.optimize.TransportOptimizeAction;
import org.elasticsearch.action.admin.indices.refresh.TransportRefreshAction;
import org.elasticsearch.action.admin.indices.segments.TransportIndicesSegmentsAction;
import org.elasticsearch.action.admin.indices.settings.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.stats.TransportIndicesStatsAction;
import org.elasticsearch.action.admin.indices.status.TransportIndicesStatusAction;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.count.TransportCountAction;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.delete.index.TransportIndexDeleteAction;
import org.elasticsearch.action.delete.index.TransportShardDeleteAction;
import org.elasticsearch.action.deletebyquery.TransportDeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.TransportIndexDeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.TransportShardDeleteByQueryAction;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.get.TransportMultiGetAction;
import org.elasticsearch.action.get.TransportShardMultiGetAction;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.mlt.TransportMoreLikeThisAction;
import org.elasticsearch.action.percolate.TransportPercolateAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.search.TransportSearchScrollAction;
import org.elasticsearch.action.search.type.TransportSearchCache;
import org.elasticsearch.action.search.type.TransportSearchDfsQueryAndFetchAction;
import org.elasticsearch.action.search.type.TransportSearchDfsQueryThenFetchAction;
import org.elasticsearch.action.search.type.TransportSearchQueryAndFetchAction;
import org.elasticsearch.action.search.type.TransportSearchQueryThenFetchAction;
import org.elasticsearch.action.search.type.TransportSearchScanAction;
import org.elasticsearch.action.search.type.TransportSearchScrollQueryAndFetchAction;
import org.elasticsearch.action.search.type.TransportSearchScrollQueryThenFetchAction;
import org.elasticsearch.action.search.type.TransportSearchScrollScanAction;
import org.elasticsearch.common.inject.AbstractModule;

/**
 * @author kimchy (shay.banon)
 */
public class TransportActionModule extends AbstractModule {

    @Override protected void configure() {

        bind(TransportNodesInfoAction.class).asEagerSingleton();
        bind(TransportNodesStatsAction.class).asEagerSingleton();
        bind(TransportNodesShutdownAction.class).asEagerSingleton();
        bind(TransportNodesRestartAction.class).asEagerSingleton();
        bind(TransportClusterStateAction.class).asEagerSingleton();
        bind(TransportClusterHealthAction.class).asEagerSingleton();
        bind(TransportClusterUpdateSettingsAction.class).asEagerSingleton();

        bind(TransportSinglePingAction.class).asEagerSingleton();
        bind(TransportBroadcastPingAction.class).asEagerSingleton();
        bind(TransportShardReplicationPingAction.class).asEagerSingleton();
        bind(TransportIndexReplicationPingAction.class).asEagerSingleton();
        bind(TransportReplicationPingAction.class).asEagerSingleton();

        bind(TransportIndicesStatsAction.class).asEagerSingleton();
        bind(TransportIndicesStatusAction.class).asEagerSingleton();
        bind(TransportIndicesSegmentsAction.class).asEagerSingleton();
        bind(TransportCreateIndexAction.class).asEagerSingleton();
        bind(TransportDeleteIndexAction.class).asEagerSingleton();
        bind(TransportOpenIndexAction.class).asEagerSingleton();
        bind(TransportCloseIndexAction.class).asEagerSingleton();
        bind(TransportIndicesExistsAction.class).asEagerSingleton();
        bind(TransportPutMappingAction.class).asEagerSingleton();
        bind(TransportDeleteMappingAction.class).asEagerSingleton();
        bind(TransportIndicesAliasesAction.class).asEagerSingleton();
        bind(TransportUpdateSettingsAction.class).asEagerSingleton();
        bind(TransportAnalyzeAction.class).asEagerSingleton();
        bind(TransportPutIndexTemplateAction.class).asEagerSingleton();
        bind(TransportDeleteIndexTemplateAction.class).asEagerSingleton();

        bind(TransportGatewaySnapshotAction.class).asEagerSingleton();

        bind(TransportRefreshAction.class).asEagerSingleton();
        bind(TransportFlushAction.class).asEagerSingleton();
        bind(TransportOptimizeAction.class).asEagerSingleton();
        bind(TransportClearIndicesCacheAction.class).asEagerSingleton();

        bind(TransportIndexAction.class).asEagerSingleton();
        bind(TransportGetAction.class).asEagerSingleton();
        bind(TransportDeleteAction.class).asEagerSingleton();
        bind(TransportIndexDeleteAction.class).asEagerSingleton();
        bind(TransportShardDeleteAction.class).asEagerSingleton();
        bind(TransportCountAction.class).asEagerSingleton();

        bind(TransportMultiGetAction.class).asEagerSingleton();
        bind(TransportShardMultiGetAction.class).asEagerSingleton();

        bind(TransportBulkAction.class).asEagerSingleton();
        bind(TransportShardBulkAction.class).asEagerSingleton();

        bind(TransportShardDeleteByQueryAction.class).asEagerSingleton();
        bind(TransportIndexDeleteByQueryAction.class).asEagerSingleton();
        bind(TransportDeleteByQueryAction.class).asEagerSingleton();


        bind(TransportSearchCache.class).asEagerSingleton();
        bind(TransportSearchDfsQueryThenFetchAction.class).asEagerSingleton();
        bind(TransportSearchQueryThenFetchAction.class).asEagerSingleton();
        bind(TransportSearchDfsQueryAndFetchAction.class).asEagerSingleton();
        bind(TransportSearchQueryAndFetchAction.class).asEagerSingleton();
        bind(TransportSearchScanAction.class).asEagerSingleton();
        bind(TransportSearchAction.class).asEagerSingleton();

        bind(TransportSearchScrollScanAction.class).asEagerSingleton();
        bind(TransportSearchScrollQueryThenFetchAction.class).asEagerSingleton();
        bind(TransportSearchScrollQueryAndFetchAction.class).asEagerSingleton();
        bind(TransportSearchScrollAction.class).asEagerSingleton();

        bind(TransportMoreLikeThisAction.class).asEagerSingleton();

        bind(TransportPercolateAction.class).asEagerSingleton();
    }
}
