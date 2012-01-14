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
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
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
import org.elasticsearch.action.admin.indices.validate.query.TransportValidateQueryAction;
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
import org.elasticsearch.action.search.type.*;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import java.util.Map;

/**
 *
 */
public class TransportActionModule extends AbstractModule {

    private final Map<String, ActionEntry> actions = Maps.newHashMap();

    static class ActionEntry {
        public final String name;
        public final Class<? extends TransportAction> action;
        public final Class[] supportActions;

        ActionEntry(String name, Class<? extends TransportAction> action, Class... supportActions) {
            this.name = name;
            this.action = action;
            this.supportActions = supportActions;
        }


    }

    public TransportActionModule() {
    }

    /**
     * Registers a custom action under the provided action name, the actual action implementation, and
     * any supported actions (bind as singletons).
     *
     * @param actionName     The action name
     * @param action         The action itself
     * @param supportActions Support actions.
     */
    public void registerAction(String actionName, Class<? extends TransportAction> action, Class... supportActions) {
        actions.put(actionName, new ActionEntry(actionName, action, supportActions));
    }

    @Override
    protected void configure() {

        registerAction(TransportActions.Admin.Cluster.Node.INFO, TransportNodesInfoAction.class);
        registerAction(TransportActions.Admin.Cluster.Node.STATS, TransportNodesStatsAction.class);
        registerAction(TransportActions.Admin.Cluster.Node.SHUTDOWN, TransportNodesShutdownAction.class);
        registerAction(TransportActions.Admin.Cluster.Node.RESTART, TransportNodesRestartAction.class);

        registerAction(TransportActions.Admin.Cluster.STATE, TransportClusterStateAction.class);
        registerAction(TransportActions.Admin.Cluster.HEALTH, TransportClusterHealthAction.class);
        registerAction(TransportActions.Admin.Cluster.UPDATE_SETTINGS, TransportClusterUpdateSettingsAction.class);
        registerAction(TransportActions.Admin.Cluster.REROUTE, TransportClusterRerouteAction.class);

        registerAction(TransportActions.Admin.Cluster.Ping.SINGLE, TransportSinglePingAction.class);
        registerAction(TransportActions.Admin.Cluster.Ping.BROADCAST, TransportBroadcastPingAction.class);
        registerAction(TransportActions.Admin.Cluster.Ping.REPLICATION, TransportReplicationPingAction.class,
                TransportIndexReplicationPingAction.class, TransportShardReplicationPingAction.class);

        registerAction(TransportActions.Admin.Indices.STATS, TransportIndicesStatsAction.class);
        registerAction(TransportActions.Admin.Indices.STATUS, TransportIndicesStatusAction.class);
        registerAction(TransportActions.Admin.Indices.SEGMENTS, TransportIndicesSegmentsAction.class);
        registerAction(TransportActions.Admin.Indices.CREATE, TransportCreateIndexAction.class);
        registerAction(TransportActions.Admin.Indices.DELETE, TransportDeleteIndexAction.class);
        registerAction(TransportActions.Admin.Indices.OPEN, TransportOpenIndexAction.class);
        registerAction(TransportActions.Admin.Indices.CLOSE, TransportCloseIndexAction.class);
        registerAction(TransportActions.Admin.Indices.EXISTS, TransportIndicesExistsAction.class);
        registerAction(TransportActions.Admin.Indices.Mapping.PUT, TransportPutMappingAction.class);
        registerAction(TransportActions.Admin.Indices.Mapping.DELETE, TransportDeleteMappingAction.class);
        registerAction(TransportActions.Admin.Indices.ALIASES, TransportIndicesAliasesAction.class);
        registerAction(TransportActions.Admin.Indices.UPDATE_SETTINGS, TransportUpdateSettingsAction.class);
        registerAction(TransportActions.Admin.Indices.ANALYZE, TransportAnalyzeAction.class);
        registerAction(TransportActions.Admin.Indices.Template.PUT, TransportPutIndexTemplateAction.class);
        registerAction(TransportActions.Admin.Indices.Template.DELETE, TransportDeleteIndexTemplateAction.class);
        registerAction(TransportActions.Admin.Indices.Validate.QUERY, TransportValidateQueryAction.class);
        registerAction(TransportActions.Admin.Indices.Gateway.SNAPSHOT, TransportGatewaySnapshotAction.class);
        registerAction(TransportActions.Admin.Indices.REFRESH, TransportRefreshAction.class);
        registerAction(TransportActions.Admin.Indices.FLUSH, TransportFlushAction.class);
        registerAction(TransportActions.Admin.Indices.OPTIMIZE, TransportOptimizeAction.class);
        registerAction(TransportActions.Admin.Indices.Cache.CLEAR, TransportClearIndicesCacheAction.class);

        registerAction(TransportActions.INDEX, TransportIndexAction.class);
        registerAction(TransportActions.GET, TransportGetAction.class);
        registerAction(TransportActions.DELETE, TransportDeleteAction.class,
                TransportIndexDeleteAction.class, TransportShardDeleteAction.class);
        registerAction(TransportActions.COUNT, TransportCountAction.class);
        registerAction(TransportActions.UPDATE, TransportUpdateAction.class);
        registerAction(TransportActions.MULTI_GET, TransportMultiGetAction.class,
                TransportShardMultiGetAction.class);
        registerAction(TransportActions.BULK, TransportBulkAction.class,
                TransportShardBulkAction.class);
        registerAction(TransportActions.DELETE_BY_QUERY, TransportDeleteByQueryAction.class,
                TransportIndexDeleteByQueryAction.class, TransportShardDeleteByQueryAction.class);
        registerAction(TransportActions.SEARCH, TransportSearchAction.class,
                TransportSearchCache.class,
                TransportSearchDfsQueryThenFetchAction.class,
                TransportSearchQueryThenFetchAction.class,
                TransportSearchDfsQueryAndFetchAction.class,
                TransportSearchQueryAndFetchAction.class,
                TransportSearchScanAction.class
        );
        registerAction(TransportActions.SEARCH_SCROLL, TransportSearchScrollAction.class,
                TransportSearchScrollScanAction.class,
                TransportSearchScrollQueryThenFetchAction.class,
                TransportSearchScrollQueryAndFetchAction.class
        );
        registerAction(TransportActions.MORE_LIKE_THIS, TransportMoreLikeThisAction.class);
        registerAction(TransportActions.PERCOLATE, TransportPercolateAction.class);

        MapBinder<String, TransportAction> actionsBinder
                = MapBinder.newMapBinder(binder(), String.class, TransportAction.class);

        for (Map.Entry<String, ActionEntry> entry : actions.entrySet()) {
            actionsBinder.addBinding(entry.getKey()).to(entry.getValue().action).asEagerSingleton();
            for (Class supportAction : entry.getValue().supportActions) {
                bind(supportAction).asEagerSingleton();
            }
        }
    }
}
