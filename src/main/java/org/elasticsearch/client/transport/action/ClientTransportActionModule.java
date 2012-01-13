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

package org.elasticsearch.client.transport.action;

import com.google.common.collect.Maps;
import org.elasticsearch.action.TransportActions;
import org.elasticsearch.client.transport.action.admin.cluster.health.ClientTransportClusterHealthAction;
import org.elasticsearch.client.transport.action.admin.cluster.node.info.ClientTransportNodesInfoAction;
import org.elasticsearch.client.transport.action.admin.cluster.node.restart.ClientTransportNodesRestartAction;
import org.elasticsearch.client.transport.action.admin.cluster.node.shutdown.ClientTransportNodesShutdownAction;
import org.elasticsearch.client.transport.action.admin.cluster.node.stats.ClientTransportNodesStatsAction;
import org.elasticsearch.client.transport.action.admin.cluster.ping.broadcast.ClientTransportBroadcastPingAction;
import org.elasticsearch.client.transport.action.admin.cluster.ping.replication.ClientTransportReplicationPingAction;
import org.elasticsearch.client.transport.action.admin.cluster.ping.single.ClientTransportSinglePingAction;
import org.elasticsearch.client.transport.action.admin.cluster.reroute.ClientTransportClusterRerouteAction;
import org.elasticsearch.client.transport.action.admin.cluster.settings.ClientTransportClusterUpdateSettingsAction;
import org.elasticsearch.client.transport.action.admin.cluster.state.ClientTransportClusterStateAction;
import org.elasticsearch.client.transport.action.admin.indices.alias.ClientTransportIndicesAliasesAction;
import org.elasticsearch.client.transport.action.admin.indices.analyze.ClientTransportAnalyzeAction;
import org.elasticsearch.client.transport.action.admin.indices.cache.clear.ClientTransportClearIndicesCacheAction;
import org.elasticsearch.client.transport.action.admin.indices.close.ClientTransportCloseIndexAction;
import org.elasticsearch.client.transport.action.admin.indices.create.ClientTransportCreateIndexAction;
import org.elasticsearch.client.transport.action.admin.indices.delete.ClientTransportDeleteIndexAction;
import org.elasticsearch.client.transport.action.admin.indices.exists.ClientTransportIndicesExistsAction;
import org.elasticsearch.client.transport.action.admin.indices.flush.ClientTransportFlushAction;
import org.elasticsearch.client.transport.action.admin.indices.gateway.snapshot.ClientTransportGatewaySnapshotAction;
import org.elasticsearch.client.transport.action.admin.indices.mapping.delete.ClientTransportDeleteMappingAction;
import org.elasticsearch.client.transport.action.admin.indices.mapping.put.ClientTransportPutMappingAction;
import org.elasticsearch.client.transport.action.admin.indices.open.ClientTransportOpenIndexAction;
import org.elasticsearch.client.transport.action.admin.indices.optimize.ClientTransportOptimizeAction;
import org.elasticsearch.client.transport.action.admin.indices.refresh.ClientTransportRefreshAction;
import org.elasticsearch.client.transport.action.admin.indices.segments.ClientTransportIndicesSegmentsAction;
import org.elasticsearch.client.transport.action.admin.indices.settings.ClientTransportUpdateSettingsAction;
import org.elasticsearch.client.transport.action.admin.indices.stats.ClientTransportIndicesStatsAction;
import org.elasticsearch.client.transport.action.admin.indices.status.ClientTransportIndicesStatusAction;
import org.elasticsearch.client.transport.action.admin.indices.template.delete.ClientTransportDeleteIndexTemplateAction;
import org.elasticsearch.client.transport.action.admin.indices.template.put.ClientTransportPutIndexTemplateAction;
import org.elasticsearch.client.transport.action.admin.indices.validate.query.ClientTransportValidateQueryAction;
import org.elasticsearch.client.transport.action.bulk.ClientTransportBulkAction;
import org.elasticsearch.client.transport.action.count.ClientTransportCountAction;
import org.elasticsearch.client.transport.action.delete.ClientTransportDeleteAction;
import org.elasticsearch.client.transport.action.deletebyquery.ClientTransportDeleteByQueryAction;
import org.elasticsearch.client.transport.action.get.ClientTransportGetAction;
import org.elasticsearch.client.transport.action.get.ClientTransportMultiGetAction;
import org.elasticsearch.client.transport.action.index.ClientTransportIndexAction;
import org.elasticsearch.client.transport.action.percolate.ClientTransportPercolateAction;
import org.elasticsearch.client.transport.action.search.ClientTransportSearchAction;
import org.elasticsearch.client.transport.action.search.ClientTransportSearchScrollAction;
import org.elasticsearch.client.transport.action.support.BaseClientTransportAction;
import org.elasticsearch.client.transport.action.update.ClientTransportUpdateAction;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import java.util.Map;

/**
 *
 */
public class ClientTransportActionModule extends AbstractModule {

    private final Map<String, ActionEntry> actions = Maps.newHashMap();

    static class ActionEntry {
        public final String name;
        public final Class<? extends BaseClientTransportAction> action;
        public final Class[] supportActions;

        ActionEntry(String name, Class<? extends BaseClientTransportAction> action, Class... supportActions) {
            this.name = name;
            this.action = action;
            this.supportActions = supportActions;
        }
    }

    /**
     * Registers a custom action under the provided action name, the actual action implementation, and
     * any supported actions (bind as singletons).
     *
     * @param actionName     The action name
     * @param action         The action itself
     * @param supportActions Support actions.
     */
    public void registerAction(String actionName, Class<? extends BaseClientTransportAction> action, Class... supportActions) {
        actions.put(actionName, new ActionEntry(actionName, action, supportActions));
    }

    @Override
    protected void configure() {
        registerAction(TransportActions.INDEX, ClientTransportIndexAction.class);
        registerAction(TransportActions.DELETE, ClientTransportDeleteAction.class);
        registerAction(TransportActions.DELETE_BY_QUERY, ClientTransportDeleteByQueryAction.class);
        registerAction(TransportActions.GET, ClientTransportGetAction.class);
        registerAction(TransportActions.MULTI_GET, ClientTransportMultiGetAction.class);
        registerAction(TransportActions.COUNT, ClientTransportCountAction.class);
        registerAction(TransportActions.SEARCH, ClientTransportSearchAction.class);
        registerAction(TransportActions.SEARCH_SCROLL, ClientTransportSearchScrollAction.class);
        registerAction(TransportActions.BULK, ClientTransportBulkAction.class);
        registerAction(TransportActions.PERCOLATE, ClientTransportPercolateAction.class);
        registerAction(TransportActions.UPDATE, ClientTransportUpdateAction.class);

        registerAction(TransportActions.Admin.Indices.EXISTS, ClientTransportIndicesExistsAction.class);
        registerAction(TransportActions.Admin.Indices.STATS, ClientTransportIndicesStatsAction.class);
        registerAction(TransportActions.Admin.Indices.STATUS, ClientTransportIndicesStatusAction.class);
        registerAction(TransportActions.Admin.Indices.SEGMENTS, ClientTransportIndicesSegmentsAction.class);
        registerAction(TransportActions.Admin.Indices.REFRESH, ClientTransportRefreshAction.class);
        registerAction(TransportActions.Admin.Indices.FLUSH, ClientTransportFlushAction.class);
        registerAction(TransportActions.Admin.Indices.OPTIMIZE, ClientTransportOptimizeAction.class);
        registerAction(TransportActions.Admin.Indices.CREATE, ClientTransportCreateIndexAction.class);
        registerAction(TransportActions.Admin.Indices.DELETE, ClientTransportDeleteIndexAction.class);
        registerAction(TransportActions.Admin.Indices.CLOSE, ClientTransportCloseIndexAction.class);
        registerAction(TransportActions.Admin.Indices.OPEN, ClientTransportOpenIndexAction.class);
        registerAction(TransportActions.Admin.Indices.Mapping.PUT, ClientTransportPutMappingAction.class);
        registerAction(TransportActions.Admin.Indices.Mapping.DELETE, ClientTransportDeleteMappingAction.class);
        registerAction(TransportActions.Admin.Indices.Gateway.SNAPSHOT, ClientTransportGatewaySnapshotAction.class);
        registerAction(TransportActions.Admin.Indices.ALIASES, ClientTransportIndicesAliasesAction.class);
        registerAction(TransportActions.Admin.Indices.Cache.CLEAR, ClientTransportClearIndicesCacheAction.class);
        registerAction(TransportActions.Admin.Indices.UPDATE_SETTINGS, ClientTransportUpdateSettingsAction.class);
        registerAction(TransportActions.Admin.Indices.ANALYZE, ClientTransportAnalyzeAction.class);
        registerAction(TransportActions.Admin.Indices.Template.PUT, ClientTransportPutIndexTemplateAction.class);
        registerAction(TransportActions.Admin.Indices.Template.DELETE, ClientTransportDeleteIndexTemplateAction.class);
        registerAction(TransportActions.Admin.Indices.Validate.QUERY, ClientTransportValidateQueryAction.class);

        registerAction(TransportActions.Admin.Cluster.Node.INFO, ClientTransportNodesInfoAction.class);
        registerAction(TransportActions.Admin.Cluster.Node.STATS, ClientTransportNodesStatsAction.class);
        registerAction(TransportActions.Admin.Cluster.Node.SHUTDOWN, ClientTransportNodesShutdownAction.class);
        registerAction(TransportActions.Admin.Cluster.Node.RESTART, ClientTransportNodesRestartAction.class);
        registerAction(TransportActions.Admin.Cluster.Ping.SINGLE, ClientTransportSinglePingAction.class);
        registerAction(TransportActions.Admin.Cluster.Ping.REPLICATION, ClientTransportReplicationPingAction.class);
        registerAction(TransportActions.Admin.Cluster.Ping.BROADCAST, ClientTransportBroadcastPingAction.class);
        registerAction(TransportActions.Admin.Cluster.STATE, ClientTransportClusterStateAction.class);
        registerAction(TransportActions.Admin.Cluster.HEALTH, ClientTransportClusterHealthAction.class);
        registerAction(TransportActions.Admin.Cluster.UPDATE_SETTINGS, ClientTransportClusterUpdateSettingsAction.class);
        registerAction(TransportActions.Admin.Cluster.REROUTE, ClientTransportClusterRerouteAction.class);

        MapBinder<String, BaseClientTransportAction> actionsBinder
                = MapBinder.newMapBinder(binder(), String.class, BaseClientTransportAction.class);

        for (Map.Entry<String, ActionEntry> entry : actions.entrySet()) {
            actionsBinder.addBinding(entry.getKey()).to(entry.getValue().action).asEagerSingleton();
            for (Class supportAction : entry.getValue().supportActions) {
                bind(supportAction).asEagerSingleton();
            }
        }
    }
}
