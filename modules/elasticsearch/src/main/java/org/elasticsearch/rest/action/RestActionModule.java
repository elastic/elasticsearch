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

package org.elasticsearch.rest.action;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.action.admin.cluster.health.RestClusterHealthAction;
import org.elasticsearch.rest.action.admin.cluster.node.info.RestNodesInfoAction;
import org.elasticsearch.rest.action.admin.cluster.node.restart.RestNodesRestartAction;
import org.elasticsearch.rest.action.admin.cluster.node.shutdown.RestNodesShutdownAction;
import org.elasticsearch.rest.action.admin.cluster.node.stats.RestNodesStatsAction;
import org.elasticsearch.rest.action.admin.cluster.ping.broadcast.RestBroadcastPingAction;
import org.elasticsearch.rest.action.admin.cluster.ping.replication.RestReplicationPingAction;
import org.elasticsearch.rest.action.admin.cluster.ping.single.RestSinglePingAction;
import org.elasticsearch.rest.action.admin.cluster.state.RestClusterStateAction;
import org.elasticsearch.rest.action.admin.indices.alias.RestGetIndicesAliasesAction;
import org.elasticsearch.rest.action.admin.indices.alias.RestIndicesAliasesAction;
import org.elasticsearch.rest.action.admin.indices.analyze.RestAnalyzeAction;
import org.elasticsearch.rest.action.admin.indices.cache.clear.RestClearIndicesCacheAction;
import org.elasticsearch.rest.action.admin.indices.close.RestCloseIndexAction;
import org.elasticsearch.rest.action.admin.indices.create.RestCreateIndexAction;
import org.elasticsearch.rest.action.admin.indices.delete.RestDeleteIndexAction;
import org.elasticsearch.rest.action.admin.indices.flush.RestFlushAction;
import org.elasticsearch.rest.action.admin.indices.gateway.snapshot.RestGatewaySnapshotAction;
import org.elasticsearch.rest.action.admin.indices.mapping.delete.RestDeleteMappingAction;
import org.elasticsearch.rest.action.admin.indices.mapping.get.RestGetMappingAction;
import org.elasticsearch.rest.action.admin.indices.mapping.put.RestPutMappingAction;
import org.elasticsearch.rest.action.admin.indices.open.RestOpenIndexAction;
import org.elasticsearch.rest.action.admin.indices.optimize.RestOptimizeAction;
import org.elasticsearch.rest.action.admin.indices.refresh.RestRefreshAction;
import org.elasticsearch.rest.action.admin.indices.settings.RestGetSettingsAction;
import org.elasticsearch.rest.action.admin.indices.settings.RestUpdateSettingsAction;
import org.elasticsearch.rest.action.admin.indices.status.RestIndicesStatusAction;
import org.elasticsearch.rest.action.admin.indices.template.delete.RestDeleteIndexTemplateAction;
import org.elasticsearch.rest.action.admin.indices.template.get.RestGetIndexTemplateAction;
import org.elasticsearch.rest.action.admin.indices.template.put.RestPutIndexTemplateAction;
import org.elasticsearch.rest.action.bulk.RestBulkAction;
import org.elasticsearch.rest.action.count.RestCountAction;
import org.elasticsearch.rest.action.delete.RestDeleteAction;
import org.elasticsearch.rest.action.deletebyquery.RestDeleteByQueryAction;
import org.elasticsearch.rest.action.get.RestGetAction;
import org.elasticsearch.rest.action.index.RestIndexAction;
import org.elasticsearch.rest.action.main.RestMainAction;
import org.elasticsearch.rest.action.mlt.RestMoreLikeThisAction;
import org.elasticsearch.rest.action.percolate.RestPercolateAction;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.rest.action.search.RestSearchScrollAction;

import java.util.List;

/**
 * @author kimchy (Shay Banon)
 */
public class RestActionModule extends AbstractModule {

    private final List<Class<? extends RestHandler>> restPluginsActions;
    private List<Class<? extends RestHandler>> disabledDefaultRestActions;

    private final Class<? extends BaseRestHandler>[] defaultRestActions = new Class[] {
            RestMainAction.class,
            RestNodesInfoAction.class,
            RestNodesStatsAction.class,
            RestNodesShutdownAction.class,
            RestNodesRestartAction.class,
            RestClusterStateAction.class,
            RestClusterHealthAction.class,
            RestSinglePingAction.class,
            RestBroadcastPingAction.class,
            RestReplicationPingAction.class,
            RestIndicesStatusAction.class,
            RestGetIndicesAliasesAction.class,
            RestIndicesAliasesAction.class,
            RestCreateIndexAction.class,
            RestDeleteIndexAction.class,
            RestCloseIndexAction.class,
            RestOpenIndexAction.class,
            RestUpdateSettingsAction.class,
            RestGetSettingsAction.class,
            RestAnalyzeAction.class,
            RestGetIndexTemplateAction.class,
            RestPutIndexTemplateAction.class,
            RestDeleteIndexTemplateAction.class,
            RestPutMappingAction.class,
            RestDeleteMappingAction.class,
            RestGetMappingAction.class,
            RestGatewaySnapshotAction.class,
            RestRefreshAction.class,
            RestFlushAction.class,
            RestOptimizeAction.class,
            RestClearIndicesCacheAction.class,
            RestIndexAction.class,
            RestGetAction.class,
            RestDeleteAction.class,
            RestDeleteByQueryAction.class,
            RestCountAction.class,
            RestBulkAction.class,
            RestSearchAction.class,
            RestSearchScrollAction.class,
            RestMoreLikeThisAction.class,
            RestPercolateAction.class
    };

    public RestActionModule(List<Class<? extends RestHandler>> restPluginsActions, List<Class<? extends RestHandler>> disabledDefaultRestActions, Settings settings) {
        this.restPluginsActions = restPluginsActions;
        this.disabledDefaultRestActions = disabledDefaultRestActions;
    }

    @Override protected void configure() {
        for (Class<? extends RestHandler> restAction : restPluginsActions) {
                bind(restAction).asEagerSingleton();
        }

        for (Class<? extends RestHandler> restAction : defaultRestActions) {
            if (!isDefaultActionDisabled(restAction)) {
                bind(restAction).asEagerSingleton();
            }
        }
    }

    private boolean isDefaultActionDisabled(Class<? extends RestHandler> restAction) {
        boolean disabled = false;
        for (Class<? extends RestHandler> restDisabledDefaultAction : disabledDefaultRestActions) {
            if (restDisabledDefaultAction.isAssignableFrom(restAction)) {
                disabled = true;
                break;
            }
        }
        return disabled;
    }


}
