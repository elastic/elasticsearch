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

import org.elasticsearch.util.guice.inject.AbstractModule;
import org.elasticsearch.rest.action.admin.cluster.health.RestClusterHealthAction;
import org.elasticsearch.rest.action.admin.cluster.node.info.RestNodesInfoAction;
import org.elasticsearch.rest.action.admin.cluster.node.shutdown.RestNodesShutdownAction;
import org.elasticsearch.rest.action.admin.cluster.ping.broadcast.RestBroadcastPingAction;
import org.elasticsearch.rest.action.admin.cluster.ping.replication.RestReplicationPingAction;
import org.elasticsearch.rest.action.admin.cluster.ping.single.RestSinglePingAction;
import org.elasticsearch.rest.action.admin.cluster.state.RestClusterStateAction;
import org.elasticsearch.rest.action.admin.indices.alias.RestIndicesAliasesAction;
import org.elasticsearch.rest.action.admin.indices.cache.clear.RestClearIndicesCacheAction;
import org.elasticsearch.rest.action.admin.indices.create.RestCreateIndexAction;
import org.elasticsearch.rest.action.admin.indices.delete.RestDeleteIndexAction;
import org.elasticsearch.rest.action.admin.indices.flush.RestFlushAction;
import org.elasticsearch.rest.action.admin.indices.gateway.snapshot.RestGatewaySnapshotAction;
import org.elasticsearch.rest.action.admin.indices.mapping.put.RestPutMappingAction;
import org.elasticsearch.rest.action.admin.indices.optimize.RestOptimizeAction;
import org.elasticsearch.rest.action.admin.indices.refresh.RestRefreshAction;
import org.elasticsearch.rest.action.admin.indices.status.RestIndicesStatusAction;
import org.elasticsearch.rest.action.count.RestCountAction;
import org.elasticsearch.rest.action.delete.RestDeleteAction;
import org.elasticsearch.rest.action.deletebyquery.RestDeleteByQueryAction;
import org.elasticsearch.rest.action.get.RestGetAction;
import org.elasticsearch.rest.action.index.RestIndexAction;
import org.elasticsearch.rest.action.main.RestMainAction;
import org.elasticsearch.rest.action.mlt.RestMoreLikeThisAction;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.rest.action.search.RestSearchScrollAction;
import org.elasticsearch.rest.action.terms.RestTermsAction;

/**
 * @author kimchy (Shay Banon)
 */
public class RestActionModule extends AbstractModule {

    @Override protected void configure() {
        bind(RestMainAction.class).asEagerSingleton();

        bind(RestNodesInfoAction.class).asEagerSingleton();
        bind(RestNodesShutdownAction.class).asEagerSingleton();
        bind(RestClusterStateAction.class).asEagerSingleton();
        bind(RestClusterHealthAction.class).asEagerSingleton();

        bind(RestSinglePingAction.class).asEagerSingleton();
        bind(RestBroadcastPingAction.class).asEagerSingleton();
        bind(RestReplicationPingAction.class).asEagerSingleton();

        bind(RestIndicesStatusAction.class).asEagerSingleton();
        bind(RestIndicesAliasesAction.class).asEagerSingleton();
        bind(RestCreateIndexAction.class).asEagerSingleton();
        bind(RestDeleteIndexAction.class).asEagerSingleton();

        bind(RestPutMappingAction.class).asEagerSingleton();

        bind(RestGatewaySnapshotAction.class).asEagerSingleton();

        bind(RestRefreshAction.class).asEagerSingleton();
        bind(RestFlushAction.class).asEagerSingleton();
        bind(RestOptimizeAction.class).asEagerSingleton();
        bind(RestClearIndicesCacheAction.class).asEagerSingleton();

        bind(RestIndexAction.class).asEagerSingleton();

        bind(RestGetAction.class).asEagerSingleton();

        bind(RestDeleteAction.class).asEagerSingleton();

        bind(RestDeleteByQueryAction.class).asEagerSingleton();

        bind(RestCountAction.class).asEagerSingleton();
        bind(RestTermsAction.class).asEagerSingleton();

        bind(RestSearchAction.class).asEagerSingleton();
        bind(RestSearchScrollAction.class).asEagerSingleton();

        bind(RestMoreLikeThisAction.class).asEagerSingleton();
    }
}
