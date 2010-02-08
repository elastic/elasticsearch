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

package org.elasticsearch.client.transport.action;

import com.google.inject.AbstractModule;
import org.elasticsearch.client.transport.action.admin.cluster.node.info.ClientTransportNodesInfoAction;
import org.elasticsearch.client.transport.action.admin.cluster.ping.broadcast.ClientTransportBroadcastPingAction;
import org.elasticsearch.client.transport.action.admin.cluster.ping.replication.ClientTransportReplicationPingAction;
import org.elasticsearch.client.transport.action.admin.cluster.ping.single.ClientTransportSinglePingAction;
import org.elasticsearch.client.transport.action.admin.cluster.state.ClientTransportClusterStateAction;
import org.elasticsearch.client.transport.action.admin.indices.create.ClientTransportCreateIndexAction;
import org.elasticsearch.client.transport.action.admin.indices.delete.ClientTransportDeleteIndexAction;
import org.elasticsearch.client.transport.action.admin.indices.flush.ClientTransportFlushAction;
import org.elasticsearch.client.transport.action.admin.indices.gateway.snapshot.ClientTransportGatewaySnapshotAction;
import org.elasticsearch.client.transport.action.admin.indices.mapping.create.ClientTransportCreateMappingAction;
import org.elasticsearch.client.transport.action.admin.indices.refresh.ClientTransportRefreshAction;
import org.elasticsearch.client.transport.action.admin.indices.status.ClientTransportIndicesStatusAction;
import org.elasticsearch.client.transport.action.count.ClientTransportCountAction;
import org.elasticsearch.client.transport.action.delete.ClientTransportDeleteAction;
import org.elasticsearch.client.transport.action.deletebyquery.ClientTransportDeleteByQueryAction;
import org.elasticsearch.client.transport.action.get.ClientTransportGetAction;
import org.elasticsearch.client.transport.action.index.ClientTransportIndexAction;
import org.elasticsearch.client.transport.action.search.ClientTransportSearchAction;
import org.elasticsearch.client.transport.action.search.ClientTransportSearchScrollAction;

/**
 * @author kimchy (Shay Banon)
 */
public class ClientTransportActionModule extends AbstractModule {

    @Override protected void configure() {
        bind(ClientTransportIndexAction.class).asEagerSingleton();
        bind(ClientTransportDeleteAction.class).asEagerSingleton();
        bind(ClientTransportDeleteByQueryAction.class).asEagerSingleton();
        bind(ClientTransportGetAction.class).asEagerSingleton();
        bind(ClientTransportCountAction.class).asEagerSingleton();
        bind(ClientTransportSearchAction.class).asEagerSingleton();
        bind(ClientTransportSearchScrollAction.class).asEagerSingleton();

        bind(ClientTransportIndicesStatusAction.class).asEagerSingleton();
        bind(ClientTransportRefreshAction.class).asEagerSingleton();
        bind(ClientTransportFlushAction.class).asEagerSingleton();
        bind(ClientTransportCreateIndexAction.class).asEagerSingleton();
        bind(ClientTransportDeleteIndexAction.class).asEagerSingleton();
        bind(ClientTransportCreateMappingAction.class).asEagerSingleton();
        bind(ClientTransportGatewaySnapshotAction.class).asEagerSingleton();
        bind(ClientTransportNodesInfoAction.class).asEagerSingleton();
        bind(ClientTransportSinglePingAction.class).asEagerSingleton();
        bind(ClientTransportReplicationPingAction.class).asEagerSingleton();
        bind(ClientTransportBroadcastPingAction.class).asEagerSingleton();
        bind(ClientTransportClusterStateAction.class).asEagerSingleton();
    }
}
