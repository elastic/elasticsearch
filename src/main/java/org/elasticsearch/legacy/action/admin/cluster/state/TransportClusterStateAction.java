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

package org.elasticsearch.legacy.action.admin.cluster.state;

import org.elasticsearch.legacy.ElasticsearchException;
import org.elasticsearch.legacy.action.ActionListener;
import org.elasticsearch.legacy.action.support.IndicesOptions;
import org.elasticsearch.legacy.action.support.master.TransportMasterNodeReadOperationAction;
import org.elasticsearch.legacy.cluster.ClusterName;
import org.elasticsearch.legacy.cluster.ClusterService;
import org.elasticsearch.legacy.cluster.ClusterState;
import org.elasticsearch.legacy.cluster.metadata.IndexMetaData;
import org.elasticsearch.legacy.cluster.metadata.MetaData;
import org.elasticsearch.legacy.cluster.routing.RoutingTable;
import org.elasticsearch.legacy.common.inject.Inject;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.threadpool.ThreadPool;
import org.elasticsearch.legacy.transport.TransportService;

/**
 *
 */
public class TransportClusterStateAction extends TransportMasterNodeReadOperationAction<ClusterStateRequest, ClusterStateResponse> {

    private final ClusterName clusterName;

    @Inject
    public TransportClusterStateAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                       ClusterName clusterName) {
        super(settings, ClusterStateAction.NAME, transportService, clusterService, threadPool);
        this.clusterName = clusterName;
    }

    @Override
    protected String executor() {
        // very lightweight operation in memory, no need to fork to a thread
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterStateRequest newRequest() {
        return new ClusterStateRequest();
    }

    @Override
    protected ClusterStateResponse newResponse() {
        return new ClusterStateResponse();
    }

    @Override
    protected void masterOperation(final ClusterStateRequest request, final ClusterState state, ActionListener<ClusterStateResponse> listener) throws ElasticsearchException {
        ClusterState currentState = clusterService.state();
        logger.trace("Serving cluster state request using version {}", currentState.version());
        ClusterState.Builder builder = ClusterState.builder(currentState.getClusterName());
        builder.version(currentState.version());
        if (request.nodes()) {
            builder.nodes(currentState.nodes());
        }
        if (request.routingTable()) {
            if (request.indices().length > 0) {
                RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
                for (String filteredIndex : request.indices()) {
                    if (currentState.routingTable().getIndicesRouting().containsKey(filteredIndex)) {
                        routingTableBuilder.add(currentState.routingTable().getIndicesRouting().get(filteredIndex));
                    }
                }
                builder.routingTable(routingTableBuilder);
            } else {
                builder.routingTable(currentState.routingTable());
            }
        }
        if (request.blocks()) {
            builder.blocks(currentState.blocks());
        }
        if (request.metaData()) {
            MetaData.Builder mdBuilder;
            if (request.indices().length == 0) {
                mdBuilder = MetaData.builder(currentState.metaData());
            } else {
                mdBuilder = MetaData.builder();
            }

            if (request.indices().length > 0) {
                String[] indices = currentState.metaData().concreteIndices(IndicesOptions.lenientExpandOpen(), request.indices());
                for (String filteredIndex : indices) {
                    IndexMetaData indexMetaData = currentState.metaData().index(filteredIndex);
                    if (indexMetaData != null) {
                        mdBuilder.put(indexMetaData, false);
                    }
                }
            }

            builder.metaData(mdBuilder);
        }
        listener.onResponse(new ClusterStateResponse(clusterName, builder.build()));
    }
}
