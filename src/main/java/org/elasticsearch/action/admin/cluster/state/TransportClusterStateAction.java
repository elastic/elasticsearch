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

package org.elasticsearch.action.admin.cluster.state;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 *
 */
public class TransportClusterStateAction extends TransportMasterNodeOperationAction<ClusterStateRequest, ClusterStateResponse> {

    private final ClusterName clusterName;

    @Inject
    public TransportClusterStateAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                       ClusterName clusterName) {
        super(settings, transportService, clusterService, threadPool);
        this.clusterName = clusterName;
    }

    @Override
    protected String executor() {
        // very lightweight operation in memory, no need to fork to a thread
        return ThreadPool.Names.SAME;
    }

    @Override
    protected String transportAction() {
        return ClusterStateAction.NAME;
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
    protected boolean localExecute(ClusterStateRequest request) {
        return request.local();
    }

    @Override
    protected void masterOperation(final ClusterStateRequest request, final ClusterState state, ActionListener<ClusterStateResponse> listener) throws ElasticSearchException {
        ClusterState currentState = clusterService.state();
        logger.trace("Serving cluster state request using version {}", currentState.version());
        ClusterState.Builder builder = ClusterState.builder();
        builder.version(currentState.version());
        if (!request.filterNodes()) {
            builder.nodes(currentState.nodes());
        }
        if (!request.filterRoutingTable()) {
            builder.routingTable(currentState.routingTable());
            builder.allocationExplanation(currentState.allocationExplanation());
        }
        if (!request.filterBlocks()) {
            builder.blocks(currentState.blocks());
        }
        if (!request.filterMetaData()) {
            MetaData.Builder mdBuilder;
            if (request.filteredIndices().length == 0 && request.filteredIndexTemplates().length == 0) {
                mdBuilder = MetaData.builder(currentState.metaData());
            } else {
                mdBuilder = MetaData.builder();
            }

            if (request.filteredIndices().length > 0) {
                if (!(request.filteredIndices().length == 1 && ClusterStateRequest.NONE.equals(request.filteredIndices()[0]))) {
                    String[] indices = currentState.metaData().concreteIndicesIgnoreMissing(request.filteredIndices());
                    for (String filteredIndex : indices) {
                        IndexMetaData indexMetaData = currentState.metaData().index(filteredIndex);
                        if (indexMetaData != null) {
                            mdBuilder.put(indexMetaData, false);
                        }
                    }
                }
            }

            if (request.filteredIndexTemplates().length > 0) {
                for (String templateName : request.filteredIndexTemplates()) {
                    IndexTemplateMetaData indexTemplateMetaData = currentState.metaData().templates().get(templateName);
                    if (indexTemplateMetaData != null) {
                        mdBuilder.put(indexTemplateMetaData);
                    }
                }
            }

            builder.metaData(mdBuilder);
        }
        listener.onResponse(new ClusterStateResponse(clusterName, builder.build()));
    }
}
