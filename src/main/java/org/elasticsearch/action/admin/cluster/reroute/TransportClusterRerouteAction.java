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

package org.elasticsearch.action.admin.cluster.reroute;

import org.elasticsearch.action.support.master.TransportClusterStateUpdateAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ack.ClusterStateUpdateActionListener;
import org.elasticsearch.cluster.metadata.MetaDataClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Cluster reroute action
 */
public class TransportClusterRerouteAction extends TransportClusterStateUpdateAction<ClusterRerouteClusterStateUpdateRequest, ClusterRerouteClusterStateUpdateResponse, ClusterRerouteRequest, ClusterRerouteResponse> {

    private final MetaDataClusterService metaDataClusterService;

    @Inject
    public TransportClusterRerouteAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool, MetaDataClusterService metaDataClusterService) {
        super(settings, transportService, clusterService, threadPool);
        this.metaDataClusterService = metaDataClusterService;
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected String transportAction() {
        return ClusterRerouteAction.NAME;
    }

    @Override
    protected ClusterRerouteRequest newRequest() {
        return new ClusterRerouteRequest();
    }

    @Override
    protected ClusterRerouteResponse newResponse() {
        return new ClusterRerouteResponse();
    }

    @Override
    protected ClusterRerouteResponse newResponse(ClusterRerouteClusterStateUpdateResponse updateResponse) {
        return new ClusterRerouteResponse(updateResponse.isAcknowledged(), updateResponse.clusterState());
    }

    @Override
    protected ClusterRerouteClusterStateUpdateRequest newClusterStateUpdateRequest(ClusterRerouteRequest acknowledgedRequest) {
        return new ClusterRerouteClusterStateUpdateRequest(acknowledgedRequest.commands, acknowledgedRequest.dryRun);
    }

    @Override
    protected void updateClusterState(ClusterRerouteClusterStateUpdateRequest updateRequest, ClusterStateUpdateActionListener<ClusterRerouteClusterStateUpdateResponse, ClusterRerouteResponse> listener) {
        metaDataClusterService.reroute(updateRequest, listener);
    }
}