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

package org.elasticsearch.action.admin.cluster.reroute;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingExplanations;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 */
public class TransportClusterRerouteAction extends TransportMasterNodeOperationAction<ClusterRerouteRequest, ClusterRerouteResponse> {

    private final AllocationService allocationService;

    @Inject
    public TransportClusterRerouteAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                         AllocationService allocationService, ActionFilters actionFilters) {
        super(settings, ClusterRerouteAction.NAME, transportService, clusterService, threadPool, actionFilters, ClusterRerouteRequest.class);
        this.allocationService = allocationService;
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterRerouteRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected ClusterRerouteResponse newResponse() {
        return new ClusterRerouteResponse();
    }

    @Override
    protected void masterOperation(final ClusterRerouteRequest request, final ClusterState state, final ActionListener<ClusterRerouteResponse> listener) throws ElasticsearchException {
        clusterService.submitStateUpdateTask("cluster_reroute (api)", Priority.IMMEDIATE, new AckedClusterStateUpdateTask<ClusterRerouteResponse>(request, listener) {

            private volatile ClusterState clusterStateToSend;
            private volatile RoutingExplanations explanations;

            @Override
            protected ClusterRerouteResponse newResponse(boolean acknowledged) {
                return new ClusterRerouteResponse(acknowledged, clusterStateToSend, explanations);
            }

            @Override
            public void onAckTimeout() {
                listener.onResponse(new ClusterRerouteResponse(false, clusterStateToSend, new RoutingExplanations()));
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.debug("failed to perform [{}]", t, source);
                super.onFailure(source, t);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                RoutingAllocation.Result routingResult = allocationService.reroute(currentState, request.commands, request.explain());
                ClusterState newState = ClusterState.builder(currentState).routingResult(routingResult).build();
                clusterStateToSend = newState;
                explanations = routingResult.explanations();
                if (request.dryRun) {
                    return currentState;
                }
                return newState;
            }
        });
    }
}