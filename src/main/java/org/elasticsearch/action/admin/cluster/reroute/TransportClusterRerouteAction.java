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

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProcessedClusterStateUpdateTask;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.ClusterState.newClusterStateBuilder;

/**
 */
public class TransportClusterRerouteAction extends TransportMasterNodeOperationAction<ClusterRerouteRequest, ClusterRerouteResponse> {

    private final AllocationService allocationService;

    @Inject
    public TransportClusterRerouteAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                         AllocationService allocationService) {
        super(settings, transportService, clusterService, threadPool);
        this.allocationService = allocationService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
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
    protected ClusterRerouteResponse masterOperation(final ClusterRerouteRequest request, ClusterState state) throws ElasticSearchException {
        final AtomicReference<Throwable> failureRef = new AtomicReference<Throwable>();
        final AtomicReference<ClusterState> clusterStateResponse = new AtomicReference<ClusterState>();
        final CountDownLatch latch = new CountDownLatch(1);

        clusterService.submitStateUpdateTask("cluster_reroute (api)", Priority.URGENT, new ProcessedClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                try {
                    RoutingAllocation.Result routingResult = allocationService.reroute(currentState, request.commands);
                    ClusterState newState = newClusterStateBuilder().state(currentState).routingResult(routingResult).build();
                    clusterStateResponse.set(newState);
                    if (request.dryRun) {
                        return currentState;
                    }
                    return newState;
                } catch (Exception e) {
                    logger.debug("failed to reroute", e);
                    failureRef.set(e);
                    latch.countDown();
                    return currentState;
                } finally {
                    // we don't release the latch here, only after we rerouted
                }
            }

            @Override
            public void clusterStateProcessed(ClusterState clusterState) {
                latch.countDown();
            }
        });

        try {
            latch.await();
        } catch (InterruptedException e) {
            failureRef.set(e);
        }

        if (failureRef.get() != null) {
            if (failureRef.get() instanceof ElasticSearchException) {
                throw (ElasticSearchException) failureRef.get();
            } else {
                throw new ElasticSearchException(failureRef.get().getMessage(), failureRef.get());
            }
        }

        return new ClusterRerouteResponse(clusterStateResponse.get());

    }
}