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

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link RoutingService} listens to clusters state. When this service
 * receives a {@link ClusterChangedEvent} the cluster state will be verified and
 * the routing tables might be updated.
 * <p>
 * Note: The {@link RoutingService} is responsible for cluster wide operations
 * that include modifications to the cluster state. Such an operation can only
 * be performed on the clusters master node. Unless the local node this service
 * is running on is the clusters master node this service will not perform any
 * actions.
 * </p>
 */
public class RoutingService extends AbstractLifecycleComponent<RoutingService> {

    private static final String CLUSTER_UPDATE_TASK_SOURCE = "cluster_reroute";

    private final ClusterService clusterService;
    private final AllocationService allocationService;

    private AtomicBoolean rerouting = new AtomicBoolean();

    @Inject
    public RoutingService(Settings settings, ClusterService clusterService, AllocationService allocationService) {
        super(settings);
        this.clusterService = clusterService;
        this.allocationService = allocationService;
    }

    @Override
    protected void doStart() {
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {
    }

    /**
     * Initiates a reroute.
     */
    public final void reroute(String reason) {
        performReroute(reason);
    }

    // visible for testing
    protected void performReroute(String reason) {
        try {
            if (lifecycle.stopped()) {
                return;
            }
            if (rerouting.compareAndSet(false, true) == false) {
                logger.trace("already has pending reroute, ignoring {}", reason);
                return;
            }
            logger.trace("rerouting {}", reason);
            clusterService.submitStateUpdateTask(CLUSTER_UPDATE_TASK_SOURCE + "(" + reason + ")", new ClusterStateUpdateTask(Priority.HIGH) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    rerouting.set(false);
                    RoutingAllocation.Result routingResult = allocationService.reroute(currentState, reason);
                    if (!routingResult.changed()) {
                        // no state changed
                        return currentState;
                    }
                    return ClusterState.builder(currentState).routingResult(routingResult).build();
                }

                @Override
                public void onNoLongerMaster(String source) {
                    rerouting.set(false);
                    // no biggie
                }

                @Override
                public void onFailure(String source, Throwable t) {
                    rerouting.set(false);
                    ClusterState state = clusterService.state();
                    if (logger.isTraceEnabled()) {
                        logger.error("unexpected failure during [{}], current state:\n{}", t, source, state.prettyPrint());
                    } else {
                        logger.error("unexpected failure during [{}], current state version [{}]", t, source, state.version());
                    }
                }
            });
        } catch (Throwable e) {
            rerouting.set(false);
            ClusterState state = clusterService.state();
            logger.warn("failed to reroute routing table, current state:\n{}", e, state.prettyPrint());
        }
    }
}
