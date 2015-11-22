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

import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.ScheduledFuture;
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
public class RoutingService extends AbstractLifecycleComponent<RoutingService> implements ClusterStateListener {

    private static final String CLUSTER_UPDATE_TASK_SOURCE = "cluster_reroute";

    final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final AllocationService allocationService;

    private AtomicBoolean rerouting = new AtomicBoolean();
    private volatile long minDelaySettingAtLastScheduling = Long.MAX_VALUE;
    private volatile ScheduledFuture registeredNextDelayFuture;

    @Inject
    public RoutingService(Settings settings, ThreadPool threadPool, ClusterService clusterService, AllocationService allocationService) {
        super(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        if (clusterService != null) {
            clusterService.addFirst(this);
        }
    }

    @Override
    protected void doStart() {
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {
        FutureUtils.cancel(registeredNextDelayFuture);
        clusterService.remove(this);
    }

    public AllocationService getAllocationService() {
        return this.allocationService;
    }

    /**
     * Initiates a reroute.
     */
    public final void reroute(String reason) {
        performReroute(reason);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().nodes().localNodeMaster()) {
            // Figure out if an existing scheduled reroute is good enough or whether we need to cancel and reschedule.
            // If the minimum of the currently relevant delay settings is larger than something we scheduled in the past,
            // we are guaranteed that the planned schedule will happen before any of the current shard delays are expired.
            long minDelaySetting = UnassignedInfo.findSmallestDelayedAllocationSetting(settings, event.state());
            if (minDelaySetting <= 0) {
                logger.trace("no need to schedule reroute - no delayed unassigned shards, minDelaySetting [{}], scheduled [{}]", minDelaySetting, minDelaySettingAtLastScheduling);
                minDelaySettingAtLastScheduling = Long.MAX_VALUE;
                FutureUtils.cancel(registeredNextDelayFuture);
            } else if (minDelaySetting < minDelaySettingAtLastScheduling) {
                FutureUtils.cancel(registeredNextDelayFuture);
                minDelaySettingAtLastScheduling = minDelaySetting;
                TimeValue nextDelay = TimeValue.timeValueNanos(UnassignedInfo.findNextDelayedAllocationIn(event.state()));
                assert nextDelay.nanos() > 0 : "next delay must be non 0 as minDelaySetting is [" + minDelaySetting + "]";
                logger.info("delaying allocation for [{}] unassigned shards, next check in [{}]",
                        UnassignedInfo.getNumberOfDelayedUnassigned(event.state()), nextDelay);
                registeredNextDelayFuture = threadPool.schedule(nextDelay, ThreadPool.Names.SAME, new AbstractRunnable() {
                    @Override
                    protected void doRun() throws Exception {
                        minDelaySettingAtLastScheduling = Long.MAX_VALUE;
                        reroute("assign delayed unassigned shards");
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        logger.warn("failed to schedule/execute reroute post unassigned shard", t);
                        minDelaySettingAtLastScheduling = Long.MAX_VALUE;
                    }
                });
            } else {
                logger.trace("no need to schedule reroute - current schedule reroute is enough. minDelaySetting [{}], scheduled [{}]", minDelaySetting, minDelaySettingAtLastScheduling);
            }
        }
    }

    // visible for testing
    long getMinDelaySettingAtLastScheduling() {
        return this.minDelaySettingAtLastScheduling;
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
            clusterService.submitStateUpdateTask(CLUSTER_UPDATE_TASK_SOURCE + "(" + reason + ")", Priority.HIGH, new ClusterStateUpdateTask() {
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
