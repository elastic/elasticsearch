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

package org.elasticsearch.cluster.routing;

import com.google.inject.Inject;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.routing.strategy.ShardsRoutingStrategy;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.component.Lifecycle;
import org.elasticsearch.util.component.LifecycleComponent;
import org.elasticsearch.util.settings.Settings;

import java.util.concurrent.Future;

import static org.elasticsearch.cluster.ClusterState.*;
import static org.elasticsearch.util.TimeValue.*;

/**
 * @author kimchy (Shay Banon)
 */
public class RoutingService extends AbstractComponent implements ClusterStateListener, LifecycleComponent<RoutingService> {

    private final Lifecycle lifecycle = new Lifecycle();

    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final ShardsRoutingStrategy shardsRoutingStrategy;

    private final TimeValue schedule;

    private volatile boolean routingTableDirty = false;

    private volatile Future scheduledRoutingTableFuture;

    @Inject public RoutingService(Settings settings, ThreadPool threadPool, ClusterService clusterService, ShardsRoutingStrategy shardsRoutingStrategy) {
        super(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.shardsRoutingStrategy = shardsRoutingStrategy;
        this.schedule = componentSettings.getAsTime("schedule", timeValueSeconds(10));
    }

    @Override public Lifecycle.State lifecycleState() {
        return this.lifecycle.state();
    }

    @Override public RoutingService start() throws ElasticSearchException {
        if (!lifecycle.moveToStarted()) {
            return this;
        }
        clusterService.add(this);
        return this;
    }

    @Override public RoutingService stop() throws ElasticSearchException {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        if (scheduledRoutingTableFuture != null) {
            scheduledRoutingTableFuture.cancel(true);
        }
        clusterService.remove(this);
        return this;
    }

    public void close() {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }
    }

    @Override public void clusterChanged(ClusterChangedEvent event) {
        if (event.source().equals(RoutingTableUpdater.CLUSTER_UPDATE_TASK_SOURCE)) {
            // that's us, ignore this event
            return;
        }
        if (event.state().nodes().localNodeMaster()) {
            // we are master, schedule the routing table updater
            if (scheduledRoutingTableFuture == null) {
                scheduledRoutingTableFuture = threadPool.scheduleWithFixedDelay(new RoutingTableUpdater(), schedule);
            }
            if (event.nodesRemoved()) {
                // if nodes were removed, we don't want to wait for the scheduled task
                // since we want to get primary election as fast as possible
                routingTableDirty = true;
                threadPool.execute(new RoutingTableUpdater());
            } else {
                if (event.routingTableChanged() || event.nodesAdded()) {
                    routingTableDirty = true;
                }
            }
        } else {
            if (scheduledRoutingTableFuture != null) {
                scheduledRoutingTableFuture.cancel(true);
                scheduledRoutingTableFuture = null;
            }
        }
    }

    private class RoutingTableUpdater implements Runnable {

        private static final String CLUSTER_UPDATE_TASK_SOURCE = "routing-table-updater";

        @Override public void run() {
            try {
                if (!routingTableDirty) {
                    return;
                }
                if (lifecycle.stopped()) {
                    return;
                }
                routingTableDirty = false;
                clusterService.submitStateUpdateTask(CLUSTER_UPDATE_TASK_SOURCE, new ClusterStateUpdateTask() {
                    @Override public ClusterState execute(ClusterState currentState) {
                        RoutingTable newRoutingTable = shardsRoutingStrategy.reroute(currentState);
                        return newClusterStateBuilder().state(currentState).routingTable(newRoutingTable).build();
                    }
                });
            } catch (Exception e) {
                logger.warn("Failed to reroute routing table", e);
            }
        }
    }
}
