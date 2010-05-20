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

package org.elasticsearch.cluster.service;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.discovery.DiscoveryService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.component.AbstractLifecycleComponent;
import org.elasticsearch.util.inject.Inject;
import org.elasticsearch.util.settings.Settings;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.*;
import static org.elasticsearch.cluster.ClusterState.*;
import static org.elasticsearch.util.TimeValue.*;
import static org.elasticsearch.util.concurrent.DynamicExecutors.*;

/**
 * @author kimchy (shay.banon)
 */
public class InternalClusterService extends AbstractLifecycleComponent<ClusterService> implements ClusterService {

    private final TimeValue timeoutInterval;

    private final ThreadPool threadPool;

    private final DiscoveryService discoveryService;

    private final TransportService transportService;

    private volatile ExecutorService updateTasksExecutor;

    private final List<ClusterStateListener> clusterStateListeners = new CopyOnWriteArrayList<ClusterStateListener>();

    private final List<TimeoutHolder> clusterStateTimeoutListeners = new CopyOnWriteArrayList<TimeoutHolder>();

    private volatile ScheduledFuture scheduledFuture;

    private volatile ClusterState clusterState = newClusterStateBuilder().build();

    @Inject public InternalClusterService(Settings settings, DiscoveryService discoveryService, TransportService transportService, ThreadPool threadPool) {
        super(settings);
        this.transportService = transportService;
        this.discoveryService = discoveryService;
        this.threadPool = threadPool;

        this.timeoutInterval = componentSettings.getAsTime("timeoutInterval", timeValueMillis(500));
    }

    @Override protected void doStart() throws ElasticSearchException {
        this.updateTasksExecutor = newSingleThreadExecutor(daemonThreadFactory(settings, "clusterService#updateTask"));
        scheduledFuture = threadPool.scheduleWithFixedDelay(new Runnable() {
            @Override public void run() {
                long timestamp = System.currentTimeMillis();
                for (final TimeoutHolder holder : clusterStateTimeoutListeners) {
                    if ((timestamp - holder.timestamp) > holder.timeout.millis()) {
                        clusterStateTimeoutListeners.remove(holder);
                        InternalClusterService.this.threadPool.execute(new Runnable() {
                            @Override public void run() {
                                holder.listener.onTimeout(holder.timeout);
                            }
                        });
                    }
                }
            }
        }, timeoutInterval);
    }

    @Override protected void doStop() throws ElasticSearchException {
        scheduledFuture.cancel(false);
        for (TimeoutHolder holder : clusterStateTimeoutListeners) {
            holder.listener.onTimeout(holder.timeout);
        }
        updateTasksExecutor.shutdown();
        try {
            updateTasksExecutor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    @Override protected void doClose() throws ElasticSearchException {
    }

    public ClusterState state() {
        return this.clusterState;
    }

    public void add(ClusterStateListener listener) {
        clusterStateListeners.add(listener);
    }

    public void remove(ClusterStateListener listener) {
        clusterStateListeners.remove(listener);
    }

    public void add(TimeValue timeout, TimeoutClusterStateListener listener) {
        clusterStateTimeoutListeners.add(new TimeoutHolder(listener, System.currentTimeMillis(), timeout));
    }

    public void remove(TimeoutClusterStateListener listener) {
        clusterStateTimeoutListeners.remove(new TimeoutHolder(listener, -1, null));
    }

    public void submitStateUpdateTask(final String source, final ClusterStateUpdateTask updateTask) {
        if (!lifecycle.started()) {
            return;
        }
        updateTasksExecutor.execute(new Runnable() {
            @Override public void run() {
                if (!lifecycle.started()) {
                    return;
                }
                ClusterState previousClusterState = clusterState;
                try {
                    clusterState = updateTask.execute(previousClusterState);
                } catch (Exception e) {
                    StringBuilder sb = new StringBuilder("Failed to execute cluster state update, state:\nVersion [").append(clusterState.version()).append("], source [").append(source).append("]\n");
                    sb.append(clusterState.nodes().prettyPrint());
                    sb.append(clusterState.routingTable().prettyPrint());
                    sb.append(clusterState.readOnlyRoutingNodes().prettyPrint());
                    logger.warn(sb.toString(), e);
                    return;
                }
                if (previousClusterState != clusterState) {
                    if (clusterState.nodes().localNodeMaster()) {
                        // only the master controls the version numbers
                        clusterState = new ClusterState(clusterState.version() + 1, clusterState.metaData(), clusterState.routingTable(), clusterState.nodes());
                    } else {
                        // we got this cluster state from the master, filter out based on versions (don't call listeners)
                        if (clusterState.version() < previousClusterState.version()) {
                            logger.info("Got old cluster state [" + clusterState.version() + "<" + previousClusterState.version() + "] from source [" + source + "], ignoring");
                            return;
                        }
                    }

                    if (logger.isTraceEnabled()) {
                        StringBuilder sb = new StringBuilder("Cluster State updated:\nVersion [").append(clusterState.version()).append("], source [").append(source).append("]\n");
                        sb.append(clusterState.nodes().prettyPrint());
                        sb.append(clusterState.routingTable().prettyPrint());
                        sb.append(clusterState.readOnlyRoutingNodes().prettyPrint());
                        logger.trace(sb.toString());
                    } else if (logger.isDebugEnabled()) {
                        logger.debug("Cluster state updated, version [{}], source [{}]", clusterState.version(), source);
                    }

                    ClusterChangedEvent clusterChangedEvent = new ClusterChangedEvent(source, clusterState, previousClusterState, discoveryService.firstMaster());
                    // new cluster state, notify all listeners
                    final DiscoveryNodes.Delta nodesDelta = clusterChangedEvent.nodesDelta();
                    if (nodesDelta.hasChanges() && logger.isInfoEnabled()) {
                        String summary = nodesDelta.shortSummary();
                        if (summary.length() > 0) {
                            logger.info("{}, Reason: {}", summary, source);
                        }
                    }

                    // TODO, do this in parallel (and wait)
                    for (DiscoveryNode node : nodesDelta.addedNodes()) {
                        try {
                            transportService.connectToNode(node);
                        } catch (Exception e) {
                            // TODO, need to mark this node as failed...
                            logger.warn("Failed to connect to node [" + node + "]", e);
                        }
                    }

                    for (TimeoutHolder timeoutHolder : clusterStateTimeoutListeners) {
                        timeoutHolder.listener.clusterChanged(clusterChangedEvent);
                    }
                    for (ClusterStateListener listener : clusterStateListeners) {
                        listener.clusterChanged(clusterChangedEvent);
                    }

                    threadPool.execute(new Runnable() {
                        @Override public void run() {
                            for (DiscoveryNode node : nodesDelta.removedNodes()) {
                                transportService.disconnectFromNode(node);
                            }
                        }
                    });

                    // if we are the master, publish the new state to all nodes
                    if (clusterState.nodes().localNodeMaster()) {
                        discoveryService.publish(clusterState);
                    }

                    if (updateTask instanceof ProcessedClusterStateUpdateTask) {
                        ((ProcessedClusterStateUpdateTask) updateTask).clusterStateProcessed(clusterState);
                    }
                }
            }
        });
    }

    private static class TimeoutHolder {
        final TimeoutClusterStateListener listener;
        final long timestamp;
        final TimeValue timeout;

        private TimeoutHolder(TimeoutClusterStateListener listener, long timestamp, TimeValue timeout) {
            this.listener = listener;
            this.timestamp = timestamp;
            this.timeout = timeout;
        }

        @Override public int hashCode() {
            return listener.hashCode();
        }

        @Override public boolean equals(Object obj) {
            return ((TimeoutHolder) obj).listener == listener;
        }
    }
}