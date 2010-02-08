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

package org.elasticsearch.cluster;

import com.google.inject.Inject;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.node.Nodes;
import org.elasticsearch.discovery.DiscoveryService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.component.Lifecycle;
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
 * @author kimchy (Shay Banon)
 */
public class DefaultClusterService extends AbstractComponent implements ClusterService {

    private final Lifecycle lifecycle = new Lifecycle();

    private final TimeValue timeoutInterval;

    private final ThreadPool threadPool;

    private final DiscoveryService discoveryService;

    private final TransportService transportService;

    private volatile ExecutorService updateTasksExecutor;

    private final List<ClusterStateListener> clusterStateListeners = new CopyOnWriteArrayList<ClusterStateListener>();

    private final List<TimeoutHolder> clusterStateTimeoutListeners = new CopyOnWriteArrayList<TimeoutHolder>();

    private volatile ScheduledFuture scheduledFuture;

    private volatile ClusterState clusterState = newClusterStateBuilder().build();

    @Inject public DefaultClusterService(Settings settings, DiscoveryService discoveryService, TransportService transportService, ThreadPool threadPool) {
        super(settings);
        this.transportService = transportService;
        this.discoveryService = discoveryService;
        this.threadPool = threadPool;

        this.timeoutInterval = componentSettings.getAsTime("timeoutInterval", timeValueMillis(500));
    }

    @Override public Lifecycle.State lifecycleState() {
        return this.lifecycle.state();
    }

    @Override public ClusterService start() throws ElasticSearchException {
        if (!lifecycle.moveToStarted()) {
            return this;
        }
        this.updateTasksExecutor = newSingleThreadExecutor(daemonThreadFactory(settings, "clusterService#updateTask"));
        scheduledFuture = threadPool.scheduleWithFixedDelay(new Runnable() {
            @Override public void run() {
                long timestamp = System.currentTimeMillis();
                for (final TimeoutHolder holder : clusterStateTimeoutListeners) {
                    if ((timestamp - holder.timestamp) > holder.timeout.millis()) {
                        clusterStateTimeoutListeners.remove(holder);
                        DefaultClusterService.this.threadPool.execute(new Runnable() {
                            @Override public void run() {
                                holder.listener.onTimeout(holder.timeout);
                            }
                        });
                    }
                }
            }
        }, timeoutInterval);
        return this;
    }

    @Override public ClusterService stop() throws ElasticSearchException {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
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
        return this;
    }

    @Override public void close() throws ElasticSearchException {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }
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
                clusterState = updateTask.execute(previousClusterState);
                if (previousClusterState != clusterState) {
                    if (clusterState.nodes().localNodeMaster()) {
                        // only the master controls the version numbers
                        clusterState = newClusterStateBuilder().state(clusterState).incrementVersion().build();
                    }

                    if (logger.isDebugEnabled()) {
                        logger.debug("Cluster state updated, version [{}], source [{}]", clusterState.version(), source);
                    }
                    if (logger.isTraceEnabled()) {
                        StringBuilder sb = new StringBuilder("Cluster State:\n");
                        sb.append(clusterState.nodes().prettyPrint());
                        sb.append(clusterState.routingTable().prettyPrint());
                        sb.append(clusterState.routingNodes().prettyPrint());
                        logger.trace(sb.toString());
                    }

                    ClusterChangedEvent clusterChangedEvent = new ClusterChangedEvent(source, clusterState, previousClusterState, discoveryService.firstMaster());
                    // new cluster state, notify all listeners
                    final Nodes.Delta nodesDelta = clusterChangedEvent.nodesDelta();
                    if (nodesDelta.hasChanges() && logger.isInfoEnabled()) {
                        String summary = nodesDelta.shortSummary();
                        if (summary.length() > 0) {
                            logger.info(summary);
                        }
                    }

                    threadPool.execute(new Runnable() {
                        @Override public void run() {
                            transportService.nodesAdded(nodesDelta.addedNodes());
                        }
                    });

                    for (TimeoutHolder timeoutHolder : clusterStateTimeoutListeners) {
                        timeoutHolder.listener.clusterChanged(clusterChangedEvent);
                    }
                    for (ClusterStateListener listener : clusterStateListeners) {
                        listener.clusterChanged(clusterChangedEvent);
                    }

                    threadPool.execute(new Runnable() {
                        @Override public void run() {
                            transportService.nodesRemoved(nodesDelta.removedNodes());
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