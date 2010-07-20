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
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.timer.Timeout;
import org.elasticsearch.common.timer.TimerTask;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.jsr166y.LinkedTransferQueue;
import org.elasticsearch.discovery.DiscoveryService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.timer.TimerService;
import org.elasticsearch.transport.TransportService;

import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.*;
import static org.elasticsearch.cluster.ClusterState.*;
import static org.elasticsearch.common.util.concurrent.EsExecutors.*;

/**
 * @author kimchy (shay.banon)
 */
public class InternalClusterService extends AbstractLifecycleComponent<ClusterService> implements ClusterService {

    private final ThreadPool threadPool;

    private final TimerService timerService;

    private final DiscoveryService discoveryService;

    private final TransportService transportService;

    private volatile ExecutorService updateTasksExecutor;

    private final List<ClusterStateListener> clusterStateListeners = new CopyOnWriteArrayList<ClusterStateListener>();

    private final Queue<Tuple<Timeout, NotifyTimeout>> onGoingTimeouts = new LinkedTransferQueue<Tuple<Timeout, NotifyTimeout>>();

    private volatile ClusterState clusterState = newClusterStateBuilder().build();

    @Inject public InternalClusterService(Settings settings, DiscoveryService discoveryService, TransportService transportService, ThreadPool threadPool,
                                          TimerService timerService) {
        super(settings);
        this.transportService = transportService;
        this.discoveryService = discoveryService;
        this.threadPool = threadPool;
        this.timerService = timerService;
    }

    @Override protected void doStart() throws ElasticSearchException {
        this.clusterState = newClusterStateBuilder().build();
        this.updateTasksExecutor = newSingleThreadExecutor(daemonThreadFactory(settings, "clusterService#updateTask"));
    }

    @Override protected void doStop() throws ElasticSearchException {
        for (Tuple<Timeout, NotifyTimeout> onGoingTimeout : onGoingTimeouts) {
            onGoingTimeout.v1().cancel();
            onGoingTimeout.v2().listener.onClose();
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
        for (Iterator<Tuple<Timeout, NotifyTimeout>> it = onGoingTimeouts.iterator(); it.hasNext();) {
            Tuple<Timeout, NotifyTimeout> tuple = it.next();
            if (tuple.v2().listener.equals(listener)) {
                tuple.v1().cancel();
                it.remove();
            }
        }
    }

    public void add(TimeValue timeout, final TimeoutClusterStateListener listener) {
        NotifyTimeout notifyTimeout = new NotifyTimeout(listener, timeout);
        Timeout timerTimeout = timerService.newTimeout(notifyTimeout, timeout, TimerService.ExecutionType.THREADED);
        onGoingTimeouts.add(new Tuple<Timeout, NotifyTimeout>(timerTimeout, notifyTimeout));
        clusterStateListeners.add(listener);
        // call the post added notification on the same event thread
        updateTasksExecutor.execute(new Runnable() {
            @Override public void run() {
                listener.postAdded();
            }
        });
    }

    public void submitStateUpdateTask(final String source, final ClusterStateUpdateTask updateTask) {
        if (!lifecycle.started()) {
            return;
        }
        updateTasksExecutor.execute(new Runnable() {
            @Override public void run() {
                if (!lifecycle.started()) {
                    logger.debug("processing [{}]: ignoring, cluster_service not started", source);
                    return;
                }
                logger.debug("processing [{}]: execute", source);
                ClusterState previousClusterState = clusterState;
                try {
                    clusterState = updateTask.execute(previousClusterState);
                } catch (Exception e) {
                    StringBuilder sb = new StringBuilder("failed to execute cluster state update, state:\nversion [").append(clusterState.version()).append("], source [").append(source).append("]\n");
                    sb.append(clusterState.nodes().prettyPrint());
                    sb.append(clusterState.routingTable().prettyPrint());
                    sb.append(clusterState.readOnlyRoutingNodes().prettyPrint());
                    logger.warn(sb.toString(), e);
                    return;
                }
                if (previousClusterState != clusterState) {
                    if (clusterState.nodes().localNodeMaster()) {
                        // only the master controls the version numbers
                        clusterState = new ClusterState(clusterState.version() + 1, clusterState);
                    } else {
                        // we got this cluster state from the master, filter out based on versions (don't call listeners)
                        if (clusterState.version() < previousClusterState.version()) {
                            logger.debug("got old cluster state [" + clusterState.version() + "<" + previousClusterState.version() + "] from source [" + source + "], ignoring");
                            return;
                        }
                    }

                    if (logger.isTraceEnabled()) {
                        StringBuilder sb = new StringBuilder("cluster state updated:\nversion [").append(clusterState.version()).append("], source [").append(source).append("]\n");
                        sb.append(clusterState.nodes().prettyPrint());
                        sb.append(clusterState.routingTable().prettyPrint());
                        sb.append(clusterState.readOnlyRoutingNodes().prettyPrint());
                        logger.trace(sb.toString());
                    } else if (logger.isDebugEnabled()) {
                        logger.debug("cluster state updated, version [{}], source [{}]", clusterState.version(), source);
                    }

                    ClusterChangedEvent clusterChangedEvent = new ClusterChangedEvent(source, clusterState, previousClusterState);
                    // new cluster state, notify all listeners
                    final DiscoveryNodes.Delta nodesDelta = clusterChangedEvent.nodesDelta();
                    if (nodesDelta.hasChanges() && logger.isInfoEnabled()) {
                        String summary = nodesDelta.shortSummary();
                        if (summary.length() > 0) {
                            logger.info("{}, reason: {}", summary, source);
                        }
                    }

                    // TODO, do this in parallel (and wait)
                    for (DiscoveryNode node : nodesDelta.addedNodes()) {
                        try {
                            transportService.connectToNode(node);
                        } catch (Exception e) {
                            // the fault detection will detect it as failed as well
                            logger.warn("failed to connect to node [" + node + "]", e);
                        }
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

                    logger.debug("processing [{}]: done applying updated cluster_state", source);
                } else {
                    logger.debug("processing [{}]: no change in cluster_state", source);
                }
            }
        });
    }

    private class NotifyTimeout implements TimerTask {
        final TimeoutClusterStateListener listener;
        final TimeValue timeout;

        private NotifyTimeout(TimeoutClusterStateListener listener, TimeValue timeout) {
            this.listener = listener;
            this.timeout = timeout;
        }

        @Override public void run(Timeout timeout) throws Exception {
            if (timeout.isCancelled()) {
                return;
            }
            listener.onTimeout(this.timeout);
            // note, we rely on the listener to remove itself in case of timeout if needed
        }
    }
}